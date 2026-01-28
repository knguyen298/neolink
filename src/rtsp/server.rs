use crate::rtsp::streams::{AudioCodec, MediaPacket, StreamMeta, StreamRegistry, StreamState};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

const RTP_MAX_PAYLOAD: usize = 1200;
const RTP_VIDEO_PT: u8 = 96;
const RTP_AUDIO_PT: u8 = 97;
const CHANNEL_VIDEO_RTP: u8 = 0;
const CHANNEL_AUDIO_RTP: u8 = 2;

#[derive(Clone)]
pub(crate) struct RtspServer {
    registry: Arc<StreamRegistry>,
}

impl RtspServer {
    pub(crate) fn new(registry: Arc<StreamRegistry>) -> Self {
        Self { registry }
    }

    pub(crate) async fn run(&self, bind_addr: &str, port: u16) -> Result<()> {
        let listener = TcpListener::bind((bind_addr, port))
            .await
            .with_context(|| format!("Failed to bind RTSP to {}:{}", bind_addr, port))?;
        loop {
            let (socket, _) = listener.accept().await?;
            let registry = self.registry.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, registry).await {
                    log::error!("RTSP connection error: {e}");
                }
            });
        }
    }
}

struct RtspRequest {
    method: String,
    uri: String,
    version: String,
    headers: HashMap<String, String>,
}

struct ConnectionState {
    writer: Arc<Mutex<OwnedWriteHalf>>,
    registry: Arc<StreamRegistry>,
    session_id: Option<String>,
    stream: Option<Arc<StreamState>>,
    stream_task: Option<JoinHandle<()>>,
    video_setup: bool,
    audio_setup: bool,
    video_rtp: Option<RtpState>,
    audio_rtp: Option<RtpState>,
    video_channel: u8,
    audio_channel: u8,
}

impl ConnectionState {
    fn new(writer: Arc<Mutex<OwnedWriteHalf>>, registry: Arc<StreamRegistry>) -> Self {
        Self {
            writer,
            registry,
            session_id: None,
            stream: None,
            stream_task: None,
            video_setup: false,
            audio_setup: false,
            video_rtp: None,
            audio_rtp: None,
            video_channel: CHANNEL_VIDEO_RTP,
            audio_channel: CHANNEL_AUDIO_RTP,
        }
    }

    fn ensure_session(&mut self) -> String {
        if let Some(session) = &self.session_id {
            return session.clone();
        }
        let session = Uuid::new_v4().to_string();
        self.session_id = Some(session.clone());
        session
    }

    fn stop_streaming(&mut self) {
        if let Some(task) = &self.stream_task {
            task.abort();
        }
        self.stream_task = None;
    }

    fn start_streaming(&mut self) {
        if self.stream_task.is_some() {
            return;
        }
        let stream = match self.stream.clone() {
            Some(stream) => stream,
            None => return,
        };
        let writer = self.writer.clone();
        let enable_audio = self.audio_setup;
        let video_rtp = self.video_rtp.take().unwrap_or_else(|| RtpState::new(90_000));
        let audio_rtp = self.audio_rtp.take();
        let video_channel = self.video_channel;
        let audio_channel = self.audio_channel;
        self.stream_task = Some(tokio::spawn(async move {
            if let Err(e) =
                stream_loop(writer, stream, enable_audio, video_rtp, audio_rtp, video_channel, audio_channel).await
            {
                log::warn!("RTSP stream loop error: {e}");
            }
        }));
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, registry: Arc<StreamRegistry>) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let writer = Arc::new(Mutex::new(write_half));
    let mut state = ConnectionState::new(writer, registry);

    loop {
        let request = match read_request(&mut reader).await? {
            Some(req) => req,
            None => break,
        };

        let method = request.method.as_str();
        let request_path = request_path(&request.uri).and_then(|p| normalize_path(&p));

        log::debug!("RTSP {} {}", method, request.uri);

        match method {
            "OPTIONS" => {
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![("Public", "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN".into())],
                    "".into(),
                )
                .await?;
            }
            "DESCRIBE" => {
                let path = request_path.ok_or_else(|| anyhow!("Invalid URI"))?;
                let stream = state
                    .registry
                    .get(&path)
                    .await
                    .ok_or_else(|| anyhow!("Stream not found"))?;
                let meta = wait_for_meta(&stream).await;
                let body = build_sdp(&path, &meta);
                let base = request.uri.clone();
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![
                        ("Content-Type", "application/sdp".to_string()),
                        ("Content-Base", format!("{base}/")),
                    ],
                    body,
                )
                .await?;
            }
            "SETUP" => {
                let path = request_path.ok_or_else(|| anyhow!("Invalid URI"))?;
                let track_id = track_id(&path).unwrap_or(0);
                let stream_path = strip_track_id(&path);
                let stream = state
                    .registry
                    .get(&stream_path)
                    .await
                    .ok_or_else(|| anyhow!("Stream not found"))?;
                state.stream = Some(stream);
                let transport_header = request
                    .headers
                    .get("transport")
                    .map(|v| v.to_lowercase())
                    .unwrap_or_default();
                if !transport_header.contains("tcp") {
                    send_response(
                        &state.writer,
                        request.headers.get("cseq"),
                        (461, "Unsupported Transport"),
                        vec![(
                            "Transport",
                            "RTP/AVP/TCP;unicast;interleaved=0-1".to_string(),
                        )],
                        "".into(),
                    )
                    .await?;
                    continue;
                }
                let session = state.ensure_session();
                let (interleaved_rtp, interleaved_rtcp) =
                    parse_interleaved(&transport_header).unwrap_or((
                        if track_id == 0 { 0 } else { 2 },
                        if track_id == 0 { 1 } else { 3 },
                    ));
                let transport = match track_id {
                    0 => {
                        state.video_setup = true;
                        state.video_channel = interleaved_rtp;
                        format!("RTP/AVP/TCP;unicast;interleaved={}-{}", interleaved_rtp, interleaved_rtcp)
                    }
                    1 => {
                        return Err(anyhow!("Audio track is currently disabled"));
                    }
                    _ => return Err(anyhow!("Unsupported track")),
                };
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![("Transport", transport), ("Session", session)],
                    "".into(),
                )
                .await?;
            }
            "PLAY" => {
                if state.stream.is_none() || !state.video_setup {
                    return Err(anyhow!("Session not established"));
                }
                let base_uri = request.uri.trim_end_matches('/');
                let mut video_rtp = RtpState::new(90_000);
                let mut rtp_infos = vec![format!(
                    "url={}/trackID=0;seq={};rtptime={}",
                    base_uri,
                    video_rtp.current_seq(),
                    video_rtp.current_timestamp()
                )];
                if state.audio_setup {
                    let audio_rtp = RtpState::new(48_000);
                    rtp_infos.push(format!(
                        "url={}/trackID=1;seq={};rtptime={}",
                        base_uri,
                        audio_rtp.current_seq(),
                        audio_rtp.current_timestamp()
                    ));
                    state.audio_rtp = Some(audio_rtp);
                }
                let rtp_info = format!(
                    "{}",
                    rtp_infos.join(",")
                );
                state.video_rtp = Some(video_rtp);
                state.start_streaming();
                let session = state.ensure_session();
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![
                        ("Session", session),
                        ("Range", "npt=0-".to_string()),
                        ("RTP-Info", rtp_info),
                    ],
                    "".into(),
                )
                .await?;
            }
            "GET_PARAMETER" | "SET_PARAMETER" => {
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![],
                    "".into(),
                )
                .await?;
            }
            "TEARDOWN" => {
                state.stop_streaming();
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (200, "OK"),
                    vec![],
                    "".into(),
                )
                .await?;
                break;
            }
            _ => {
                send_response(
                    &state.writer,
                    request.headers.get("cseq"),
                    (501, "Not Implemented"),
                    vec![],
                    "".into(),
                )
                .await?;
            }
        }
    }
    state.stop_streaming();
    Ok(())
}

async fn read_request(reader: &mut BufReader<OwnedReadHalf>) -> Result<Option<RtspRequest>> {
    let mut request_line = String::new();
    loop {
        request_line.clear();
        let bytes = reader.read_line(&mut request_line).await?;
        if bytes == 0 {
            return Ok(None);
        }
        if !request_line.trim().is_empty() {
            break;
        }
    }
    let mut parts = request_line.trim_end().split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("Missing RTSP method"))?
        .to_string();
    let uri = parts
        .next()
        .ok_or_else(|| anyhow!("Missing RTSP URI"))?
        .to_string();
    let version = parts
        .next()
        .ok_or_else(|| anyhow!("Missing RTSP version"))?
        .to_string();

    let mut headers = HashMap::new();
    loop {
        let mut header_line = String::new();
        reader.read_line(&mut header_line).await?;
        let line = header_line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_lowercase(), value.trim().to_string());
        }
    }

    Ok(Some(RtspRequest {
        method,
        uri,
        version,
        headers,
    }))
}

fn request_path(uri: &str) -> Option<String> {
    if let Some(idx) = uri.find("rtsp://") {
        let tail = &uri[idx + 7..];
        if let Some(idx2) = tail.find('/') {
            return Some(tail[idx2..].to_string());
        }
        return Some("/".to_string());
    }
    if uri.starts_with('/') {
        return Some(uri.to_string());
    }
    None
}

fn normalize_path(path: &str) -> Option<String> {
    let clean = path.split('?').next().unwrap_or(path).to_string();
    Some(clean)
}

fn strip_track_id(path: &str) -> String {
    if let Some(idx) = path.rfind("/trackID=") {
        return path[..idx].to_string();
    }
    if let Some(idx) = path.rfind("/trackid=") {
        return path[..idx].to_string();
    }
    path.to_string()
}

fn track_id(path: &str) -> Option<u8> {
    let idx = path.rfind("trackID=").or_else(|| path.rfind("trackid="))?;
    let (_, rest) = path.split_at(idx + 8);
    rest.parse::<u8>().ok()
}

async fn send_response(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    cseq: Option<&String>,
    status: (u16, &'static str),
    headers: Vec<(&'static str, String)>,
    body: String,
) -> Result<()> {
    let mut response = format!("RTSP/1.0 {} {}\r\n", status.0, status.1);
    if let Some(seq) = cseq {
        response.push_str(&format!("CSeq: {}\r\n", seq));
    }
    for (name, value) in headers {
        response.push_str(&format!("{}: {}\r\n", name, value));
    }
    if !body.is_empty() {
        response.push_str(&format!("Content-Length: {}\r\n", body.len()));
        response.push_str("\r\n");
        response.push_str(&body);
    } else {
        response.push_str("\r\n");
    }
    let mut locked = writer.lock().await;
    locked.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn stream_loop(
    writer: Arc<Mutex<OwnedWriteHalf>>,
    stream: Arc<StreamState>,
    enable_audio: bool,
    mut video_rtp: RtpState,
    mut audio_rtp: Option<RtpState>,
    video_channel: u8,
    audio_channel: u8,
) -> Result<()> {
    let mut rx = stream.subscribe();
    let mut started = false;

    let snapshot = stream.snapshot().await;
    if let Some(start_idx) = snapshot.iter().rposition(|packet| {
        matches!(packet.as_ref(), MediaPacket::Video { keyframe: true, .. })
    }) {
        for packet in snapshot[start_idx..].iter() {
            process_packet(
                packet.as_ref(),
                &writer,
                &stream,
                enable_audio,
                &mut video_rtp,
                &mut audio_rtp,
                video_channel,
                audio_channel,
                &mut started,
            )
            .await?;
        }
    }

    loop {
        let packet = match rx.recv().await {
            Ok(packet) => packet,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        };
        process_packet(
            packet.as_ref(),
            &writer,
            &stream,
            enable_audio,
            &mut video_rtp,
            &mut audio_rtp,
            video_channel,
            audio_channel,
            &mut started,
        )
        .await?;
    }
    Ok(())
}

async fn process_packet(
    packet: &MediaPacket,
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    stream: &StreamState,
    enable_audio: bool,
    video_rtp: &mut RtpState,
    audio_rtp: &mut Option<RtpState>,
    video_channel: u8,
    audio_channel: u8,
    started: &mut bool,
) -> Result<()> {
    match packet {
        MediaPacket::Video {
            video_type,
            data,
            timestamp_us,
            keyframe,
        } => {
            if !*started {
                if !*keyframe {
                    return Ok(());
                }
                let meta = stream.meta().await;
                send_parameter_sets(writer, video_rtp, *video_type, &meta, *timestamp_us).await?;
                *started = true;
            }
            let timestamp = video_rtp.next_timestamp(*timestamp_us);
            send_video_frame(writer, video_rtp, *video_type, data, timestamp, video_channel).await?;
        }
        MediaPacket::Audio {
            codec,
            data,
            duration_us,
        } => {
            if !enable_audio || !*started {
                return Ok(());
            }
            let AudioCodec::Aac { sample_rate, .. } = codec else {
                return Ok(());
            };
            let rtp = audio_rtp.get_or_insert_with(|| RtpState::new(*sample_rate));
            let payload = strip_adts(data);
            if payload.is_empty() {
                return Ok(());
            }
            let samples = samples_from_duration(*duration_us, *sample_rate);
            let timestamp = rtp.advance_samples(samples);
            send_audio_packet(writer, rtp, payload, timestamp, audio_channel).await?;
        }
    }
    Ok(())
}

fn strip_adts(data: &[u8]) -> &[u8] {
    if data.len() < 7 {
        return &[];
    }
    let protection_absent = (data[1] & 0x01) == 1;
    let header_len = if protection_absent { 7 } else { 9 };
    if data.len() <= header_len {
        return &[];
    }
    &data[header_len..]
}

fn samples_from_duration(duration_us: u32, sample_rate: u32) -> u32 {
    if duration_us == 0 {
        return 1024;
    }
    ((duration_us as u64 * sample_rate as u64) / 1_000_000) as u32
}

async fn send_parameter_sets(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    video_type: neolink_core::bcmedia::model::VideoType,
    meta: &StreamMeta,
    timestamp_us: u32,
) -> Result<()> {
    let timestamp = rtp.next_timestamp(timestamp_us);
    match video_type {
        neolink_core::bcmedia::model::VideoType::H264 => {
            if let (Some(sps), Some(pps)) = (meta.sps.as_ref(), meta.pps.as_ref()) {
                send_video_nal(writer, rtp, video_type, sps, timestamp, false, CHANNEL_VIDEO_RTP).await?;
                send_video_nal(writer, rtp, video_type, pps, timestamp, false, CHANNEL_VIDEO_RTP).await?;
            }
        }
        neolink_core::bcmedia::model::VideoType::H265 => {
            if let (Some(vps), Some(sps), Some(pps)) =
                (meta.vps.as_ref(), meta.sps.as_ref(), meta.pps.as_ref())
            {
                send_video_nal(writer, rtp, video_type, vps, timestamp, false, CHANNEL_VIDEO_RTP).await?;
                send_video_nal(writer, rtp, video_type, sps, timestamp, false, CHANNEL_VIDEO_RTP).await?;
                send_video_nal(writer, rtp, video_type, pps, timestamp, false, CHANNEL_VIDEO_RTP).await?;
            }
        }
    }
    Ok(())
}

async fn send_video_frame(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    video_type: neolink_core::bcmedia::model::VideoType,
    data: &[u8],
    timestamp: u32,
    channel: u8,
) -> Result<()> {
    let nalus = split_nalus(data);
    if nalus.is_empty() {
        return Ok(());
    }
    for (idx, nal) in nalus.iter().enumerate() {
        let marker = idx + 1 == nalus.len();
        send_video_nal(writer, rtp, video_type, nal, timestamp, marker, channel).await?;
    }
    Ok(())
}

async fn send_video_nal(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    video_type: neolink_core::bcmedia::model::VideoType,
    nal: &[u8],
    timestamp: u32,
    marker: bool,
    channel: u8,
) -> Result<()> {
    match video_type {
        neolink_core::bcmedia::model::VideoType::H264 => {
            send_h264_nal(writer, rtp, nal, timestamp, marker, channel).await
        }
        neolink_core::bcmedia::model::VideoType::H265 => {
            send_h265_nal(writer, rtp, nal, timestamp, marker, channel).await
        }
    }
}

async fn send_h264_nal(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    nal: &[u8],
    timestamp: u32,
    marker: bool,
    channel: u8,
) -> Result<()> {
    if nal.is_empty() {
        return Ok(());
    }
    if nal.len() <= RTP_MAX_PAYLOAD {
        send_rtp_packet(writer, channel, rtp, RTP_VIDEO_PT, marker, timestamp, nal)
            .await?;
        return Ok(());
    }

    let nal_header = nal[0];
    let nal_type = nal_header & 0x1F;
    let fu_indicator = (nal_header & 0xE0) | 28;
    let max_fragment = RTP_MAX_PAYLOAD - 2;
    let mut offset = 1;
    let mut first = true;
    while offset < nal.len() {
        let end = (offset + max_fragment).min(nal.len());
        let chunk = &nal[offset..end];
        let last = end == nal.len();
        let fu_header = (if first { 0x80 } else { 0 }) | (if last { 0x40 } else { 0 }) | nal_type;
        let mut payload = Vec::with_capacity(2 + chunk.len());
        payload.push(fu_indicator);
        payload.push(fu_header);
        payload.extend_from_slice(chunk);
        send_rtp_packet(writer, channel, rtp, RTP_VIDEO_PT, marker && last, timestamp, &payload)
            .await?;
        first = false;
        offset = end;
    }
    Ok(())
}

async fn send_h265_nal(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    nal: &[u8],
    timestamp: u32,
    marker: bool,
    channel: u8,
) -> Result<()> {
    if nal.len() < 3 {
        return Ok(());
    }
    if nal.len() <= RTP_MAX_PAYLOAD {
        send_rtp_packet(writer, channel, rtp, RTP_VIDEO_PT, marker, timestamp, nal)
            .await?;
        return Ok(());
    }

    let nal_header0 = nal[0];
    let nal_header1 = nal[1];
    let nal_type = (nal_header0 >> 1) & 0x3F;
    let fu_indicator0 = (nal_header0 & 0x81) | (49 << 1);
    let fu_indicator1 = nal_header1;
    let max_fragment = RTP_MAX_PAYLOAD - 3;
    let mut offset = 2;
    let mut first = true;
    while offset < nal.len() {
        let end = (offset + max_fragment).min(nal.len());
        let chunk = &nal[offset..end];
        let last = end == nal.len();
        let fu_header = (if first { 0x80 } else { 0 }) | (if last { 0x40 } else { 0 }) | nal_type;
        let mut payload = Vec::with_capacity(3 + chunk.len());
        payload.push(fu_indicator0);
        payload.push(fu_indicator1);
        payload.push(fu_header);
        payload.extend_from_slice(chunk);
        send_rtp_packet(writer, channel, rtp, RTP_VIDEO_PT, marker && last, timestamp, &payload)
            .await?;
        first = false;
        offset = end;
    }
    Ok(())
}

async fn send_audio_packet(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    rtp: &mut RtpState,
    payload: &[u8],
    timestamp: u32,
    channel: u8,
) -> Result<()> {
    let mut offset = 0;
    while offset < payload.len() {
        let end = (offset + RTP_MAX_PAYLOAD).min(payload.len());
        let chunk = &payload[offset..end];
        let marker = end == payload.len();
        send_rtp_packet(writer, channel, rtp, RTP_AUDIO_PT, marker, timestamp, chunk).await?;
        offset = end;
    }
    Ok(())
}

async fn send_rtp_packet(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    channel: u8,
    rtp: &mut RtpState,
    payload_type: u8,
    marker: bool,
    timestamp: u32,
    payload: &[u8],
) -> Result<()> {
    let header = build_rtp_header(payload_type, marker, rtp.next_seq(), timestamp, rtp.ssrc);
    let mut packet = Vec::with_capacity(12 + payload.len());
    packet.extend_from_slice(&header);
    packet.extend_from_slice(payload);
    send_interleaved(writer, channel, &packet).await
}

async fn send_interleaved(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    channel: u8,
    payload: &[u8],
) -> Result<()> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.push(0x24);
    buf.push(channel);
    buf.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    buf.extend_from_slice(payload);
    let mut locked = writer.lock().await;
    locked.write_all(&buf).await?;
    Ok(())
}

fn build_rtp_header(
    payload_type: u8,
    marker: bool,
    seq: u16,
    timestamp: u32,
    ssrc: u32,
) -> [u8; 12] {
    let mut header = [0u8; 12];
    header[0] = 0x80;
    header[1] = (if marker { 0x80 } else { 0x00 }) | (payload_type & 0x7F);
    header[2..4].copy_from_slice(&seq.to_be_bytes());
    header[4..8].copy_from_slice(&timestamp.to_be_bytes());
    header[8..12].copy_from_slice(&ssrc.to_be_bytes());
    header
}

struct RtpState {
    seq: u16,
    timestamp: u32,
    ssrc: u32,
    last_ts_us: Option<u32>,
    clock_rate: u32,
}

impl RtpState {
    fn new(clock_rate: u32) -> Self {
        let uuid = Uuid::new_v4();
        let random = uuid.as_bytes();
        let seq = u16::from_be_bytes([random[0], random[1]]);
        let ssrc = u32::from_be_bytes([random[2], random[3], random[4], random[5]]);
        let timestamp = u32::from_be_bytes([random[6], random[7], random[8], random[9]]);
        Self {
            seq,
            timestamp,
            ssrc,
            last_ts_us: None,
            clock_rate,
        }
    }

    fn next_seq(&mut self) -> u16 {
        let current = self.seq;
        self.seq = self.seq.wrapping_add(1);
        current
    }

    fn current_seq(&self) -> u16 {
        self.seq
    }

    fn current_timestamp(&self) -> u32 {
        self.timestamp
    }

    fn next_timestamp(&mut self, timestamp_us: u32) -> u32 {
        if let Some(prev) = self.last_ts_us {
            let delta = timestamp_us.wrapping_sub(prev) as u64;
            let inc = ((delta * self.clock_rate as u64) / 1_000_000).max(1) as u32;
            self.timestamp = self.timestamp.wrapping_add(inc);
        }
        self.last_ts_us = Some(timestamp_us);
        self.timestamp
    }

    fn advance_samples(&mut self, samples: u32) -> u32 {
        self.timestamp = self.timestamp.wrapping_add(samples);
        self.timestamp
    }
}

fn split_nalus(data: &[u8]) -> Vec<&[u8]> {
    let mut nalus = vec![];
    let mut pos = 0;
    while pos + 4 <= data.len() {
        if &data[pos..pos + 4] == &[0, 0, 0, 1] {
            let start = pos + 4;
            pos = start;
            while pos + 4 <= data.len() && &data[pos..pos + 4] != &[0, 0, 0, 1] {
                pos += 1;
            }
            let end = if pos + 4 <= data.len() && &data[pos..pos + 4] == &[0, 0, 0, 1] {
                pos
            } else {
                data.len()
            };
            if start < end {
                nalus.push(&data[start..end]);
            }
            pos = end;
        } else {
            pos += 1;
        }
    }
    nalus
}

fn parse_interleaved(transport: &str) -> Option<(u8, u8)> {
    let token = transport.split(';').find(|part| part.trim_start().starts_with("interleaved="))?;
    let values = token.trim_start().trim_start_matches("interleaved=");
    let mut parts = values.split('-');
    let first = parts.next()?.parse::<u8>().ok()?;
    let second = parts.next()?.parse::<u8>().ok()?;
    Some((first, second))
}

fn build_sdp(path: &str, meta: &StreamMeta) -> String {
    let mut sdp = format!(
        "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns={}\r\nc=IN IP4 0.0.0.0\r\nt=0 0\r\n",
        path
    );
    let mut has_video = false;

    if let Some(video_type) = meta.video_type {
        has_video = true;
        let payload_type = RTP_VIDEO_PT;
        let rtpmap = match video_type {
            neolink_core::bcmedia::model::VideoType::H264 => "H264/90000",
            neolink_core::bcmedia::model::VideoType::H265 => "H265/90000",
        };
        sdp.push_str(&format!("m=video 0 RTP/AVP {}\r\n", payload_type));
        sdp.push_str(&format!("a=rtpmap:{} {}\r\n", payload_type, rtpmap));
        match video_type {
            neolink_core::bcmedia::model::VideoType::H264 => {
                if let (Some(sps), Some(pps)) = (meta.sps.as_ref(), meta.pps.as_ref()) {
                    let sps_encoded = BASE64.encode(sps);
                    let pps_encoded = BASE64.encode(pps);
                    sdp.push_str(&format!(
                        "a=fmtp:{} packetization-mode=1;sprop-parameter-sets={sps_encoded},{pps_encoded}\r\n",
                        payload_type
                    ));
                }
            }
            neolink_core::bcmedia::model::VideoType::H265 => {
                if let (Some(vps), Some(sps), Some(pps)) =
                    (meta.vps.as_ref(), meta.sps.as_ref(), meta.pps.as_ref())
                {
                    let vps_encoded = BASE64.encode(vps);
                    let sps_encoded = BASE64.encode(sps);
                    let pps_encoded = BASE64.encode(pps);
                    sdp.push_str(&format!(
                        "a=fmtp:{} sprop-vps={vps_encoded};sprop-sps={sps_encoded};sprop-pps={pps_encoded}\r\n",
                        payload_type
                    ));
                }
            }
        }
        sdp.push_str("a=control:trackID=0\r\n");
    }

    if meta.audio.is_some() {
        log::warn!("Audio is currently disabled in RTSP SDP to avoid AAC LATM incompatibilities.");
    }
    sdp
}

async fn wait_for_meta(stream: &StreamState) -> StreamMeta {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
    loop {
        let meta = stream.meta().await;
        let ready = match meta.video_type {
            Some(neolink_core::bcmedia::model::VideoType::H264) => {
                meta.sps.is_some() && meta.pps.is_some()
            }
            Some(neolink_core::bcmedia::model::VideoType::H265) => {
                meta.vps.is_some() && meta.sps.is_some() && meta.pps.is_some()
            }
            None => false,
        };
        if ready {
            return meta;
        }
        if tokio::time::Instant::now() >= deadline {
            return meta;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
