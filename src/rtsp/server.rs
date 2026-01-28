use crate::rtsp::streams::{AudioCodec, StreamMeta, StreamRegistry};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

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
            tokio::spawn(async move { if let Err(e) = handle_connection(socket, registry).await { log::error!("RTSP connection error: {e}"); } });
        }
    }
}

struct RtspRequest {
    method: String,
    uri: String,
    version: String,
    headers: HashMap<String, String>,
}

async fn handle_connection(stream: tokio::net::TcpStream, registry: Arc<StreamRegistry>) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut writer = write_half;
    let mut session_path: Option<String> = None;

    loop {
        if let Some(request) = read_request(&mut reader).await? {
            let request_path = request_path(&request.uri);
            let response = process_request(&request, &registry, request_path.clone(), session_path.clone()).await;
            match response {
                Ok((body, headers, status)) => {
                    send_response(&mut writer, request.headers.get("cseq"), status, headers, body).await?;
                    if request.method == "SETUP" {
                        session_path = request_path;
                    }
                }
                Err(err) => {
                    send_response(&mut writer, request.headers.get("cseq"), (500, "Internal Server Error"), vec![], err.to_string()).await?;
                }
            }
        } else {
            break;
        }
    }
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
    let method = parts.next().ok_or_else(|| anyhow!("Missing RTSP method"))?.to_string();
    let uri = parts.next().ok_or_else(|| anyhow!("Missing RTSP URI"))?.to_string();
    let version = parts.next().ok_or_else(|| anyhow!("Missing RTSP version"))?.to_string();

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

    Ok(Some(RtspRequest { method, uri, version, headers }))
}

async fn process_request(
    request: &RtspRequest,
    registry: &StreamRegistry,
    request_path: Option<String>,
    session_path: Option<String>,
) -> Result<(String, Vec<(&'static str, String)>, (u16, &'static str))> {
    match request.method.as_str() {
        "OPTIONS" => Ok(("".into(), vec![("Public", "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN".into())], (200, "OK"))),
        "DESCRIBE" => {
            if let Some(path) = request_path {
                if let Some(stream) = registry.get(&path).await {
                    let meta = stream.meta().await;
                    let body = build_sdp(&path, &meta);
                    Ok((body, vec![("Content-Type", "application/sdp".to_string())], (200, "OK")))
                } else {
                    Err(anyhow!("Stream not found"))
                }
            } else {
                Err(anyhow!("Invalid URI"))
            }
        }
        "SETUP" => Ok(("".into(), vec![("Transport", "RTP/AVP/TCP;interleaved=0-1".into())], (200, "OK"))),
        "PLAY" => {
            if session_path.is_none() {
                Err(anyhow!("Session not established"))
            } else {
                Ok(("".into(), vec![], (200, "OK")))
            }
        }
        "TEARDOWN" => Ok(("".into(), vec![], (200, "OK"))),
        _ => Err(anyhow!("Unsupported RTSP method")),
    }
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

async fn send_response(
    writer: &mut OwnedWriteHalf,
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
    writer.write_all(response.as_bytes()).await?;
    Ok(())
}

fn build_sdp(path: &str, meta: &StreamMeta) -> String {
    let mut sdp = format!("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns={}\r\nt=0 0\r\n", path);
    let mut control_id = 0;
    if let Some(video_type) = meta.video_type {
        let payload_type = 96;
        sdp.push_str(&format!("m=video 0 RTP/AVP {}\r\n", payload_type));
        sdp.push_str("a=rtpmap:96 H264/90000\r\n");
        if let (Some(sps), Some(pps)) = (meta.sps.as_ref(), meta.pps.as_ref()) {
            let sps_encoded = BASE64.encode(sps);
            let pps_encoded = BASE64.encode(pps);
            sdp.push_str(&format!(
                "a=fmtp:96 packetization-mode=1;sprop-parameter-sets={sps_encoded},{pps_encoded}\r\n"
            ));
        }
        sdp.push_str(&format!("a=control:trackID={control_id}\r\n"));
        control_id += 1;
    }
    if let Some(AudioCodec::Aac { sample_rate, channels, config, .. }) = meta.audio.as_ref() {
        let payload = 97;
        sdp.push_str(&format!("m=audio 0 RTP/AVP {payload}\r\n"));
        sdp.push_str(&format!("a=rtpmap:{payload} MP4A-LATM/{sample_rate}/{channels}\r\n"));
        sdp.push_str(&format!("a=fmtp:{payload} streamtype=5;profile-level-id=15;mode=AAC-hbr;config={:02X}{:02X}\r\n", config[0], config[1]));
        sdp.push_str(&format!("a=control:trackID={control_id}\r\n"));
        control_id += 1;
    } else if let Some(AudioCodec::Adpcm { sample_rate, channels }) = meta.audio.as_ref() {
        let payload = 98;
        sdp.push_str(&format!("m=audio 0 RTP/AVP {payload}\r\n"));
        sdp.push_str(&format!("a=rtpmap:{payload} DVI4/{sample_rate}/{channels}\r\n"));
        sdp.push_str(&format!("a=control:trackID={control_id}\r\n"));
        control_id += 1;
    }
    sdp
}
