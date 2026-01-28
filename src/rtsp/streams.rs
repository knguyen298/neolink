use anyhow::Context;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use neolink_core::bc_protocol::StreamKind;
use neolink_core::bcmedia::model::{BcMedia, BcMediaAac, BcMediaAdpcm, BcMediaIframe, BcMediaPframe, VideoType};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};

const BROADCAST_CAPACITY: usize = 1024;

/// Audio codecs that we currently expose via SDP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AudioCodec {
    /// AAC/ADTS
    Aac {
        sample_rate: u32,
        channels: u8,
        config: [u8; 2],
    },
    /// ADPCM (DVI4)
    Adpcm { sample_rate: u32, channels: u8 },
}

impl fmt::Display for AudioCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AudioCodec::Aac { .. } => write!(f, "AAC"),
            AudioCodec::Adpcm { .. } => write!(f, "ADPCM"),
        }
    }
}

/// Represents a generic frame coming out of the Baichuan stream.
#[derive(Clone, Debug)]
pub(crate) enum MediaPacket {
    Video {
        video_type: VideoType,
        data: Arc<[u8]>,
        timestamp_us: u32,
        keyframe: bool,
    },
    Audio {
        codec: AudioCodec,
        data: Arc<[u8]>,
        duration_us: u32,
    },
}

impl MediaPacket {
    fn try_from_bcmedia(media: BcMedia) -> Option<Self> {
        match media {
            BcMedia::Iframe(BcMediaIframe { video_type, data, microseconds, .. }) => Some(
                MediaPacket::Video {
                    video_type,
                    data: Arc::from(data.into_boxed_slice()),
                    timestamp_us: microseconds,
                    keyframe: true,
                },
            ),
            BcMedia::Pframe(BcMediaPframe { video_type, data, microseconds, .. }) => Some(
                MediaPacket::Video {
                    video_type,
                    data: Arc::from(data.into_boxed_slice()),
                    timestamp_us: microseconds,
                    keyframe: false,
                },
            ),
            BcMedia::Aac(BcMediaAac { data, .. }) => {
                let duration_us = BcMediaAac { data: data.clone(), }.duration().unwrap_or(0);
                parse_aac_adts(&data).map(|(sample_rate, channels, config)| MediaPacket::Audio {
                    codec: AudioCodec::Aac {
                        sample_rate,
                        channels,
                        config,
                    },
                    data: Arc::from(data.into_boxed_slice()),
                    duration_us,
                })
            }
            BcMedia::Adpcm(BcMediaAdpcm { data, .. }) => {
                let duration_us = BcMediaAdpcm { data: data.clone(), }.duration().unwrap_or(0);
                // DVI4 is always 8000Hz mono
                Some(MediaPacket::Audio {
                    codec: AudioCodec::Adpcm {
                        sample_rate: 8000,
                        channels: 1,
                    },
                    data: Arc::from(data.into_boxed_slice()),
                    duration_us,
                })
            }
            _ => None,
        }
    }
}

fn parse_aac_adts(data: &[u8]) -> Option<(u32, u8, [u8; 2])> {
    if data.len() < 7 {
        return None;
    }
    let sampling_index = (data[2] & 0x3C) >> 2;
    let channel_config = ((data[2] & 0x01) << 2) | ((data[3] & 0xC0) >> 6);
    let profile = ((data[2] & 0xC0) >> 6) + 1;
    let sample_rate = match sampling_index {
        0 => 96000,
        1 => 88200,
        2 => 64000,
        3 => 48000,
        4 => 44100,
        5 => 32000,
        6 => 24000,
        7 => 22050,
        8 => 16000,
        9 => 12000,
        10 => 11025,
        11 => 8000,
        _ => return None,
    };
    let config: [u8; 2] = [((profile << 3) | (sampling_index >> 1)) as u8, ((sampling_index & 1) << 7 | (channel_config << 3)) as u8];
    Some((sample_rate, channel_config as u8, config))
}

/// Metadata that describes the capabilities of a stream.
#[derive(Debug, Default, Clone)]
pub(crate) struct StreamMeta {
    pub video_type: Option<VideoType>,
    pub sps: Option<Vec<u8>>,
    pub pps: Option<Vec<u8>>,
    pub vps: Option<Vec<u8>>,
    pub audio: Option<AudioCodec>,
}

/// Shared state for a single Baichuan stream.
pub(crate) struct StreamState {
    pub buffer_duration_ms: u64,
    buffer: Mutex<StreamBuffer>,
    broadcaster: broadcast::Sender<Arc<MediaPacket>>,
    meta: RwLock<StreamMeta>,
    stream_kind: StreamKind,
}

struct StreamBuffer {
    entries: VecDeque<Arc<MediaPacket>>,
    capacity: usize,
}

impl StreamBuffer {
    fn new(capacity: usize) -> Self {
        Self { entries: VecDeque::with_capacity(capacity), capacity }
    }

    fn push(&mut self, packet: Arc<MediaPacket>) {
        self.entries.push_back(packet);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    fn snapshot(&self) -> Vec<Arc<MediaPacket>> {
        self.entries.iter().cloned().collect()
    }
}

impl StreamState {
    pub(crate) fn new(stream_kind: StreamKind, buffer_duration_ms: u64) -> Self {
        let capacity = (buffer_duration_ms as usize / 30).max(5).min(256);
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            buffer_duration_ms,
            buffer: Mutex::new(StreamBuffer::new(capacity)),
            broadcaster: tx,
            meta: RwLock::new(StreamMeta::default()),
            stream_kind,
        }
    }

    pub(crate) async fn push_packet(&self, packet: Arc<MediaPacket>) {
        self.update_meta(&packet).await;
        {
            let mut buffer = self.buffer.lock().await;
            buffer.push(packet.clone());
        }
        let _ = self.broadcaster.send(packet);
    }

    pub(crate) async fn snapshot(&self) -> Vec<Arc<MediaPacket>> {
        let buffer = self.buffer.lock().await;
        buffer.snapshot()
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Arc<MediaPacket>> {
        self.broadcaster.subscribe()
    }

    pub(crate) async fn meta(&self) -> StreamMeta {
        self.meta.read().await.clone()
    }

    async fn update_meta(&self, packet: &MediaPacket) {
        let mut meta = self.meta.write().await;
        match packet {
            MediaPacket::Video { video_type, data, .. } => {
                meta.video_type = Some(*video_type);
                for nal in split_nalus(data) {
                    if *video_type == VideoType::H264 {
                        match nal[0] & 0x1F {
                            7 => meta.sps = Some(nal.to_vec()),
                            8 => meta.pps = Some(nal.to_vec()),
                            _ => {}
                        }
                    } else if *video_type == VideoType::H265 {
                        match (nal[0] >> 1) & 0x3F {
                            32 => meta.vps = Some(nal.to_vec()),
                            33 => meta.sps = Some(nal.to_vec()),
                            34 => meta.pps = Some(nal.to_vec()),
                            _ => {}
                        }
                    }
                }
            }
            MediaPacket::Audio { codec, .. } => {
                meta.audio = Some(codec.clone());
            }
        }
    }

    pub(crate) fn stream_name(&self) -> &'static str {
        match self.stream_kind {
            StreamKind::Main => "main",
            StreamKind::Sub => "sub",
            StreamKind::Extern => "extern",
        }
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

/// Registry for all active RTSP streams.
pub(crate) struct StreamRegistry {
    inner: RwLock<HashMap<String, Arc<StreamState>>>,
}

impl StreamRegistry {
    pub(crate) fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    pub(crate) async fn register(&self, path: String, state: Arc<StreamState>) {
        self.inner.write().await.insert(path, state);
    }

    pub(crate) async fn get(&self, path: &str) -> Option<Arc<StreamState>> {
        self.inner.read().await.get(path).cloned()
    }

    pub(crate) async fn remove(&self, path: &str) {
        self.inner.write().await.remove(path);
    }
}

pub(crate) fn canonical_paths(name: &str, stream: StreamKind) -> Vec<String> {
    let base = format!("/{name}");
    match stream {
        StreamKind::Main => vec![base.clone(), format!("{base}/main"), format!("{base}/mainStream")],
        StreamKind::Sub => vec![format!("{base}/sub"), format!("{base}/subStream")],
        StreamKind::Extern => vec![format!("{base}/extern"), format!("{base}/externStream")],
    }
}
