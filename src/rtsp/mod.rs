use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tokio::time::sleep;

use crate::cmdline::Opt;
use crate::common::{NeoInstance, NeoReactor};
use crate::config::CameraConfig;
use crate::rtsp::server::RtspServer;
use crate::rtsp::streams::{canonical_paths, MediaPacket, StreamRegistry, StreamState};
use neolink_core::bc_protocol::{StreamKind};

mod cmdline;
mod server;
mod streams;

pub(crate) use cmdline::Opt;

pub(crate) async fn main(_opt: Opt, reactor: NeoReactor) -> Result<()> {
    let config = reactor.config().await?.borrow().clone();
    let registry = Arc::new(StreamRegistry::new());
    for cam_config in config.cameras.into_iter().filter(|cam| cam.enabled) {
        let reactor = reactor.clone();
        let registry = registry.clone();
        task::spawn(async move {
            spawn_camera_streams(reactor, cam_config, registry).await;
        });
    }

    let server = RtspServer::new(registry.clone());
    server.run(&config.bind_addr, config.bind_port).await
}

async fn spawn_camera_streams(
    reactor: NeoReactor,
    config: CameraConfig,
    registry: Arc<StreamRegistry>,
) {
    let name = config.name.clone();
    let stream_kinds = config.stream.as_stream_kinds();
    if stream_kinds.is_empty() {
        return;
    }

    if let Ok(instance) = reactor.get(&name).await {
        for kind in stream_kinds {
            let buffer_duration = config.buffer_duration;
            let state = Arc::new(StreamState::new(kind, buffer_duration));
            for path in canonical_paths(&name, kind) {
                registry.register(path, state.clone()).await;
            }
            let instance_clone = instance.clone();
            tokio::spawn(async move {
                run_stream(instance_clone, kind, state).await;
            });
        }
    }
}

async fn run_stream(instance: NeoInstance, kind: StreamKind, state: Arc<StreamState>) {
    loop {
        match instance.stream(kind).await {
            Ok(mut rx) => {
                while let Some(media) = rx.recv().await {
                    match media {
                        Ok(bc_media) => {
                            if let Some(packet) = MediaPacket::try_from_bcmedia(bc_media) {
                                state.push_packet(Arc::new(packet)).await;
                            }
                        }
                        Err(e) => {
                            log::warn!("{} stream error: {e}", state.stream_name());
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("{} stream disconnected: {e}", state.stream_name());
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}
