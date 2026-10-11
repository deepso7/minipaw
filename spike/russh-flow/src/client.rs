//! The russh client side of the spike.

use std::sync::Arc;

use russh::client::{self, Handle};
use russh::keys::PublicKeyOrCertificate;
use russh::{ChannelMsg, ChannelReadHalf};
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;

pub struct Client;

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

pub fn config() -> client::Config {
    client::Config {
        inactivity_timeout: None,
        ..Default::default()
    }
}

/// Connect over `stream` and authenticate with `none`.
pub async fn connect(stream: UnixStream, cfg: client::Config) -> Handle<Client> {
    let mut h = client::connect_stream(Arc::new(cfg), stream, Client)
        .await
        .expect("handshake");
    let ok = h.authenticate_none("spike").await.expect("auth");
    assert!(ok.success(), "auth none rejected");
    h
}

/// What came back on a channel.
#[derive(Debug, Default)]
pub struct Outcome {
    pub stdout_len: u64,
    pub stdout_sha: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_status: Option<u32>,
    pub eof: bool,
    pub closed: bool,
    pub other: Vec<String>,
}

/// Read until the channel closes, hashing stdout and keeping stderr.
pub async fn collect(mut read: ChannelReadHalf) -> Outcome {
    let mut out = Outcome::default();
    let mut sha = Sha256::new();
    while let Some(msg) = read.wait().await {
        match msg {
            ChannelMsg::Data { data } => {
                out.stdout_len += data.len() as u64;
                sha.update(&data);
            }
            ChannelMsg::ExtendedData { data, ext: 1 } => out.stderr.extend_from_slice(&data),
            ChannelMsg::ExitStatus { exit_status } => out.exit_status = Some(exit_status),
            ChannelMsg::Eof => out.eof = true,
            ChannelMsg::Close => {
                out.closed = true;
                break;
            }
            ChannelMsg::WindowAdjusted { .. } | ChannelMsg::Success => {}
            other => out.other.push(format!("{other:?}")),
        }
    }
    out.stdout_sha = sha.finalize().to_vec();
    out
}

pub fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}
