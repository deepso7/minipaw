//! The russh server's settings, its host key, and the limits the server
//! keeps to.

use std::borrow::Cow;
use std::time::Duration;

use russh::keys::ssh_key::private::Ed25519Keypair;
use russh::keys::{Algorithm, PrivateKey};
use russh::server::Config;
use russh::{MethodKind, MethodSet, Preferred, cipher, compression, kex};

/// How much input a channel may have buffered for its program, counted in
/// bytes received. Passing it closes the channel (see `channel.rs`).
pub const INPUT_CAP: usize = 8 * 1024 * 1024;

/// What a channel's client reads on stderr when its input passes the cap.
pub const CAP_MESSAGE: &str = "minipaw: input exceeded 8 MiB buffer, closing\n";

/// The largest piece of output written to a channel at once.
pub const CHUNK: usize = 32 * 1024;

/// How long a connection has to authenticate, from its first byte.
pub const PRE_AUTH_DEADLINE: Duration = Duration::from_secs(20);

/// How long a torn-down program has to exit before it is killed.
pub const GRACE: Duration = Duration::from_secs(2);

/// How long a cut connection has for russh to notice and end.
pub const CONNECTION_END: Duration = Duration::from_secs(2);

/// How long a stop waits for every connection to tear down before the last
/// resort. Teardown is concurrent, so the grace and a connection's end fit.
pub const SHUTDOWN_BUDGET: Duration = Duration::from_secs(5);

/// How often a program is checked for having exited.
pub const EXIT_POLL: Duration = Duration::from_millis(50);

/// The host key: an ed25519 key from the identity's SSH host seed, so it
/// stays the same as long as the ticket does.
pub fn host_key(seed: &[u8; 32]) -> PrivateKey {
    PrivateKey::from(Ed25519Keypair::from_seed(seed))
}

/// The server's settings: public keys, or `none` alone under `--no-auth`,
/// and modern algorithms only.
pub fn server(host_key: PrivateKey, no_auth: bool) -> Config {
    let method = if no_auth {
        MethodKind::None
    } else {
        MethodKind::PublicKey
    };
    Config {
        methods: MethodSet::from(&[method][..]),
        preferred: Preferred {
            kex: Cow::Borrowed(&[
                kex::MLKEM768X25519_SHA256,
                kex::CURVE25519,
                kex::EXTENSION_SUPPORT_AS_SERVER,
                kex::EXTENSION_OPENSSH_STRICT_KEX_AS_SERVER,
            ]),
            key: Cow::Borrowed(&[Algorithm::Ed25519]),
            cipher: Cow::Borrowed(&[
                cipher::CHACHA20_POLY1305,
                cipher::AES_256_GCM,
                cipher::AES_128_GCM,
            ]),
            compression: Cow::Borrowed(&[compression::NONE]),
            ..Preferred::default()
        },
        keys: vec![host_key],
        max_auth_attempts: 6,
        auth_rejection_time: Duration::from_secs(1),
        // OpenSSH probes with `none` first; that need not wait.
        auth_rejection_time_initial: Some(Duration::ZERO),
        // Before auth the connection's deadline applies; after it, an idle
        // shell may stay as long as its client answers keepalives.
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(30)),
        ..Config::default()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    use minipaw::Identity;

    use super::*;

    #[test]
    fn a_known_identity_has_a_known_host_key() {
        let dir = std::env::temp_dir().join(format!("minipaw-hostkey-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        let path = dir.join("serve.key");
        let _ = std::fs::remove_file(&path);
        let key: String = (0u8..32).map(|b| format!("{b:02x}")).collect();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("create");
        write!(
            file,
            "minipaw identity v1\nkey {key}\ntoken {}\n",
            "aa".repeat(16)
        )
        .expect("write");
        let identity = Identity::load(&path).expect("load");
        std::fs::remove_dir_all(&dir).expect("clean up");

        let public = host_key(&identity.ssh_host_seed())
            .public_key()
            .to_openssh()
            .expect("openssh");
        assert_eq!(public, HOST_KEY);
    }

    /// The host key of the identity whose secret is 00 01 .. 1f, computed
    /// from its seed with Python's `cryptography`.
    const HOST_KEY: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID8gGzNKaCxdHC4pXT2kKLfIWTWjDvzgugVPzENKtJt8";

    #[test]
    fn only_the_configured_method_is_offered() {
        let key = host_key(&[7; 32]);
        let config = server(key.clone(), false);
        assert_eq!(&*config.methods, &[MethodKind::PublicKey]);
        let config = server(key, true);
        assert_eq!(&*config.methods, &[MethodKind::None]);
        assert!(config.inactivity_timeout.is_none());
    }
}
