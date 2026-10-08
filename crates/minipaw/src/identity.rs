//! A saved key and token, so a listener's ticket stays the same from run to
//! run.
//!
//! The file is text:
//!
//! ```text
//! minipaw identity v1
//! key <64 hex digits: the Ed25519 secret key>
//! token <32 hex digits>
//! ```
//!
//! On Unix it must be a private regular file (no symlink, owned by us, no
//! group or other bits) in a directory owned by us that only we can write,
//! so nobody else can read the key or swap the file. A new file is written
//! to a temporary file in the same directory and published with a hard link,
//! which never replaces anything; a rotated one is renamed over the old.
//! Either way a reader never sees a partial file.

use std::fmt;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use minip2p::{Ed25519Keypair, PeerAddr, PeerId};

use crate::Error;
use crate::ticket::Ticket;
use crate::wire::Token;

const HEADER: &str = "minipaw identity v1";
/// Longer files are not identities; reading stops there.
const MAX_LEN: u64 = 256;

/// A listener's persistent key pair and ticket token. Run with the same
/// identity ([`Config::identity`](crate::Config::identity)) and relay, and
/// the ticket is the same every time.
///
/// ```no_run
/// let (identity, created) = minipaw::Identity::load_or_create("serve.key")?;
/// println!("{}", identity.ticket(None));
/// # Ok::<(), minipaw::Error>(())
/// ```
///
/// The file holds the secret key, so the security checks described on
/// [`load`](Self::load) apply on Unix (Linux, macOS). Elsewhere they are
/// skipped: keep the file private yourself. `Debug` shows only the peer id.
#[derive(Clone)]
pub struct Identity {
    key: Ed25519Keypair,
    token: Token,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("peer_id", &format_args!("{}", self.peer_id()))
            .finish_non_exhaustive()
    }
}

impl Identity {
    /// A fresh random identity.
    ///
    /// # Errors
    ///
    /// [`Error::Other`] when the system's randomness is unavailable.
    pub fn generate() -> Result<Identity, Error> {
        let mut bytes = [0u8; 48];
        getrandom::fill(&mut bytes).map_err(|e| Error::Other(format!("system randomness: {e}")))?;
        let (key, token) = bytes.split_at(32);
        Ok(Identity {
            key: Ed25519Keypair::from_secret_key_bytes(key.try_into().unwrap_or_default()),
            token: token.try_into().unwrap_or_default(),
        })
    }

    /// Reads the identity file at `path`.
    ///
    /// On Unix, its directory must be owned by the current user and not
    /// writable by group or others, and the file must be a regular file (not
    /// a symlink, FIFO or device) owned by the current user with no group or
    /// other permissions.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the file is missing, unreadable, fails those
    /// checks, or is not exactly an identity file.
    pub fn load(path: impl AsRef<Path>) -> Result<Identity, Error> {
        let path = path.as_ref();
        read(path).map_err(|e| failed(path, &e))
    }

    /// Saves the identity to a new file at `path`, with mode 0600. Never
    /// replaces an existing file; see [`replace`](Self::replace).
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when `path` already exists, or its directory fails
    /// the checks on [`load`](Self::load) or cannot be written.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        let path = path.as_ref();
        self.create(path).map_err(|e| failed(path, &e))
    }

    /// Loads the identity at `path`, or creates and saves a new one if there
    /// is none. Also says whether it was created. When several processes
    /// race to create it, they all end up with the same one.
    ///
    /// # Errors
    ///
    /// As [`load`](Self::load) and [`save`](Self::save). An existing file
    /// that fails the checks is an error, never replaced.
    pub fn load_or_create(path: impl AsRef<Path>) -> Result<(Identity, bool), Error> {
        let path = path.as_ref();
        match read(path) {
            Ok(identity) => return Ok((identity, false)),
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(failed(path, &e)),
            Err(_) => {}
        }
        let identity = Identity::generate()?;
        match identity.create(path) {
            Ok(()) => Ok((identity, true)),
            // Someone else created it first: theirs wins.
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                Ok((Identity::load(path)?, false))
            }
            Err(e) => Err(failed(path, &e)),
        }
    }

    /// Saves the identity at `path`, atomically replacing whatever is there,
    /// to rotate a ticket. A symlink at `path` is replaced, not followed.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the directory fails the checks on
    /// [`load`](Self::load) or cannot be written.
    pub fn replace(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        let path = path.as_ref();
        self.rotate(path).map_err(|e| failed(path, &e))
    }

    /// The peer id this identity runs as.
    pub fn peer_id(&self) -> PeerId {
        self.key.peer_id()
    }

    /// The ticket a listener with this identity prints when reachable
    /// through `relay` (`None` means [`DEFAULT_RELAY`](crate::DEFAULT_RELAY)).
    /// Take `relay` from [`parse_relay`](crate::parse_relay), which checks it
    /// fits in a ticket.
    pub fn ticket(&self, relay: Option<&PeerAddr>) -> Ticket {
        Ticket::listener(self.peer_id(), self.token, relay)
    }

    pub(crate) fn key(&self) -> &Ed25519Keypair {
        &self.key
    }

    pub(crate) fn token(&self) -> Token {
        self.token
    }

    fn encode(&self) -> String {
        format!(
            "{HEADER}\nkey {}\ntoken {}\n",
            hex(&self.key.secret_key_bytes()),
            hex(&self.token)
        )
    }

    /// Publishes a new file at `path`, failing with `AlreadyExists` if
    /// there is one.
    fn create(&self, path: &Path) -> io::Result<()> {
        let dir = sys::open_dir(dir_of(path))?;
        let tmp = self.write_temp(path)?;
        // Unlike rename, a hard link never replaces an existing file.
        let linked = fs::hard_link(&tmp, path);
        if let Err(e) = fs::remove_file(&tmp) {
            log::debug!("remove {}: {e}", tmp.display());
        }
        linked?;
        dir.sync_all()
    }

    /// Replaces whatever is at `path`.
    fn rotate(&self, path: &Path) -> io::Result<()> {
        let dir = sys::open_dir(dir_of(path))?;
        let tmp = self.write_temp(path)?;
        if let Err(e) = fs::rename(&tmp, path) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        dir.sync_all()
    }

    /// Writes the identity to a fresh private file next to `path`, synced to
    /// disk, and returns its path. A crash leaves at worst this file behind;
    /// nothing reads it.
    fn write_temp(&self, path: &Path) -> io::Result<PathBuf> {
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::other("not a file path"))?;
        let mut suffix = [0u8; 8];
        getrandom::fill(&mut suffix).map_err(|e| io::Error::other(e.to_string()))?;
        let mut tmp_name = std::ffi::OsString::from(".");
        tmp_name.push(name);
        tmp_name.push(format!(".tmp-{}", hex(&suffix)));
        let tmp = dir_of(path).join(tmp_name);
        let mut file = sys::create_private(&tmp)?;
        let written = file
            .write_all(self.encode().as_bytes())
            .and_then(|()| file.sync_all());
        if let Err(e) = written {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(tmp)
    }
}

fn read(path: &Path) -> io::Result<Identity> {
    sys::open_dir(dir_of(path))?;
    let file = sys::open_private(path)?;
    let mut raw = Vec::new();
    file.take(MAX_LEN + 1).read_to_end(&mut raw)?;
    std::str::from_utf8(&raw)
        .ok()
        .and_then(parse)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("not a minipaw identity file (expected the lines '{HEADER}', 'key <64 hex digits>', 'token <32 hex digits>')"),
            )
        })
}

/// Accepts exactly what [`Identity::encode`] writes.
fn parse(text: &str) -> Option<Identity> {
    let rest = text.strip_prefix(HEADER)?.strip_prefix('\n')?;
    let (key, rest) = rest.strip_prefix("key ")?.split_once('\n')?;
    let (token, rest) = rest.strip_prefix("token ")?.split_once('\n')?;
    if !rest.is_empty() {
        return None;
    }
    Some(Identity {
        key: Ed25519Keypair::from_secret_key_bytes(unhex(key)?),
        token: unhex(token)?,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Lowercase hex of exactly `N` bytes.
fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let s = s.as_bytes();
    if s.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (byte, &[hi, lo]) in out.iter_mut().zip(s.as_chunks::<2>().0) {
        *byte = (digit(hi)? << 4) | digit(lo)?;
    }
    Some(out)
}

/// The directory holding `path`; `.` for a bare file name.
fn dir_of(path: &Path) -> &Path {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    }
}

fn failed(path: &Path, e: &io::Error) -> Error {
    Error::Config(format!("identity file {}: {e}", path.display()))
}

#[cfg(unix)]
mod sys {
    use std::fs::{File, Metadata};
    use std::io;
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Path;

    use rustix::fs::{Mode, OFlags};
    use rustix::io::Errno;

    /// Opens `dir` without following a symlink and checks that only we can
    /// change its entries. The handle is for syncing it.
    pub fn open_dir(dir: &Path) -> io::Result<File> {
        let subject = format!("directory {}", dir.display());
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let file = open(dir, flags, Mode::empty(), &subject)?;
        let meta = file.metadata()?;
        check_owner(&meta, &subject)?;
        if meta.mode() & 0o022 != 0 {
            return Err(denied(format!(
                "{subject} is writable by group or others (chmod go-w it, or pick another)"
            )));
        }
        Ok(file)
    }

    /// Opens an existing identity file, which must be private. Opening does
    /// not block on a FIFO or a device; the checks then reject it.
    pub fn open_private(path: &Path) -> io::Result<File> {
        let subject = "the file";
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        let file = open(path, flags, Mode::empty(), subject)?;
        let meta = file.metadata()?;
        if !meta.is_file() {
            return Err(denied(format!("{subject} is not a regular file")));
        }
        check_owner(&meta, subject)?;
        if meta.mode() & 0o077 != 0 {
            return Err(denied(format!(
                "{subject} is readable or writable by group or others (chmod 600 it)"
            )));
        }
        Ok(file)
    }

    /// Creates a new file, readable and writable only by us.
    pub fn create_private(path: &Path) -> io::Result<File> {
        let flags =
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        open(path, flags, Mode::RUSR | Mode::WUSR, "the file")
    }

    fn open(path: &Path, flags: OFlags, mode: Mode, subject: &str) -> io::Result<File> {
        match rustix::fs::open(path, flags, mode) {
            Ok(fd) => Ok(File::from(fd)),
            // Which of these a symlink gives depends on the system and the
            // flags.
            Err(e @ (Errno::LOOP | Errno::NOTDIR | Errno::MLINK)) => {
                if path.symlink_metadata().is_ok_and(|m| m.is_symlink()) {
                    Err(denied(format!("{subject} is a symlink; use its target")))
                } else {
                    Err(io::Error::new(
                        io::Error::from(e).kind(),
                        format!("{subject}: {e}"),
                    ))
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    fn check_owner(meta: &Metadata, subject: &str) -> io::Result<()> {
        if meta.uid() != rustix::process::geteuid().as_raw() {
            return Err(denied(format!(
                "{subject} is not owned by the current user"
            )));
        }
        Ok(())
    }

    fn denied(message: String) -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, message)
    }
}

/// Without Unix ownership and modes there is nothing to check.
#[cfg(not(unix))]
mod sys {
    use std::fs::{self, File, OpenOptions};
    use std::io;
    use std::path::Path;

    pub struct Dir;

    impl Dir {
        pub fn sync_all(&self) -> io::Result<()> {
            Ok(())
        }
    }

    pub fn open_dir(dir: &Path) -> io::Result<Dir> {
        if !fs::metadata(dir)?.is_dir() {
            return Err(io::Error::other(format!(
                "{} is not a directory",
                dir.display()
            )));
        }
        Ok(Dir)
    }

    pub fn open_private(path: &Path) -> io::Result<File> {
        let file = File::open(path)?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::other("not a regular file"));
        }
        Ok(file)
    }

    pub fn create_private(path: &Path) -> io::Result<File> {
        OpenOptions::new().write(true).create_new(true).open(path)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::*;

    /// A private scratch directory, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> TempDir {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let dir = std::env::temp_dir().join(format!(
                "minipaw-identity-{name}-{}-{nanos}",
                std::process::id()
            ));
            fs::create_dir(&dir).expect("create temp dir");
            chmod(&dir, 0o700);
            TempDir(dir)
        }

        fn join(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn chmod(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn write(path: &Path, text: &str, mode: u32) {
        fs::write(path, text).expect("write");
        chmod(path, mode);
    }

    fn same(a: &Identity, b: &Identity) -> bool {
        a.key == b.key && a.token == b.token
    }

    fn error(result: Result<impl fmt::Debug, Error>) -> String {
        match result.expect_err("should fail") {
            Error::Config(message) => message,
            other => panic!("not a config error: {other:?}"),
        }
    }

    #[test]
    fn round_trips() {
        let dir = TempDir::new("round-trip");
        let path = dir.join("serve.key");
        let (created, new) = Identity::load_or_create(&path).expect("create");
        assert!(new);
        let mode = fs::metadata(&path).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let text = fs::read_to_string(&path).expect("read");
        assert!(text.starts_with("minipaw identity v1\nkey "), "{text}");

        let (loaded, new) = Identity::load_or_create(&path).expect("load");
        assert!(!new && same(&created, &loaded));
        assert!(same(&created, &Identity::load(&path).expect("load")));
        // Nothing else is left in the directory.
        assert_eq!(fs::read_dir(&dir.0).expect("list").count(), 1);
    }

    #[test]
    fn save_never_replaces() {
        let dir = TempDir::new("save");
        let path = dir.join("id");
        let first = Identity::generate().expect("generate");
        first.save(&path).expect("save");
        let second = Identity::generate().expect("generate");
        assert!(error(second.save(&path)).contains("exists"));
        assert!(same(&first, &Identity::load(&path).expect("load")));
    }

    #[test]
    fn the_same_identity_makes_the_same_ticket() {
        let dir = TempDir::new("ticket");
        let path = dir.join("serve.key");
        let (a, _) = Identity::load_or_create(&path).expect("create");
        let (b, _) = Identity::load_or_create(&path).expect("load");
        assert_eq!(a.ticket(None).to_string(), b.ticket(None).to_string());
        // The default relay is left out, as a listener on it does.
        let default = crate::parse_relay(crate::DEFAULT_RELAY).expect("relay");
        assert_eq!(a.ticket(Some(&default)), a.ticket(None));
        assert!(a.ticket(None).relay().is_none());
        assert_eq!(a.ticket(None).peer(), &a.peer_id());
        let other = Identity::generate().expect("generate");
        assert_ne!(other.ticket(None), a.ticket(None));
    }

    #[test]
    fn debug_hides_the_key() {
        let identity = Identity::generate().expect("generate");
        let debug = format!("{identity:?}");
        let secret = hex(&identity.key.secret_key_bytes());
        assert!(
            !debug.contains(&secret) && debug.contains("peer_id"),
            "{debug}"
        );
    }

    #[test]
    fn shared_files_are_rejected() {
        let dir = TempDir::new("shared");
        let path = dir.join("serve.key");
        let identity = Identity::generate().expect("generate");
        for mode in [0o640, 0o604, 0o620, 0o601] {
            write(&path, &identity.encode(), mode);
            let e = error(Identity::load(&path));
            assert!(e.contains("group or others"), "{mode:o}: {e}");
            // Never replaced, either.
            assert!(Identity::load_or_create(&path).is_err());
        }
        // Wrong owner cannot be set up without root; `check_owner` covers
        // it the same way for files and directories.
    }

    #[test]
    fn malformed_files_are_rejected() {
        let dir = TempDir::new("malformed");
        let path = dir.join("serve.key");
        let good = Identity::generate().expect("generate").encode();
        let bad = [
            String::new(),
            good.trim_end().to_owned(),
            format!("{good}\n"),
            good.to_uppercase(),
            good.replace("v1", "v2"),
            good.replace("key ", "key  "),
            good.replace("token ", "token 00"),
            format!("{good}extra"),
            "x".repeat(1000),
        ];
        for text in bad {
            write(&path, &text, 0o600);
            let e = error(Identity::load(&path));
            assert!(e.contains("not a minipaw identity file"), "{text:?}: {e}");
        }
        fs::write(&path, [0xff, 0xfe]).expect("write");
        assert!(error(Identity::load(&path)).contains("not a minipaw identity file"));
    }

    #[test]
    fn symlinks_are_rejected() {
        let dir = TempDir::new("symlink");
        let target = dir.join("real.key");
        Identity::load_or_create(&target).expect("create");
        let link = dir.join("serve.key");
        symlink(&target, &link).expect("symlink");
        assert!(error(Identity::load(&link)).contains("symlink"));
        assert!(error(Identity::load_or_create(&link)).contains("symlink"));
        // A dangling one is not a missing file to create through.
        let dangling = dir.join("dangling.key");
        symlink(dir.join("nowhere"), &dangling).expect("symlink");
        assert!(error(Identity::load_or_create(&dangling)).contains("symlink"));
        assert!(!dir.join("nowhere").exists());
        // Nor is a symlinked directory accepted.
        let linked_dir = dir.join("linked");
        symlink(&dir.0, &linked_dir).expect("symlink");
        assert!(error(Identity::load(linked_dir.join("real.key"))).contains("symlink"));
    }

    #[test]
    fn a_fifo_is_rejected_quickly() {
        let dir = TempDir::new("fifo");
        let path = dir.join("serve.key");
        let made = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("mkfifo");
        assert!(made.success());
        chmod(&path, 0o600);
        let started = Instant::now();
        let e = error(Identity::load_or_create(&path));
        assert!(e.contains("not a regular file"), "{e}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn shared_directories_are_rejected() {
        let dir = TempDir::new("shared-dir");
        let path = dir.join("serve.key");
        Identity::load_or_create(&path).expect("create");
        for mode in [0o770, 0o707, 0o777, 0o1777] {
            chmod(&dir.0, mode);
            let e = error(Identity::load(&path));
            assert!(e.contains("writable by group or others"), "{mode:o}: {e}");
            let fresh = dir.join("fresh.key");
            assert!(Identity::load_or_create(&fresh).is_err());
            assert!(!fresh.exists());
        }
        // Group- or world-readable is fine.
        chmod(&dir.0, 0o755);
        Identity::load(&path).expect("load");
    }

    #[test]
    fn concurrent_creators_agree() {
        let dir = TempDir::new("race");
        let path = Arc::new(dir.join("serve.key"));
        for _ in 0..20 {
            let _ = fs::remove_file(&*path);
            let barrier = Arc::new(Barrier::new(4));
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let (path, barrier) = (path.clone(), barrier.clone());
                    std::thread::spawn(move || {
                        barrier.wait();
                        Identity::load_or_create(&*path).expect("load_or_create")
                    })
                })
                .collect();
            let results: Vec<_> = threads
                .into_iter()
                .map(|t| t.join().expect("join"))
                .collect();
            assert_eq!(results.iter().filter(|(_, new)| *new).count(), 1);
            assert!(results.iter().all(|(id, _)| same(id, &results[0].0)));
        }
        assert_eq!(fs::read_dir(&dir.0).expect("list").count(), 1);
    }

    #[test]
    fn leftover_temp_files_are_ignored() {
        let dir = TempDir::new("leftover");
        let path = dir.join("serve.key");
        let stray = dir.join(".serve.key.tmp-0011223344556677");
        write(&stray, "minipaw identity v1\nkey ", 0o600);
        let (created, new) = Identity::load_or_create(&path).expect("create");
        assert!(new);
        assert!(same(&created, &Identity::load(&path).expect("load")));
        assert!(stray.exists());
    }

    #[test]
    fn rotating_replaces_a_symlink_without_writing_through_it() {
        let dir = TempDir::new("rotate");
        let target = dir.join("target");
        write(&target, "not ours", 0o600);
        let path = dir.join("serve.key");
        symlink(&target, &path).expect("symlink");

        let fresh = Identity::generate().expect("generate");
        fresh.replace(&path).expect("replace");
        assert_eq!(fs::read_to_string(&target).expect("read"), "not ours");
        assert!(!fs::symlink_metadata(&path).expect("stat").is_symlink());
        assert!(same(&fresh, &Identity::load(&path).expect("load")));

        // Rotating a regular file changes the ticket.
        let newer = Identity::generate().expect("generate");
        newer.replace(&path).expect("replace");
        let loaded = Identity::load(&path).expect("load");
        assert!(same(&newer, &loaded));
        assert_ne!(loaded.ticket(None), fresh.ticket(None));
        assert_eq!(fs::read_dir(&dir.0).expect("list").count(), 2);
    }

    #[test]
    fn a_missing_directory_is_an_error() {
        let dir = TempDir::new("missing");
        let e = error(Identity::load_or_create(dir.join("nope/serve.key")));
        assert!(e.starts_with("identity file "), "{e}");
    }
}
