//! Who may log in: the authorized public keys, read once at startup, or
//! anyone holding the ticket under `--no-auth`.
//!
//! Key files are checked as sshd's StrictModes would: the file and every
//! directory above it must be owned by root or the serving user and not be
//! writable by anyone else, and nothing on the way may be a symlink.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read as _};
use std::os::fd::OwnedFd;
use std::path::{Component, Path, PathBuf};

use russh::keys::PublicKey;
use russh::keys::ssh_key::public::KeyData;
use rustix::fs::{FileType, Mode, OFlags};
use rustix::io::Errno;

/// The most an authorized keys file may hold.
const MAX_FILE: u64 = 1024 * 1024;

/// The key types that can log in. RSA is left out: russh accepts `ssh-rsa`
/// (SHA-1) signatures whatever it is told to prefer.
const SUPPORTED: &[&str] = &[
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
];

/// Who may log in.
#[derive(Debug)]
pub enum Policy {
    /// Holders of one of these keys.
    Keys(Vec<KeyData>),
    /// Anyone who reaches the server: the ticket is the only credential.
    NoAuth,
}

impl Policy {
    /// Whether `key` may log in.
    pub fn accepts(&self, key: &PublicKey) -> bool {
        match self {
            Policy::Keys(keys) => keys.contains(key.key_data()),
            Policy::NoAuth => false,
        }
    }

    /// Whether the `none` method logs in.
    pub fn accepts_none(&self) -> bool {
        matches!(self, Policy::NoAuth)
    }
}

/// The keys in `paths`, with a `#` warning line for each key skipped.
///
/// # Errors
///
/// A file failing the checks or not readable, a line with options or a key
/// that does not parse, or no usable key at all; the message names the file
/// and line.
pub fn load(paths: &[PathBuf], uid: u32) -> Result<(Policy, Vec<String>), String> {
    let mut keys = Vec::new();
    let mut warnings = Vec::new();
    for path in paths {
        let text =
            read(path, uid).map_err(|e| format!("authorized keys {}: {e}", path.display()))?;
        parse(&path.display().to_string(), &text, &mut keys, &mut warnings)?;
    }
    if keys.is_empty() {
        let files: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
        return Err(format!(
            "no usable key in {} (ed25519 and ecdsa keys work; or pass --no-auth)",
            files.join(", ")
        ));
    }
    Ok((Policy::Keys(keys), warnings))
}

/// Adds the keys in `text`, read from `file`, to `keys`. Each line is
/// parsed on its own: blank lines and `#` comments are skipped, and so are
/// keys of a type that cannot log in, with a warning.
fn parse(
    file: &str,
    text: &[u8],
    keys: &mut Vec<KeyData>,
    warnings: &mut Vec<String>,
) -> Result<(), String> {
    for (i, line) in text.split(|&b| b == b'\n').enumerate() {
        let at = format!("{file}:{}", i + 1);
        let line = String::from_utf8_lossy(line);
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let kind = line.split_ascii_whitespace().next().unwrap_or_default();
        if !is_key_type(kind) {
            return Err(format!(
                "{at}: key options (such as command=, from= or restrict) and \
                 cert-authority are not supported; remove them or use another file"
            ));
        }
        if !SUPPORTED.contains(&kind) {
            warnings.push(format!(
                "# warning: {at}: skipping a {kind} key (only ed25519 and ecdsa keys work)"
            ));
            continue;
        }
        let key = PublicKey::from_openssh(line).map_err(|e| format!("{at}: invalid key: {e}"))?;
        if key.algorithm().as_str() != kind {
            return Err(format!("{at}: invalid key: it is not {kind}"));
        }
        if !keys.contains(key.key_data()) {
            keys.push(key.key_data().clone());
        }
    }
    Ok(())
}

/// Whether a line's first token names a key type rather than options. Key
/// types are lower-case names like `ssh-ed25519` or
/// `sk-ssh-ed25519@openssh.com`; options start with their own names, none
/// of which begins like that.
fn is_key_type(token: &str) -> bool {
    ["ssh-", "ecdsa-", "sk-"]
        .iter()
        .any(|prefix| token.starts_with(prefix))
        && token
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.@".contains(&b))
}

/// Reads the keys file at `path` after the checks in the module docs.
fn read(path: &Path, uid: u32) -> io::Result<Vec<u8>> {
    // Resolve it first, then walk the result without following anything:
    // a symlink swapped in meanwhile fails rather than being followed.
    let path = path.canonicalize()?;
    let root = open_root()?;
    read_under(root, Path::new("/"), &path, uid)
}

/// [`read`] for `path`, which is canonical and under `top`, whose handle is
/// `top_fd`, walking from there: `top` is checked, but not what is above
/// it.
fn read_under(top_fd: OwnedFd, top: &Path, path: &Path, uid: u32) -> io::Result<Vec<u8>> {
    let rest = path
        .strip_prefix(top)
        .map_err(|_| denied(format!("it is not under {}", top.display())))?;
    let mut parts: Vec<&OsStr> = rest
        .components()
        .filter_map(|part| match part {
            Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();
    let Some(name) = parts.pop() else {
        return Err(denied("it is not a file".into()));
    };
    let mut fd = top_fd;
    let mut walked = top.to_owned();
    check(&fd, uid, &format!("directory {}", walked.display()), true)?;
    for part in parts {
        walked.push(part);
        let subject = format!("directory {}", walked.display());
        fd = open_at(&fd, part, OFlags::DIRECTORY, &subject)?;
        check(&fd, uid, &subject, true)?;
    }
    let file = open_at(&fd, name, OFlags::NONBLOCK, "the file")?;
    check(&file, uid, "the file", false)?;
    let mut text = Vec::new();
    File::from(file).take(MAX_FILE + 1).read_to_end(&mut text)?;
    if text.len() as u64 > MAX_FILE {
        return Err(denied("it is over 1 MiB".into()));
    }
    Ok(text)
}

fn open_root() -> io::Result<OwnedFd> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    Ok(rustix::fs::open("/", flags, Mode::empty())?)
}

/// Opens `name` in `dir`, without following a symlink.
fn open_at(dir: &OwnedFd, name: &OsStr, extra: OFlags, subject: &str) -> io::Result<OwnedFd> {
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | extra;
    match rustix::fs::openat(dir, name, flags, Mode::empty()) {
        Ok(fd) => Ok(fd),
        // Which of these a symlink gives depends on the system and the
        // flags.
        Err(Errno::LOOP | Errno::NOTDIR | Errno::MLINK) => {
            Err(denied(format!("{subject} changed into a symlink")))
        }
        Err(e) => Err(io::Error::new(
            io::Error::from(e).kind(),
            format!("{subject}: {e}"),
        )),
    }
}

/// Checks that `fd` is a directory (or a regular file) owned by root or
/// `uid` that no one else can write to.
fn check(fd: &OwnedFd, uid: u32, subject: &str, dir: bool) -> io::Result<()> {
    let stat = rustix::fs::fstat(fd)?;
    let kind = FileType::from_raw_mode(stat.st_mode as _);
    if dir && kind != FileType::Directory {
        return Err(denied(format!("{subject} is not a directory")));
    }
    if !dir && kind != FileType::RegularFile {
        return Err(denied(format!("{subject} is not a regular file")));
    }
    if stat.st_uid != 0 && stat.st_uid != uid {
        return Err(denied(format!(
            "{subject} is owned by neither root nor the serving user"
        )));
    }
    if stat.st_mode as u32 & 0o022 != 0 {
        return Err(denied(format!(
            "{subject} is writable by group or others (chmod go-w it)"
        )));
    }
    Ok(())
}

fn denied(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    const ED25519: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID8gGzNKaCxdHC4pXT2kKLfIWTWjDvzgugVPzENKtJt8";
    const ECDSA: &str = "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=";
    const RSA: &str = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQDDHr/jh2Jy4yALcK4JyWbVkPRaWmhck3IgCoeOO3z1e2dBowLh64QAM+Qb72pxekALga2oi4GvT+TlWNhzPH4V4JZpL/6aq0F6QhR4Dc5+ZhaBVdjHrqh/eS9IqTEVSk8jwdyl6efTaaqxAp4phRgOoHn5D1sOoYfoQ7qlntZrNw== rsa";

    fn parsed(text: &str) -> Result<(Vec<KeyData>, Vec<String>), String> {
        let mut keys = Vec::new();
        let mut warnings = Vec::new();
        parse("f", text.as_bytes(), &mut keys, &mut warnings)?;
        Ok((keys, warnings))
    }

    #[test]
    fn keys_comments_and_blank_lines() {
        let text = format!("# mine\n\n  {ED25519} me@laptop\n{ECDSA}\r\n{ED25519} again\n");
        let (keys, warnings) = parsed(&text).expect("parse");
        // The same key twice is one key.
        assert_eq!(keys.len(), 2);
        assert!(warnings.is_empty());
        // Keys compare on their data, whatever the comment.
        let policy = Policy::Keys(keys);
        let key = PublicKey::from_openssh(&format!("{ED25519} other comment")).expect("key");
        assert!(policy.accepts(&key));
        assert!(!policy.accepts_none());
        let other = super::super::config::host_key(&[1; 32])
            .public_key()
            .clone();
        assert!(!policy.accepts(&other));
        assert!(Policy::NoAuth.accepts_none() && !Policy::NoAuth.accepts(&key));
    }

    #[test]
    fn options_are_an_error_naming_the_line() {
        for line in [
            format!("restrict {ED25519}"),
            format!("command=\"ls\" {ED25519}"),
            format!("from=\"10.0.0.1\",no-pty {ED25519}"),
            format!("cert-authority {ED25519}"),
            format!("no-port-forwarding {ED25519}"),
            format!("environment=\"A=b\" {ED25519}"),
        ] {
            let e = parsed(&format!("# x\n{line}\n")).unwrap_err();
            assert!(e.starts_with("f:2: key options"), "{line}: {e}");
        }
    }

    #[test]
    fn unsupported_types_are_skipped_with_a_warning() {
        let text = format!("{RSA}\nssh-dss AAAA x\nsk-ssh-ed25519@openssh.com AAAA y\n{ED25519}\n");
        let (keys, warnings) = parsed(&text).expect("parse");
        assert_eq!(keys.len(), 1);
        assert_eq!(
            warnings,
            [
                "# warning: f:1: skipping a ssh-rsa key (only ed25519 and ecdsa keys work)",
                "# warning: f:2: skipping a ssh-dss key (only ed25519 and ecdsa keys work)",
                "# warning: f:3: skipping a sk-ssh-ed25519@openssh.com key (only ed25519 and ecdsa keys work)",
            ]
        );
        // Nothing usable is an error at load time, not here.
        assert!(parsed(RSA).expect("parse").0.is_empty());
    }

    #[test]
    fn bad_keys_are_an_error() {
        for line in [
            "ssh-ed25519 not*base64",
            "ssh-ed25519",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID8gGzNK",
        ] {
            let e = parsed(line).unwrap_err();
            assert!(e.starts_with("f:1: invalid key"), "{line}: {e}");
        }
        // A blob of another type than the line says.
        let swapped = ECDSA.replacen("ecdsa-sha2-nistp256", "ssh-ed25519", 1);
        assert!(
            parsed(&swapped)
                .unwrap_err()
                .starts_with("f:1: invalid key")
        );
    }

    /// A private directory (0700, ours) under the workspace's `target`,
    /// removed on drop. The path tests walk from it, as [`read`] walks from
    /// `/`: the directories above it may well be shared (a umask of 002, a
    /// sticky /tmp).
    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Dir {
            let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/sshd-tests");
            let path = base.join(format!("{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("mkdir");
            chmod(&path, 0o700);
            Dir(path.canonicalize().expect("canonical"))
        }

        fn write(&self, name: &str, text: &str, mode: u32) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, text).expect("write");
            chmod(&path, mode);
            path
        }

        /// Reads `path`, walking from this directory.
        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            self.read_as(path, uid())
        }

        fn read_as(&self, path: &Path, uid: u32) -> io::Result<Vec<u8>> {
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
            let top = rustix::fs::open(&self.0, flags, Mode::empty()).expect("open");
            read_under(top, &self.0, path, uid)
        }

        fn error(&self, path: &Path) -> String {
            self.read(path).expect_err("refused").to_string()
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn chmod(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn uid() -> u32 {
        rustix::process::geteuid().as_raw()
    }

    #[test]
    fn the_path_policy() {
        let dir = Dir::new("auth-policy");
        let good = dir.write("good", &format!("{ED25519}\n"), 0o644);
        assert_eq!(
            dir.read(&good).expect("read"),
            format!("{ED25519}\n").as_bytes()
        );
        // Writable by others.
        let shared = dir.write("shared", ED25519, 0o664);
        let e = dir.error(&shared);
        assert!(e.contains("the file is writable by group or others"), "{e}");
        // Owned by neither root nor the serving user: the same file, served
        // as another user.
        if uid() != 0 {
            let e = dir.read_as(&good, uid() + 1).unwrap_err().to_string();
            assert!(
                e.contains("is owned by neither root nor the serving user"),
                "{e}"
            );
        }
        // A writable ancestor.
        let sub = dir.0.join("sub");
        std::fs::create_dir(&sub).expect("mkdir");
        std::fs::write(sub.join("keys"), ED25519).expect("write");
        chmod(&sub.join("keys"), 0o600);
        chmod(&sub, 0o777);
        let e = dir.error(&sub.join("keys"));
        assert!(
            e.contains(&format!("directory {} is writable", sub.display())),
            "{e}"
        );
        chmod(&sub, 0o700);
        assert!(dir.read(&sub.join("keys")).is_ok());
        // A symlink, to the file or on the way to it, is not followed.
        let link = dir.0.join("link");
        std::os::unix::fs::symlink(&good, &link).expect("symlink");
        assert!(dir.error(&link).contains("symlink"));
        std::os::unix::fs::symlink(&sub, dir.0.join("via")).expect("symlink");
        let e = dir.error(&dir.0.join("via/keys"));
        assert!(e.contains("symlink"), "{e}");
        // A FIFO is refused without blocking.
        let fifo = dir.0.join("fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo");
        assert!(status.success());
        assert!(dir.error(&fifo).contains("not a regular file"));
        // A directory is not a file.
        assert!(dir.error(&sub).contains("not a regular file"));
        // Exactly 1 MiB is fine, a byte more is not.
        let head = format!("{ED25519}\n#");
        let exact = head.clone() + &"x".repeat(1024 * 1024 - head.len());
        assert!(dir.read(&dir.write("exact", &exact, 0o600)).is_ok());
        let big = dir.write("big", &(exact + "x"), 0o600);
        assert!(dir.error(&big).contains("over 1 MiB"));
        // Missing.
        assert!(dir.error(&dir.0.join("missing")).contains("No such file"));
    }

    #[test]
    fn loading_resolves_symlinks_first() {
        let dir = Dir::new("auth-load");
        let good = dir.write("good", &format!("{RSA}\n{ED25519}\n"), 0o600);
        let link = dir.0.join("link");
        std::os::unix::fs::symlink(&good, &link).expect("symlink");
        let rsa = dir.write("rsa", RSA, 0o600);
        match load(&[link], uid()) {
            Ok((Policy::Keys(keys), warnings)) => {
                assert_eq!(keys.len(), 1);
                assert_eq!(warnings.len(), 1);
                assert!(warnings[0].contains(&format!("{}:1: skipping", good.display())));
            }
            Ok(_) => panic!("not keys"),
            // Only where this checkout's ancestors pass the checks.
            Err(e) => assert!(e.contains("writable by group or others"), "{e}"),
        }
        let e = load(std::slice::from_ref(&rsa), uid()).unwrap_err();
        assert!(
            e.starts_with("no usable key in ") || e.contains("writable by group or others"),
            "{e}"
        );
        let e = load(&[dir.0.join("missing")], uid()).unwrap_err();
        assert!(
            e.starts_with("authorized keys ") && e.contains("No such file"),
            "{e}"
        );
    }
}
