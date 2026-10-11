//! The `sftp` subsystem: an SFTP v3 server on the serving user's files, the
//! channel's program in place of a process.
//!
//! The protocol types and the request methods are russh-sftp's ([`Packet`]
//! and [`Handler`]), but the loop is ours rather than its detached
//! `server::run`: it runs inside the channel's supervisor, which cancels it
//! with the channel, and it ends at the first framing or transport error.
//! It reads the channel's capped input buffer through [`InputReader`], and
//! a request's length is checked before anything is allocated for it. Each
//! request's layout is checked on the wire before russh-sftp decodes it,
//! since its decoder lets missing fields and trailing bytes through.
//!
//! Every file operation runs on the blocking pool through [`Jobs`], at most
//! [`MAX_JOBS`] at once across the server: a stuck filesystem can hold a
//! job past its session's end, but cannot use up the pool.
//!
//! Paths are relative to the serving user's home, and absolute paths reach
//! anywhere that user can: this is not a sandbox, any more than a shell is.
//! Names must be UTF-8 (russh-sftp's types are `String`s): a request with
//! another name is refused, and listings leave such names out, so no name
//! is ever changed on its way to or from the filesystem.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use bytes::Bytes;
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttr, FileAttributes, Handle, Name, OpenFlags, Packet, Status,
    StatusCode, Version,
};
use russh_sftp::server::{Handler, StatusReply};
use rustix::fs::{AtFlags, Dir, Mode, OFlags, RenameFlags, Stat, Timespec, Timestamps};
use rustix::io::Errno;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};
use tokio::sync::Semaphore;

use super::channel::Input;
use super::config::CHUNK;

/// The most a read returns, and the most a write carries.
pub const MAX_READ: u32 = 256 * 1024;

/// The longest request taken: a write of [`MAX_READ`] and room for its
/// header (type, id, handle and offset).
pub const MAX_FRAME: u32 = MAX_READ + 1024;

/// How many files and directories a session may have open at once.
pub const MAX_HANDLES: usize = 256;

/// How many names one `readdir` returns.
pub const READDIR_PAGE: usize = 128;

/// How many file operations may run at once, across every SFTP session of
/// the server. A session runs one at a time; the rest is room for jobs
/// stuck in the filesystem after their session ended.
pub const MAX_JOBS: usize = 64;

const SSH_FXP_INIT: u8 = 1;
const SSH_FXP_OPEN: u8 = 3;
const SSH_FXP_CLOSE: u8 = 4;
const SSH_FXP_READ: u8 = 5;
const SSH_FXP_WRITE: u8 = 6;
const SSH_FXP_LSTAT: u8 = 7;
const SSH_FXP_FSTAT: u8 = 8;
const SSH_FXP_SETSTAT: u8 = 9;
const SSH_FXP_FSETSTAT: u8 = 10;
const SSH_FXP_OPENDIR: u8 = 11;
const SSH_FXP_READDIR: u8 = 12;
const SSH_FXP_REMOVE: u8 = 13;
const SSH_FXP_MKDIR: u8 = 14;
const SSH_FXP_RMDIR: u8 = 15;
const SSH_FXP_REALPATH: u8 = 16;
const SSH_FXP_STAT: u8 = 17;
const SSH_FXP_RENAME: u8 = 18;
const SSH_FXP_READLINK: u8 = 19;
const SSH_FXP_SYMLINK: u8 = 20;
const SSH_FXP_EXTENDED: u8 = 200;

/// Why the loop ended early.
#[derive(Debug)]
pub enum Error {
    /// A request claimed more than [`MAX_FRAME`] bytes.
    TooLong(u32),
    /// A request of no bytes at all.
    Empty,
    /// The input ended inside a request.
    Truncated,
    /// A request of a known type did not have its type's layout, or its
    /// reply did not encode.
    Malformed(String),
    /// The channel failed.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TooLong(len) => write!(f, "a {len}-byte request, over the {MAX_FRAME} limit"),
            Error::Empty => f.write_str("an empty request"),
            Error::Truncated => f.write_str("the input ended inside a request"),
            Error::Malformed(e) => write!(f, "a malformed request: {e}"),
            Error::Io(e) => write!(f, "the channel failed: {e}"),
        }
    }
}

/// Serves SFTP requests from `input`, replying on `output` in pieces of at
/// most [`CHUNK`], until the client's EOF between requests (`Ok`), or the
/// first framing or transport error.
pub async fn serve(
    fs: &mut Fs,
    mut input: impl AsyncRead + Unpin,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error> {
    while let Some(frame) = read_frame(&mut input).await? {
        let reply = answer(fs, frame).await?;
        let reply = Bytes::try_from(reply).map_err(|e| Error::Malformed(e.to_string()))?;
        for piece in reply.chunks(CHUNK) {
            output.write_all(piece).await.map_err(Error::Io)?;
        }
    }
    Ok(())
}

/// The next request, without its length; `None` at EOF before one starts.
/// The length is checked against [`MAX_FRAME`] before the request is read.
async fn read_frame(input: &mut (impl AsyncRead + Unpin)) -> Result<Option<Bytes>, Error> {
    let mut len = [0; 4];
    if input.read(&mut len[..1]).await.map_err(Error::Io)? == 0 {
        return Ok(None);
    }
    read_exact(input, &mut len[1..]).await?;
    let len = u32::from_be_bytes(len);
    if len > MAX_FRAME {
        return Err(Error::TooLong(len));
    }
    if len == 0 {
        return Err(Error::Empty);
    }
    let mut frame = vec![0; len as usize];
    read_exact(input, &mut frame).await?;
    Ok(Some(Bytes::from(frame)))
}

async fn read_exact(input: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> Result<(), Error> {
    match input.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(Error::Truncated),
        Err(e) => Err(Error::Io(e)),
    }
}

/// The reply to one request (type and payload, without the length).
async fn answer(fs: &mut Fs, mut frame: Bytes) -> Result<Packet, Error> {
    let kind = frame[0];
    let Some(shape) = Shape::of(&frame) else {
        if is_request(kind) {
            return Err(Error::Malformed(format!("type {kind}")));
        }
        let id = frame
            .get(1..5)
            .map_or(0, |id| u32::from_be_bytes(id.try_into().expect("4 bytes")));
        return Ok(status(id, StatusCode::OpUnsupported.into()));
    };
    if !shape.utf8 {
        let reply = StatusCode::Failure.with_message("names must be UTF-8");
        return Ok(status(shape.id, reply));
    }
    // russh-sftp's attributes drop the extended pairs; the raw flags still
    // show them.
    if matches!(kind, SSH_FXP_SETSTAT | SSH_FXP_FSETSTAT)
        && shape.attrs.unwrap_or(0) & !(FileAttr::PERMISSIONS | FileAttr::ACMODTIME).bits() != 0
    {
        return Ok(status(shape.id, unsupported_attrs()));
    }
    let request = Packet::try_from(&mut frame).map_err(|e| Error::Malformed(e.to_string()))?;
    Ok(dispatch(fs, request).await)
}

/// The request types a client may send (SSH_FXP_INIT, OPEN through
/// SYMLINK, and EXTENDED); anything else gets `OpUnsupported`.
fn is_request(kind: u8) -> bool {
    matches!(
        kind,
        SSH_FXP_INIT | SSH_FXP_OPEN..=SSH_FXP_SYMLINK | SSH_FXP_EXTENDED
    )
}

/// What a request's layout says, read from the wire before russh-sftp
/// decodes it: its decoder takes a request cut short after a field as
/// having empty attributes, ignores trailing bytes, and replaces bytes that
/// are not UTF-8 in names.
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    id: u32,
    /// The attribute flags, for a request with attributes.
    attrs: Option<u32>,
    /// Whether every name in it is UTF-8.
    utf8: bool,
}

impl Shape {
    /// `None` for a request that is not a request type, or does not have
    /// its type's layout to the last byte.
    fn of(frame: &[u8]) -> Option<Shape> {
        let mut wire = Wire {
            rest: &frame[1..],
            utf8: true,
        };
        let kind = frame[0];
        if kind == SSH_FXP_INIT {
            wire.u32()?;
            // Extension pairs, to the end.
            while !wire.rest.is_empty() {
                wire.bytes()?;
                wire.bytes()?;
            }
            return Some(Shape {
                id: 0,
                attrs: None,
                utf8: true,
            });
        }
        let id = wire.u32()?;
        let mut attrs = None;
        match kind {
            SSH_FXP_OPEN => {
                wire.name()?;
                wire.u32()?;
                attrs = Some(wire.attrs()?);
            }
            SSH_FXP_SETSTAT | SSH_FXP_FSETSTAT | SSH_FXP_MKDIR => {
                wire.name()?;
                attrs = Some(wire.attrs()?);
            }
            SSH_FXP_READ => {
                wire.name()?;
                wire.u64()?;
                wire.u32()?;
            }
            SSH_FXP_WRITE => {
                wire.name()?;
                wire.u64()?;
                wire.bytes()?;
            }
            SSH_FXP_CLOSE | SSH_FXP_LSTAT | SSH_FXP_FSTAT | SSH_FXP_OPENDIR | SSH_FXP_READDIR
            | SSH_FXP_REMOVE | SSH_FXP_RMDIR | SSH_FXP_REALPATH | SSH_FXP_STAT
            | SSH_FXP_READLINK => {
                wire.name()?;
            }
            SSH_FXP_RENAME | SSH_FXP_SYMLINK => {
                wire.name()?;
                wire.name()?;
            }
            SSH_FXP_EXTENDED => {
                // Its name, then whatever the extension takes.
                wire.bytes()?;
                wire.rest = &[];
            }
            _ => return None,
        }
        wire.rest.is_empty().then_some(Shape {
            id,
            attrs,
            utf8: wire.utf8,
        })
    }
}

/// A request's fields, read in order.
struct Wire<'a> {
    rest: &'a [u8],
    /// Cleared by a name that is not UTF-8.
    utf8: bool,
}

impl<'a> Wire<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let rest: &'a [u8] = self.rest;
        let (taken, rest) = rest.split_at_checked(n)?;
        self.rest = rest;
        Some(taken)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u32()?).ok()?;
        self.take(len)
    }

    /// A path, handle or link target.
    fn name(&mut self) -> Option<()> {
        let name = self.bytes()?;
        if std::str::from_utf8(name).is_err() {
            self.utf8 = false;
        }
        Some(())
    }

    /// An attributes block: its flags, then the fields they say it has.
    fn attrs(&mut self) -> Option<u32> {
        let flags = self.u32()?;
        let has = |flag: FileAttr| flags & flag.bits() != 0;
        if has(FileAttr::SIZE) {
            self.u64()?;
        }
        if has(FileAttr::UIDGID) {
            self.u32()?;
            self.u32()?;
        }
        if has(FileAttr::PERMISSIONS) {
            self.u32()?;
        }
        if has(FileAttr::ACMODTIME) {
            self.u32()?;
            self.u32()?;
        }
        if has(FileAttr::EXTENDED) {
            // Each pair takes at least 8 bytes, so this ends with the frame.
            for _ in 0..self.u32()? {
                self.bytes()?;
                self.bytes()?;
            }
        }
        Some(flags)
    }
}

fn status(id: u32, reply: StatusReply) -> Packet {
    let StatusReply {
        status_code,
        error_message,
        language_tag,
    } = reply;
    Packet::Status(Status {
        id,
        status_code,
        error_message: error_message.unwrap_or_else(|| status_code.to_string()),
        language_tag: language_tag.unwrap_or_else(|| "en-US".to_owned()),
    })
}

/// Calls the handler's method for `request`, as russh-sftp's own loop does.
async fn dispatch(fs: &mut Fs, request: Packet) -> Packet {
    let id = request.get_request_id();
    let reply = match request {
        Packet::Init(r) => fs.init(r.version, r.extensions).await.map(Packet::from),
        Packet::Open(r) => fs
            .open(r.id, r.filename, r.pflags, r.attrs)
            .await
            .map(Packet::from),
        Packet::Close(r) => fs.close(r.id, r.handle).await.map(Packet::from),
        Packet::Read(r) => fs
            .read(r.id, r.handle, r.offset, r.len)
            .await
            .map(Packet::from),
        Packet::Write(r) => fs
            .write(r.id, r.handle, r.offset, r.data)
            .await
            .map(Packet::from),
        Packet::Lstat(r) => fs.lstat(r.id, r.path).await.map(Packet::from),
        Packet::Fstat(r) => fs.fstat(r.id, r.handle).await.map(Packet::from),
        Packet::SetStat(r) => fs.setstat(r.id, r.path, r.attrs).await.map(Packet::from),
        Packet::FSetStat(r) => fs.fsetstat(r.id, r.handle, r.attrs).await.map(Packet::from),
        Packet::OpenDir(r) => fs.opendir(r.id, r.path).await.map(Packet::from),
        Packet::ReadDir(r) => fs.readdir(r.id, r.handle).await.map(Packet::from),
        Packet::Remove(r) => fs.remove(r.id, r.filename).await.map(Packet::from),
        Packet::MkDir(r) => fs.mkdir(r.id, r.path, r.attrs).await.map(Packet::from),
        Packet::RmDir(r) => fs.rmdir(r.id, r.path).await.map(Packet::from),
        Packet::RealPath(r) => fs.realpath(r.id, r.path).await.map(Packet::from),
        Packet::Stat(r) => fs.stat(r.id, r.path).await.map(Packet::from),
        Packet::Rename(r) => fs
            .rename(r.id, r.oldpath, r.newpath)
            .await
            .map(Packet::from),
        Packet::ReadLink(r) => fs.readlink(r.id, r.path).await.map(Packet::from),
        Packet::Symlink(r) => fs
            .symlink(r.id, r.linkpath, r.targetpath)
            .await
            .map(Packet::from),
        Packet::Extended(r) => fs.extended(r.id, r.request, r.data).await,
        _ => Err(fs.unimplemented()),
    };
    reply.unwrap_or_else(|e| status(id, e))
}

/// A pending [`Input::pop`].
type Pop = Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send>>;

/// The channel's input as an [`AsyncRead`]: SFTP reads the same capped
/// buffer the `data` callback fills, never a russh read half.
pub struct InputReader {
    input: Arc<Input>,
    piece: Vec<u8>,
    /// How much of `piece` was read.
    at: usize,
    next: Option<Pop>,
}

impl InputReader {
    pub fn new(input: Arc<Input>) -> InputReader {
        InputReader {
            input,
            piece: Vec::new(),
            at: 0,
            next: None,
        }
    }
}

impl AsyncRead for InputReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.at == this.piece.len() {
            let input = &this.input;
            let next = this.next.get_or_insert_with(|| {
                let input = input.clone();
                Box::pin(async move { input.pop().await })
            });
            let piece = std::task::ready!(next.as_mut().poll(cx));
            this.next = None;
            match piece {
                Some(piece) => {
                    this.piece = piece;
                    this.at = 0;
                }
                // EOF, or the input was closed.
                None => return Poll::Ready(Ok(())),
            }
        }
        let n = buf.remaining().min(this.piece.len() - this.at);
        buf.put_slice(&this.piece[this.at..this.at + n]);
        this.at += n;
        Poll::Ready(Ok(()))
    }
}

/// Runs a session's file operations on the blocking pool, each holding one
/// of the server's [`MAX_JOBS`] permits until it is really done.
///
/// A syscall cannot be aborted: when the session is torn down, a job
/// already running finishes by itself and only then gives its permit back,
/// and the teardown does not wait for it. A job queued but not yet started
/// does nothing once its session is gone.
pub struct Jobs {
    permits: Arc<Semaphore>,
    /// Set when the session goes.
    gone: Arc<AtomicBool>,
}

impl Jobs {
    pub fn new(permits: Arc<Semaphore>) -> Jobs {
        Jobs {
            permits,
            gone: Arc::default(),
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> Result<T, StatusReply> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| StatusCode::Failure.with_message(e.to_string()))?;
        let gone = self.gone.clone();
        let done = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if gone.load(Ordering::Acquire) {
                return Err(io::Error::other("the session ended"));
            }
            job()
        });
        done.await
            .map_err(|e| StatusCode::Failure.with_message(e.to_string()))?
            .map_err(failed)
    }
}

impl Drop for Jobs {
    fn drop(&mut self) {
        self.gone.store(true, Ordering::Release);
    }
}

/// What a handle names: an open file or directory, by descriptor, so a
/// rename under it changes nothing.
struct Open {
    fd: Arc<OwnedFd>,
    /// A directory's listing, read a page at a time.
    entries: Option<Arc<Mutex<Dir>>>,
}

/// The SFTP requests, on the serving user's files.
pub struct Fs {
    /// Where relative paths start.
    home: PathBuf,
    handles: HashMap<String, Open>,
    next: u64,
    jobs: Jobs,
}

impl Fs {
    pub fn new(home: PathBuf, jobs: Jobs) -> Fs {
        Fs {
            home,
            handles: HashMap::new(),
            next: 0,
            jobs,
        }
    }

    /// `path` from the client: relative to home, or absolute.
    fn path(&self, path: &str) -> PathBuf {
        self.home.join(path)
    }

    fn room(&self) -> Result<(), StatusReply> {
        if self.handles.len() >= MAX_HANDLES {
            return Err(StatusCode::Failure.with_message("too many open handles"));
        }
        Ok(())
    }

    fn add(&mut self, open: Open) -> String {
        self.next += 1;
        let handle = self.next.to_string();
        self.handles.insert(handle.clone(), open);
        handle
    }

    fn fd(&self, handle: &str) -> Result<Arc<OwnedFd>, StatusReply> {
        let open = self.handles.get(handle).ok_or_else(bad_handle)?;
        Ok(open.fd.clone())
    }
}

fn bad_handle() -> StatusReply {
    StatusCode::Failure.with_message("no such handle")
}

fn unsupported_attrs() -> StatusReply {
    StatusCode::OpUnsupported.with_message("only the mode and times can be set")
}

/// An I/O error as SFTP reports it.
fn failed(e: io::Error) -> StatusReply {
    let code = match e.kind() {
        io::ErrorKind::NotFound => StatusCode::NoSuchFile,
        io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        _ => StatusCode::Failure,
    };
    code.with_message(e.to_string())
}

fn ok(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "Ok".to_owned(),
        language_tag: "en-US".to_owned(),
    }
}

/// A name for the client, which must be UTF-8 to be sent unchanged.
fn utf8(name: OsString) -> io::Result<String> {
    name.into_string().map_err(|name| {
        io::Error::other(format!(
            "{} is not UTF-8",
            String::from_utf8_lossy(name.as_bytes())
        ))
    })
}

/// The permission bits of `mode` as rustix takes them (`mode_t` is 16 bits
/// on macOS).
fn mode(mode: u32) -> Mode {
    Mode::from_bits_truncate(narrow(mode & 0o7777))
}

fn narrow<T: TryFrom<u32> + Default>(n: u32) -> T {
    T::try_from(n).unwrap_or_default()
}

/// `stat` fields widened, whatever their width on this platform.
fn wide<T: Into<i64>>(n: T) -> i64 {
    n.into()
}

fn bits<T: Into<u32>>(n: T) -> u32 {
    n.into()
}

fn seconds(t: i64) -> u32 {
    t.clamp(0, i64::from(u32::MAX)) as u32
}

/// `stat` as SFTP attributes: the raw mode, with its file type.
fn attrs(stat: &Stat) -> FileAttributes {
    FileAttributes {
        size: Some(u64::try_from(wide(stat.st_size)).unwrap_or(0)),
        uid: Some(stat.st_uid),
        user: None,
        gid: Some(stat.st_gid),
        group: None,
        permissions: Some(bits(stat.st_mode)),
        atime: Some(seconds(wide(stat.st_atime))),
        mtime: Some(seconds(wide(stat.st_mtime))),
    }
}

/// A directory entry, with an `ls -l` style long name.
fn entry(name: String, stat: &Stat) -> File {
    let mut file = File::new(name, attrs(stat));
    let kind = match bits(stat.st_mode) & 0o170_000 {
        0o040_000 => "d",
        0o120_000 => "l",
        0o010_000 => "p",
        0o140_000 => "s",
        0o020_000 => "c",
        0o060_000 => "b",
        _ => "-",
    };
    file.longname.replace_range(..1, kind);
    file
}

/// A single name, as `realpath` and `readlink` answer.
fn name(id: u32, name: String) -> Name {
    Name {
        id,
        files: vec![File {
            filename: name.clone(),
            longname: name,
            attrs: FileAttributes::empty(),
        }],
    }
}

/// The open(2) flags for SFTP's, mapped directly: Rust's `OpenOptions`
/// refuses to create a file opened only for reading. Never blocking, so a
/// FIFO cannot hold a job forever.
fn open_flags(pflags: OpenFlags) -> OFlags {
    let read = pflags.contains(OpenFlags::READ);
    let write = pflags.contains(OpenFlags::WRITE);
    let mut flags = match (read, write) {
        (true, true) => OFlags::RDWR,
        (false, true) => OFlags::WRONLY,
        _ => OFlags::RDONLY,
    } | OFlags::NONBLOCK
        | OFlags::CLOEXEC
        | OFlags::NOCTTY;
    for (sftp, unix) in [
        (OpenFlags::APPEND, OFlags::APPEND),
        (OpenFlags::CREATE, OFlags::CREATE),
        (OpenFlags::TRUNCATE, OFlags::TRUNC),
        (OpenFlags::EXCLUDE, OFlags::EXCL),
    ] {
        if pflags.contains(sftp) {
            flags |= unix;
        }
    }
    flags
}

/// The canonical form of `path`: its longest existing prefix resolved,
/// symlinks and all, and the rest, which does not exist, with `.` and `..`
/// worked out by name. Only a missing component is passed over.
fn resolve(path: &Path) -> io::Result<PathBuf> {
    let parts: Vec<Component<'_>> = path.components().collect();
    for split in (1..=parts.len()).rev() {
        let prefix: PathBuf = parts[..split].iter().collect();
        match std::fs::canonicalize(&prefix) {
            Ok(mut real) => {
                for part in &parts[split..] {
                    match part {
                        Component::Normal(name) => real.push(name),
                        Component::ParentDir => {
                            real.pop();
                        }
                        Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
                    }
                }
                return Ok(real);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::from(io::ErrorKind::NotFound))
}

/// Renames `from` to `to` unless `to` exists, atomically. A filesystem
/// without no-replace renames (EINVAL, ENOSYS, ENOTSUP) gets what OpenSSH's
/// sftp-server does: a hard link and an unlink, which is atomic too, for
/// anything but a directory; a directory gets the original error.
fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    let cwd = rustix::fs::CWD;
    let err = match rustix::fs::renameat_with(cwd, from, cwd, to, RenameFlags::NOREPLACE) {
        Ok(()) => return Ok(()),
        Err(e @ (Errno::INVAL | Errno::NOSYS | Errno::NOTSUP)) => e,
        Err(e) => return Err(e.into()),
    };
    let is_dir =
        rustix::fs::lstat(from).is_ok_and(|stat| bits(stat.st_mode) & 0o170_000 == 0o040_000);
    if is_dir {
        return Err(err.into());
    }
    rustix::fs::linkat(cwd, from, cwd, to, AtFlags::empty())?;
    rustix::fs::unlinkat(cwd, from, AtFlags::empty())?;
    Ok(())
}

/// What a `setstat` asks for: the mode and the times, nothing else.
fn settable(attrs: &FileAttributes) -> Result<(Option<Mode>, Option<Timestamps>), StatusReply> {
    if attrs.size.is_some() || attrs.uid.is_some() || attrs.gid.is_some() {
        return Err(unsupported_attrs());
    }
    let time = |t: u32| Timespec {
        tv_sec: t.into(),
        tv_nsec: 0,
    };
    let times = match (attrs.atime, attrs.mtime) {
        (Some(atime), Some(mtime)) => Some(Timestamps {
            last_access: time(atime),
            last_modification: time(mtime),
        }),
        _ => None,
    };
    Ok((attrs.permissions.map(mode), times))
}

impl Handler for Fs {
    type Error = StatusReply;

    fn unimplemented(&self) -> StatusReply {
        StatusCode::OpUnsupported.into()
    }

    /// Version 3, with no extensions. Not `limits@openssh.com` in
    /// particular: OpenSSH's client would then send 64 requests of up to
    /// 256 KiB at once, past the channel's input cap.
    async fn init(
        &mut self,
        _version: u32,
        _extensions: HashMap<String, String>,
    ) -> Result<Version, StatusReply> {
        Ok(Version::new())
    }

    /// Created with the client's mode less the umask, as sftp-server does.
    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        attrs: FileAttributes,
    ) -> Result<Handle, StatusReply> {
        self.room()?;
        let path = self.path(&filename);
        let flags = open_flags(pflags);
        let mode = mode(attrs.permissions.unwrap_or(0o666));
        let fd = self
            .jobs
            .run(move || Ok(rustix::fs::open(&path, flags, mode)?))
            .await?;
        let handle = self.add(Open {
            fd: Arc::new(fd),
            entries: None,
        });
        Ok(Handle { id, handle })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, StatusReply> {
        self.handles.remove(&handle).ok_or_else(bad_handle)?;
        Ok(ok(id))
    }

    /// At most [`MAX_READ`] bytes, whatever the client asked for.
    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, StatusReply> {
        let fd = self.fd(&handle)?;
        let len = len.min(MAX_READ) as usize;
        let data = self
            .jobs
            .run(move || {
                let mut data = vec![0; len];
                let mut got = 0;
                while got < len {
                    match rustix::io::pread(&*fd, &mut data[got..], offset + got as u64) {
                        Ok(0) => break,
                        Ok(n) => got += n,
                        Err(Errno::INTR) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                data.truncate(got);
                Ok(data)
            })
            .await?;
        if data.is_empty() && len > 0 {
            return Err(StatusCode::Eof.into());
        }
        Ok(Data { id, data })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, StatusReply> {
        let fd = self.fd(&handle)?;
        self.jobs
            .run(move || {
                let mut sent = 0;
                while sent < data.len() {
                    match rustix::io::pwrite(&*fd, &data[sent..], offset + sent as u64) {
                        Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                        Ok(n) => sent += n,
                        Err(Errno::INTR) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(())
            })
            .await?;
        Ok(ok(id))
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, StatusReply> {
        let path = self.path(&path);
        let stat = self.jobs.run(move || Ok(rustix::fs::lstat(&path)?)).await?;
        Ok(Attrs {
            id,
            attrs: attrs(&stat),
        })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, StatusReply> {
        let fd = self.fd(&handle)?;
        let stat = self.jobs.run(move || Ok(rustix::fs::fstat(&*fd)?)).await?;
        Ok(Attrs {
            id,
            attrs: attrs(&stat),
        })
    }

    /// The mode and the times only; asking for anything else (size, owner,
    /// extended attributes) changes nothing and gets `OpUnsupported`.
    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusReply> {
        let (mode, times) = settable(&attrs)?;
        let path = self.path(&path);
        self.jobs
            .run(move || {
                if let Some(mode) = mode {
                    rustix::fs::chmod(&path, mode)?;
                }
                if let Some(times) = times {
                    rustix::fs::utimensat(rustix::fs::CWD, &path, &times, AtFlags::empty())?;
                }
                Ok(())
            })
            .await?;
        Ok(ok(id))
    }

    /// As `setstat`, on the open file or directory itself.
    async fn fsetstat(
        &mut self,
        id: u32,
        handle: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusReply> {
        let (mode, times) = settable(&attrs)?;
        let fd = self.fd(&handle)?;
        self.jobs
            .run(move || {
                if let Some(mode) = mode {
                    rustix::fs::fchmod(&*fd, mode)?;
                }
                if let Some(times) = times {
                    rustix::fs::futimens(&*fd, &times)?;
                }
                Ok(())
            })
            .await?;
        Ok(ok(id))
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, StatusReply> {
        self.room()?;
        let path = self.path(&path);
        let (fd, entries) = self
            .jobs
            .run(move || {
                let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
                let fd = rustix::fs::open(&path, flags, Mode::empty())?;
                let entries = Dir::read_from(&fd)?;
                Ok((fd, entries))
            })
            .await?;
        let handle = self.add(Open {
            fd: Arc::new(fd),
            entries: Some(Arc::new(Mutex::new(entries))),
        });
        Ok(Handle { id, handle })
    }

    /// The next [`READDIR_PAGE`] names at most, then `Eof`. Names that are
    /// not UTF-8 are left out rather than changed.
    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, StatusReply> {
        let open = self.handles.get(&handle).ok_or_else(bad_handle)?;
        let (fd, entries) = match &open.entries {
            Some(entries) => (open.fd.clone(), entries.clone()),
            None => return Err(bad_handle()),
        };
        let files = self
            .jobs
            .run(move || {
                let mut entries = entries.lock().unwrap_or_else(PoisonError::into_inner);
                let mut files = Vec::new();
                while files.len() < READDIR_PAGE {
                    let Some(dirent) = entries.read() else {
                        break;
                    };
                    let dirent = dirent?;
                    let raw = dirent.file_name();
                    if matches!(raw.to_bytes(), b"." | b"..") {
                        continue;
                    }
                    let Ok(name) = raw.to_str() else {
                        log::debug!("sftp: leaving out a name that is not UTF-8: {raw:?}");
                        continue;
                    };
                    // Gone since it was listed: leave it out.
                    let Ok(stat) = rustix::fs::statat(&*fd, raw, AtFlags::SYMLINK_NOFOLLOW) else {
                        continue;
                    };
                    files.push(entry(name.to_owned(), &stat));
                }
                Ok(files)
            })
            .await?;
        if files.is_empty() {
            return Err(StatusCode::Eof.into());
        }
        Ok(Name { id, files })
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, StatusReply> {
        let path = self.path(&filename);
        self.jobs.run(move || std::fs::remove_file(path)).await?;
        Ok(ok(id))
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusReply> {
        let path = self.path(&path);
        let mode = attrs.permissions.map_or(0o777, |mode| mode & 0o7777);
        self.jobs
            .run(move || std::fs::DirBuilder::new().mode(mode).create(path))
            .await?;
        Ok(ok(id))
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, StatusReply> {
        let path = self.path(&path);
        self.jobs.run(move || std::fs::remove_dir(path)).await?;
        Ok(ok(id))
    }

    /// The canonical path, following symlinks as far as the path exists.
    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, StatusReply> {
        let path = self.path(&path);
        let real = self
            .jobs
            .run(move || utf8(resolve(&path)?.into_os_string()))
            .await?;
        Ok(name(id, real))
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, StatusReply> {
        let path = self.path(&path);
        let stat = self.jobs.run(move || Ok(rustix::fs::stat(&path)?)).await?;
        Ok(Attrs {
            id,
            attrs: attrs(&stat),
        })
    }

    /// As SFTP v3 says: an error if `newpath` exists, checked atomically.
    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, StatusReply> {
        let (from, to) = (self.path(&oldpath), self.path(&newpath));
        self.jobs.run(move || rename_new(&from, &to)).await?;
        Ok(ok(id))
    }

    /// The link's target, which must be UTF-8 to be sent unchanged.
    async fn readlink(&mut self, id: u32, path: String) -> Result<Name, StatusReply> {
        let path = self.path(&path);
        let target = self
            .jobs
            .run(move || utf8(std::fs::read_link(path)?.into_os_string()))
            .await?;
        Ok(name(id, target))
    }

    /// In OpenSSH's order, which every client follows: the target first,
    /// then the link's path (the draft, and russh-sftp's field names, have
    /// them the other way round). The target is stored as given.
    async fn symlink(
        &mut self,
        id: u32,
        target: String,
        link: String,
    ) -> Result<Status, StatusReply> {
        let link = self.path(&link);
        self.jobs
            .run(move || std::os::unix::fs::symlink(target, link))
            .await?;
        Ok(ok(id))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    use std::time::Duration;

    use super::*;

    /// The longest any wait below may take.
    const LIMIT: Duration = Duration::from_secs(10);

    /// A fresh directory to serve, under the system's temporary directory.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            let dir =
                std::env::temp_dir().join(format!("minipaw-sftp-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("dir");
            Scratch(dir)
        }

        fn fs(&self) -> Fs {
            fs_at(&self.0)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn fs_at(home: &Path) -> Fs {
        Fs::new(
            home.to_owned(),
            Jobs::new(Arc::new(Semaphore::new(MAX_JOBS))),
        )
    }

    fn code(reply: Result<impl fmt::Debug, StatusReply>) -> StatusCode {
        reply.expect_err("an error").status_code
    }

    /// A request as a client frames it.
    fn frame(body: &[u8]) -> Vec<u8> {
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    /// SFTP's string: a length, then the bytes.
    fn string(out: &mut Vec<u8>, s: &[u8]) {
        out.extend_from_slice(&(s.len() as u32).to_be_bytes());
        out.extend_from_slice(s);
    }

    /// The replies in `output`, decoded.
    fn replies(output: Vec<u8>) -> Vec<Packet> {
        let mut output = Bytes::from(output);
        let mut out = Vec::new();
        while !output.is_empty() {
            let len = u32::from_be_bytes(output[..4].try_into().unwrap()) as usize;
            let mut reply = output.split_to(4 + len).split_off(4);
            out.push(Packet::try_from(&mut reply).expect("parse"));
        }
        out
    }

    fn status_of(packet: &Packet) -> (u32, StatusCode) {
        let Packet::Status(status) = packet else {
            panic!("not a status: {packet:?}");
        };
        (status.id, status.status_code)
    }

    #[tokio::test]
    async fn an_oversize_length_is_refused_before_reading_on() {
        // Only the length arrives, and the writer stays open: a reader that
        // read on, or allocated for it, would wait (or run out of memory).
        let (mut client, mut server) = tokio::io::duplex(64);
        client
            .write_all(&u32::MAX.to_be_bytes())
            .await
            .expect("write");
        let read = tokio::time::timeout(LIMIT, read_frame(&mut server))
            .await
            .expect("refused without waiting for more");
        assert!(matches!(read, Err(Error::TooLong(u32::MAX))));
        drop(client);
        let len = (MAX_FRAME + 1).to_be_bytes();
        assert!(matches!(
            read_frame(&mut &len[..]).await,
            Err(Error::TooLong(n)) if n == MAX_FRAME + 1
        ));
        // The limit itself is fine.
        let body = vec![7; MAX_FRAME as usize];
        let framed = frame(&body);
        let got = read_frame(&mut &framed[..]).await.expect("read");
        assert_eq!(got.expect("a frame").len(), MAX_FRAME as usize);
    }

    #[tokio::test]
    async fn a_truncated_frame_is_an_error_and_eof_between_frames_is_not() {
        let framed = frame(&[1, 0, 0, 0, 3]);
        assert!(matches!(
            read_frame(&mut &framed[..framed.len() - 1]).await,
            Err(Error::Truncated)
        ));
        assert!(matches!(
            read_frame(&mut &framed[..2]).await,
            Err(Error::Truncated)
        ));
        assert!(matches!(read_frame(&mut &[][..]).await, Ok(None)));
        assert!(matches!(
            read_frame(&mut &[0, 0, 0, 0][..]).await,
            Err(Error::Empty)
        ));
        // The serving loop ends on it, after answering what came whole.
        let mut input = frame(&[1, 0, 0, 0, 3]);
        input.extend_from_slice(&[0, 0, 1]);
        let mut fs = fs_at(Path::new("/"));
        let mut output = Vec::new();
        let served = serve(&mut fs, &input[..], &mut output).await;
        assert!(matches!(served, Err(Error::Truncated)), "{served:?}");
        assert_eq!(output[4], 2, "a version reply");
    }

    #[tokio::test]
    async fn unknown_types_get_op_unsupported() {
        let mut fs = fs_at(Path::new("/"));
        // A reply type from a client, and an unassigned type.
        for kind in [101, 42] {
            let mut input = frame(&[kind, 0, 0, 0, 9]);
            input.extend(frame(&[200, 0, 0, 0, 10, 0, 0, 0, 1, b'x']));
            let mut output = Vec::new();
            serve(&mut fs, &input[..], &mut output)
                .await
                .expect("served");
            let got: Vec<_> = replies(output).iter().map(status_of).collect();
            assert_eq!(
                got,
                [
                    (9, StatusCode::OpUnsupported),
                    (10, StatusCode::OpUnsupported)
                ]
            );
        }
    }

    /// An OPEN cut off after its flags would decode as one with no
    /// attributes; it ends the session instead, and creates nothing.
    #[tokio::test]
    async fn a_truncated_open_ends_the_session() {
        let dir = Scratch::new("truncated");
        let mut fs = dir.fs();
        let mut open = vec![SSH_FXP_OPEN, 0, 0, 0, 1];
        string(&mut open, b"made");
        let pflags = OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE;
        open.extend_from_slice(&pflags.bits().to_be_bytes());
        let mut output = Vec::new();
        let served = serve(&mut fs, &frame(&open)[..], &mut output).await;
        assert!(matches!(served, Err(Error::Malformed(_))), "{served:?}");
        assert!(output.is_empty());
        assert!(!dir.0.join("made").exists());
        // An attributes block whose flags promise a field it lacks.
        open.extend_from_slice(&FileAttr::PERMISSIONS.bits().to_be_bytes());
        let served = serve(&mut fs, &frame(&open)[..], &mut Vec::new()).await;
        assert!(matches!(served, Err(Error::Malformed(_))), "{served:?}");
        assert!(!dir.0.join("made").exists());
        // Whole, it works.
        open.extend_from_slice(&0o600u32.to_be_bytes());
        let mut output = Vec::new();
        serve(&mut fs, &frame(&open)[..], &mut output)
            .await
            .expect("served");
        assert!(matches!(replies(output)[..], [Packet::Handle(_)]));
        assert!(dir.0.join("made").exists());
    }

    #[tokio::test]
    async fn trailing_bytes_end_the_session() {
        let dir = Scratch::new("trailing");
        std::fs::write(dir.0.join("f"), b"").expect("write");
        let mut fs = dir.fs();
        let mut remove = vec![SSH_FXP_REMOVE, 0, 0, 0, 1];
        string(&mut remove, b"f");
        remove.push(0);
        let served = serve(&mut fs, &frame(&remove)[..], &mut Vec::new()).await;
        assert!(matches!(served, Err(Error::Malformed(_))), "{served:?}");
        assert!(dir.0.join("f").exists());
    }

    /// A name that is not UTF-8 is refused, not changed into another name.
    #[tokio::test]
    async fn names_that_are_not_utf8_are_refused() {
        let dir = Scratch::new("utf8");
        let mut fs = dir.fs();
        let mut mkdir = vec![SSH_FXP_MKDIR, 0, 0, 0, 4];
        string(&mut mkdir, b"a\xff");
        mkdir.extend_from_slice(&[0, 0, 0, 0]);
        let mut output = Vec::new();
        serve(&mut fs, &frame(&mkdir)[..], &mut output)
            .await
            .expect("served");
        assert_eq!(status_of(&replies(output)[0]), (4, StatusCode::Failure));
        assert_eq!(std::fs::read_dir(&dir.0).expect("list").count(), 0);
    }

    #[tokio::test]
    async fn handles_are_capped() {
        let dir = Scratch::new("handles");
        std::fs::write(dir.0.join("f"), b"x").expect("write");
        let mut fs = dir.fs();
        let mut handles = Vec::new();
        for i in 0..MAX_HANDLES {
            let handle = if i % 2 == 0 {
                fs.open(1, "f".into(), OpenFlags::READ, FileAttributes::empty())
                    .await
            } else {
                fs.opendir(1, ".".into()).await
            };
            handles.push(handle.expect("open").handle);
        }
        let more = fs
            .open(2, "f".into(), OpenFlags::READ, FileAttributes::empty())
            .await;
        assert_eq!(code(more), StatusCode::Failure);
        assert_eq!(code(fs.opendir(2, ".".into()).await), StatusCode::Failure);
        // Closing one makes room for one.
        fs.close(3, handles.pop().unwrap()).await.expect("close");
        fs.opendir(4, ".".into()).await.expect("room again");
        assert_eq!(code(fs.close(5, "nope".into()).await), StatusCode::Failure);
    }

    #[tokio::test]
    async fn open_flags_map_as_sftp_means_them() {
        let dir = Scratch::new("flags");
        let mut fs = dir.fs();
        // Created though opened only for reading, with the mode asked for.
        let attrs = FileAttributes {
            permissions: Some(0o640),
            ..FileAttributes::empty()
        };
        let read_create = OpenFlags::READ | OpenFlags::CREATE;
        fs.open(1, "new".into(), read_create, attrs)
            .await
            .expect("read-only create");
        let metadata = std::fs::metadata(dir.0.join("new")).expect("created");
        assert_eq!(metadata.mode() & 0o7777 & !0o022, 0o640 & !0o022);
        // Exclusive creation of a file that exists fails, and leaves it.
        std::fs::write(dir.0.join("new"), b"kept").expect("write");
        let exclusive = OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::EXCLUDE;
        let again = fs
            .open(2, "new".into(), exclusive, FileAttributes::empty())
            .await;
        assert_eq!(code(again), StatusCode::Failure);
        assert_eq!(std::fs::read(dir.0.join("new")).expect("read"), b"kept");
        // Without CREATE nothing is made.
        let missing = fs
            .open(3, "none".into(), OpenFlags::WRITE, FileAttributes::empty())
            .await;
        assert_eq!(code(missing), StatusCode::NoSuchFile);
    }

    #[tokio::test]
    async fn reads_are_capped_and_end_with_eof() {
        let dir = Scratch::new("reads");
        let content: Vec<u8> = (0..MAX_READ as usize * 2 + 10).map(|i| i as u8).collect();
        std::fs::write(dir.0.join("big"), &content).expect("write");
        let mut fs = dir.fs();
        let handle = fs
            .open(1, "big".into(), OpenFlags::READ, FileAttributes::empty())
            .await
            .expect("open")
            .handle;
        let data = fs.read(2, handle.clone(), 0, u32::MAX).await.expect("read");
        assert_eq!(data.data.len(), MAX_READ as usize);
        assert_eq!(data.data[..], content[..MAX_READ as usize]);
        let tail = fs
            .read(3, handle.clone(), MAX_READ as u64 * 2, 100)
            .await
            .expect("read");
        assert_eq!(tail.data[..], content[MAX_READ as usize * 2..]);
        let past = fs.read(4, handle.clone(), content.len() as u64, 100).await;
        assert_eq!(code(past), StatusCode::Eof);
        // The reply to the biggest read still fits a frame.
        let reply = Bytes::try_from(Packet::from(data)).expect("encode");
        assert!(reply.len() - 4 <= MAX_FRAME as usize);
    }

    #[tokio::test]
    async fn readdir_pages_at_128() {
        let dir = Scratch::new("readdir");
        for i in 0..300 {
            std::fs::write(dir.0.join(format!("f{i:03}")), b"").expect("write");
        }
        std::fs::create_dir(dir.0.join("sub")).expect("mkdir");
        std::os::unix::fs::symlink("f000", dir.0.join("link")).expect("symlink");
        // Some filesystems (APFS) take no such name; then there is nothing
        // to leave out.
        let odd = std::ffi::OsStr::from_bytes(b"odd\xff");
        let odd_made = std::fs::write(dir.0.join(odd), b"").is_ok();
        let mut fs = dir.fs();
        let handle = fs.opendir(1, ".".into()).await.expect("opendir").handle;
        let mut pages = Vec::new();
        let mut names = Vec::new();
        loop {
            match fs.readdir(2, handle.clone()).await {
                Ok(name) => {
                    pages.push(name.files.len());
                    names.extend(name.files);
                }
                Err(e) => {
                    assert_eq!(e.status_code, StatusCode::Eof);
                    break;
                }
            }
        }
        // `.`, `..` and the odd name are not among them.
        assert_eq!(pages, [128, 128, 46], "odd name made: {odd_made}");
        assert!(names.iter().all(|f| !f.filename.contains('\u{fffd}')));
        let long = |n: &str| {
            let file = names.iter().find(|f| f.filename == n).expect(n);
            file.longname.clone()
        };
        assert!(long("sub").starts_with('d'), "{}", long("sub"));
        assert!(long("link").starts_with('l'), "{}", long("link"));
        assert!(long("f001").starts_with('-'), "{}", long("f001"));
        assert!(long("f001").ends_with(" f001"), "{}", long("f001"));
    }

    #[tokio::test]
    async fn setstat_takes_the_mode_and_times_only() {
        let dir = Scratch::new("setstat");
        let path = dir.0.join("f");
        std::fs::write(&path, b"x").expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let mut fs = dir.fs();
        let mode = |path: &Path| std::fs::metadata(path).expect("stat").mode() & 0o7777;
        for refused in [
            FileAttributes {
                size: Some(0),
                ..FileAttributes::empty()
            },
            FileAttributes {
                uid: Some(0),
                gid: Some(0),
                permissions: Some(0o600),
                ..FileAttributes::empty()
            },
        ] {
            let reply = fs.setstat(1, "f".into(), refused.clone()).await;
            assert_eq!(code(reply), StatusCode::OpUnsupported);
            let handle = fs
                .open(2, "f".into(), OpenFlags::READ, FileAttributes::empty())
                .await
                .expect("open")
                .handle;
            let reply = fs.fsetstat(3, handle, refused).await;
            assert_eq!(code(reply), StatusCode::OpUnsupported);
            // Nothing changed, not even the part that was allowed.
            assert_eq!(std::fs::read(&path).expect("read"), b"x");
            assert_eq!(mode(&path), 0o644);
        }
        let set = FileAttributes {
            permissions: Some(0o100_600),
            atime: Some(1_000_000),
            mtime: Some(2_000_000),
            ..FileAttributes::empty()
        };
        fs.setstat(4, "f".into(), set).await.expect("setstat");
        let metadata = std::fs::metadata(&path).expect("stat");
        assert_eq!(metadata.mode() & 0o7777, 0o600);
        assert_eq!((metadata.atime(), metadata.mtime()), (1_000_000, 2_000_000));
        let handle = fs
            .open(5, "f".into(), OpenFlags::READ, FileAttributes::empty())
            .await
            .expect("open")
            .handle;
        let set = FileAttributes {
            permissions: Some(0o640),
            atime: Some(3),
            mtime: Some(4),
            ..FileAttributes::empty()
        };
        fs.fsetstat(6, handle, set).await.expect("fsetstat");
        let metadata = std::fs::metadata(&path).expect("stat");
        assert_eq!(metadata.mode() & 0o7777, 0o640);
        assert_eq!(metadata.mtime(), 4);

        // Extended attributes, which russh-sftp's types drop, are seen on
        // the wire and refused too.
        let mut body = vec![SSH_FXP_SETSTAT, 0, 0, 0, 7, 0, 0, 0, 1, b'f'];
        let flags = FileAttr::PERMISSIONS.bits() | FileAttr::EXTENDED.bits();
        body.extend_from_slice(&flags.to_be_bytes());
        body.extend_from_slice(&0o777u32.to_be_bytes());
        body.extend_from_slice(&[0, 0, 0, 0]);
        let reply = answer(&mut fs, Bytes::from(body)).await.expect("answer");
        assert_eq!(status_of(&reply), (7, StatusCode::OpUnsupported));
        assert_eq!(mode(&path), 0o640);
    }

    /// A directory handle stays on the directory it opened, through a
    /// rename and a new directory in its place.
    #[tokio::test]
    async fn a_directory_handle_follows_its_directory() {
        let dir = Scratch::new("dirfd");
        std::fs::create_dir(dir.0.join("d")).expect("mkdir");
        std::fs::write(dir.0.join("d/inside"), b"").expect("write");
        let mut fs = dir.fs();
        let handle = fs.opendir(1, "d".into()).await.expect("opendir").handle;
        std::fs::rename(dir.0.join("d"), dir.0.join("moved")).expect("rename");
        std::fs::create_dir(dir.0.join("d")).expect("mkdir");
        let set = FileAttributes {
            permissions: Some(0o700),
            ..FileAttributes::empty()
        };
        fs.fsetstat(2, handle.clone(), set).await.expect("fsetstat");
        let mode = |name: &str| std::fs::metadata(dir.0.join(name)).expect("stat").mode();
        assert_eq!(mode("moved") & 0o7777, 0o700);
        assert_ne!(mode("d") & 0o7777, 0o700);
        let attrs = fs.fstat(3, handle.clone()).await.expect("fstat").attrs;
        assert_eq!(attrs.permissions, Some(mode("moved")));
        let names = fs.readdir(4, handle).await.expect("readdir").files;
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].filename, "inside");
    }

    #[tokio::test]
    async fn rename_never_replaces() {
        let dir = Scratch::new("rename");
        std::fs::write(dir.0.join("a"), b"a").expect("write");
        std::fs::write(dir.0.join("b"), b"b").expect("write");
        let mut fs = dir.fs();
        let replace = fs.rename(1, "a".into(), "b".into()).await;
        assert_eq!(code(replace), StatusCode::Failure);
        assert_eq!(std::fs::read(dir.0.join("b")).expect("read"), b"b");
        fs.rename(2, "a".into(), "c".into()).await.expect("rename");
        assert_eq!(std::fs::read(dir.0.join("c")).expect("read"), b"a");
        assert!(!dir.0.join("a").exists());
    }

    /// realpath follows symlinks as far as the path exists, then works out
    /// the rest by name.
    #[tokio::test]
    async fn realpath_resolves_the_existing_prefix() {
        let dir = Scratch::new("realpath");
        let base = std::fs::canonicalize(&dir.0).expect("canonical");
        std::fs::create_dir_all(base.join("srv/data")).expect("mkdir");
        std::fs::create_dir(base.join("home")).expect("mkdir");
        std::os::unix::fs::symlink(base.join("srv/data"), base.join("home/link")).expect("symlink");
        let mut fs = fs_at(&base.join("home"));
        let real = |name: Name| name.files[0].filename.clone();
        let got = real(
            fs.realpath(1, "link/../new".into())
                .await
                .expect("realpath"),
        );
        assert_eq!(Path::new(&got), base.join("srv/new"));
        let got = real(fs.realpath(2, ".".into()).await.expect("realpath"));
        assert_eq!(Path::new(&got), base.join("home"));
        let got = real(fs.realpath(3, "a/./b/../c".into()).await.expect("realpath"));
        assert_eq!(Path::new(&got), base.join("home/a/c"));
        // Other errors are not passed over.
        std::fs::write(base.join("home/file"), b"").expect("write");
        let not_dir = fs.realpath(4, "file/x".into()).await;
        assert_eq!(code(not_dir), StatusCode::Failure);
    }

    #[tokio::test]
    async fn a_link_target_that_is_not_utf8_is_refused() {
        let dir = Scratch::new("readlink");
        let target = std::ffi::OsStr::from_bytes(b"t\xff");
        if std::os::unix::fs::symlink(target, dir.0.join("l")).is_err() {
            return; // Not on this filesystem.
        }
        let mut fs = dir.fs();
        assert_eq!(code(fs.readlink(1, "l".into()).await), StatusCode::Failure);
    }

    /// A job stuck in the filesystem keeps its permit until it really ends,
    /// without holding up its session's teardown; a job still queued when
    /// the session goes never runs.
    #[test]
    fn jobs_are_bounded_and_end_with_their_session() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let permits = Arc::new(Semaphore::new(MAX_JOBS));
            let jobs = Jobs::new(permits.clone());
            let (started_tx, started) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel::<()>();
            let stuck = jobs.run(move || {
                let _ = started_tx.send(());
                let _ = released.recv_timeout(LIMIT);
                Ok(())
            });
            // The session gives up on it at once.
            let gave_up = tokio::time::timeout(Duration::from_millis(200), stuck).await;
            assert!(gave_up.is_err());
            started.recv_timeout(LIMIT).expect("the job started");
            assert_eq!(permits.available_permits(), MAX_JOBS - 1);
            // Queued behind it, on the only blocking thread.
            let ran = Arc::new(AtomicBool::new(false));
            let flag = ran.clone();
            let queued = jobs.run(move || {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            });
            let gave_up = tokio::time::timeout(Duration::from_millis(200), queued).await;
            assert!(gave_up.is_err());
            drop(jobs);
            release.send(()).expect("release");
            tokio::time::timeout(LIMIT, async {
                while permits.available_permits() < MAX_JOBS {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("every permit back");
            assert!(
                !ran.load(Ordering::SeqCst),
                "a queued job ran after its session"
            );
        });
    }

    #[test]
    fn paths_are_relative_to_home_or_absolute() {
        let fs = fs_at(Path::new("/home/me"));
        assert_eq!(fs.path("a/b"), Path::new("/home/me/a/b"));
        assert_eq!(fs.path(""), Path::new("/home/me"));
        assert_eq!(fs.path("/etc/hosts"), Path::new("/etc/hosts"));
    }

    #[test]
    fn request_layouts() {
        let mut stat = vec![SSH_FXP_STAT, 0, 0, 0, 5];
        string(&mut stat, b"x");
        assert_eq!(
            Shape::of(&stat),
            Some(Shape {
                id: 5,
                attrs: None,
                utf8: true
            })
        );
        assert_eq!(Shape::of(&stat[..stat.len() - 1]), None);
        stat.push(0);
        assert_eq!(Shape::of(&stat), None);
        // A string longer than what is left.
        assert_eq!(
            Shape::of(&[SSH_FXP_STAT, 0, 0, 0, 5, 0, 0, 0, 9, b'x']),
            None
        );
        // Extended pairs in attributes are counted and skipped.
        let mut mkdir = vec![SSH_FXP_MKDIR, 0, 0, 0, 6];
        string(&mut mkdir, b"d");
        mkdir.extend_from_slice(&FileAttr::EXTENDED.bits().to_be_bytes());
        mkdir.extend_from_slice(&1u32.to_be_bytes());
        string(&mut mkdir, b"k");
        string(&mut mkdir, b"v");
        let shape = Shape::of(&mkdir).expect("whole");
        assert_eq!(shape.attrs, Some(FileAttr::EXTENDED.bits()));
        // INIT has no id, then extension pairs to the end.
        let mut init = vec![SSH_FXP_INIT, 0, 0, 0, 3];
        string(&mut init, b"a");
        assert_eq!(Shape::of(&init), None);
        string(&mut init, b"b");
        assert!(Shape::of(&init).is_some());
    }
}
