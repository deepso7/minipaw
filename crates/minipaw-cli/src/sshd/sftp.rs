//! The `sftp` subsystem: an SFTP v3 server on the serving user's files, the
//! channel's program in place of a process.
//!
//! The protocol types and the request methods are russh-sftp's ([`Packet`]
//! and [`Handler`]), but the loop is ours rather than its detached
//! `server::run`: it runs inside the channel's supervisor, which cancels it
//! with the channel, and it ends at the first framing or transport error.
//! It reads the channel's capped input buffer through [`InputReader`], and
//! a request's length is checked before anything is allocated for it.
//!
//! Paths are relative to the serving user's home, and absolute paths reach
//! anywhere that user can: this is not a sandbox, any more than a shell is.
//! Names are UTF-8 strings in russh-sftp's types, so a name that is not
//! UTF-8 arrives with its odd bytes replaced and is not found.

use std::collections::HashMap;
use std::fmt;
use std::fs::{Metadata, Permissions};
use std::future::Future;
use std::io;
use std::os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttr, FileAttributes, Handle, Name, OpenFlags, Packet, Status,
    StatusCode, Version,
};
use russh_sftp::server::{Handler, StatusReply};
use rustix::fs::{AtFlags, Mode, Timespec, Timestamps};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, ReadBuf};

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

/// The request types a client may send (SSH_FXP_INIT, OPEN through
/// SYMLINK, and EXTENDED); anything else gets `OpUnsupported`.
fn is_request(kind: u8) -> bool {
    matches!(kind, 1 | 3..=20 | 200)
}

const SSH_FXP_SETSTAT: u8 = 9;
const SSH_FXP_FSETSTAT: u8 = 10;

/// Why the loop ended early.
#[derive(Debug)]
pub enum Error {
    /// A request claimed more than [`MAX_FRAME`] bytes.
    TooLong(u32),
    /// A request of no bytes at all.
    Empty,
    /// The input ended inside a request.
    Truncated,
    /// A request of a known type did not parse, or its reply did not
    /// encode.
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
    if !is_request(kind) {
        let id = frame
            .get(1..5)
            .map_or(0, |id| u32::from_be_bytes(id.try_into().expect("4 bytes")));
        return Ok(status(id, StatusCode::OpUnsupported.into()));
    }
    // russh-sftp's attributes drop the extended pairs; the raw flags still
    // show them.
    if matches!(kind, SSH_FXP_SETSTAT | SSH_FXP_FSETSTAT)
        && let Some((id, flags)) = setstat_flags(&frame)
        && flags & !(FileAttr::PERMISSIONS | FileAttr::ACMODTIME).bits() != 0
    {
        return Ok(status(id, unsupported_attrs()));
    }
    let request = Packet::try_from(&mut frame).map_err(|e| Error::Malformed(e.to_string()))?;
    Ok(dispatch(fs, request).await)
}

/// A `setstat` or `fsetstat`'s id and raw attribute flags: after the type,
/// the id, then a path or handle string.
fn setstat_flags(frame: &[u8]) -> Option<(u32, u32)> {
    let u32_at = |at: usize| {
        let bytes = frame.get(at..at.checked_add(4)?)?;
        Some(u32::from_be_bytes(bytes.try_into().ok()?))
    };
    let id = u32_at(1)?;
    let name = usize::try_from(u32_at(5)?).ok()?;
    let flags = u32_at(9usize.checked_add(name)?)?;
    Some((id, flags))
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

/// What a handle names.
enum Open {
    File(Arc<std::fs::File>),
    Dir {
        path: PathBuf,
        entries: tokio::fs::ReadDir,
    },
}

/// The SFTP requests, on the serving user's files.
pub struct Fs {
    /// Where relative paths start.
    home: PathBuf,
    handles: HashMap<String, Open>,
    next: u64,
}

impl Fs {
    pub fn new(home: PathBuf) -> Fs {
        Fs {
            home,
            handles: HashMap::new(),
            next: 0,
        }
    }

    /// `path` from the client: relative to home, or absolute.
    fn path(&self, path: &str) -> PathBuf {
        self.home.join(path)
    }

    fn add(&mut self, open: Open) -> Result<String, StatusReply> {
        if self.handles.len() >= MAX_HANDLES {
            return Err(StatusCode::Failure.with_message("too many open handles"));
        }
        self.next += 1;
        let handle = self.next.to_string();
        self.handles.insert(handle.clone(), open);
        Ok(handle)
    }

    fn get(&self, handle: &str) -> Result<&Open, StatusReply> {
        self.handles.get(handle).ok_or_else(bad_handle)
    }

    fn file(&self, handle: &str) -> Result<Arc<std::fs::File>, StatusReply> {
        match self.get(handle)? {
            Open::File(file) => Ok(file.clone()),
            Open::Dir { .. } => Err(bad_handle()),
        }
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

/// Runs a blocking file operation off the runtime's threads.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> Result<T, StatusReply> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StatusCode::Failure.with_message(e.to_string()))?
        .map_err(failed)
}

fn seconds(t: i64) -> u32 {
    t.clamp(0, i64::from(u32::MAX)) as u32
}

/// `metadata` as SFTP attributes: the raw mode, with its file type.
fn attrs(metadata: &Metadata) -> FileAttributes {
    FileAttributes {
        size: Some(metadata.len()),
        uid: Some(metadata.uid()),
        user: None,
        gid: Some(metadata.gid()),
        group: None,
        permissions: Some(metadata.mode()),
        atime: Some(seconds(metadata.atime())),
        mtime: Some(seconds(metadata.mtime())),
    }
}

/// A directory entry, with an `ls -l` style long name.
fn entry(name: String, metadata: &Metadata) -> File {
    let mut file = File::new(name, attrs(metadata));
    let kind = match metadata.mode() & 0o170_000 {
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
fn name(id: u32, path: &Path) -> Name {
    let path = path.to_string_lossy().into_owned();
    Name {
        id,
        files: vec![File {
            filename: path.clone(),
            longname: path,
            attrs: FileAttributes::empty(),
        }],
    }
}

/// `path` with `.` and `..` worked out by name alone, for a path that does
/// not exist (yet).
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::from("/");
    for part in path.components() {
        match part {
            Component::Normal(name) => out.push(name),
            Component::ParentDir => {
                out.pop();
            }
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
        }
    }
    out
}

/// What a `setstat` asks for: the mode and the times, nothing else.
fn settable(attrs: &FileAttributes) -> Result<(Option<u32>, Option<Timestamps>), StatusReply> {
    if attrs.size.is_some() || attrs.uid.is_some() || attrs.gid.is_some() {
        return Err(unsupported_attrs());
    }
    let mode = attrs.permissions.map(|mode| mode & 0o7777);
    let time = |t: u32| Timespec {
        tv_sec: i64::from(t),
        tv_nsec: 0,
    };
    let times = match (attrs.atime, attrs.mtime) {
        (Some(atime), Some(mtime)) => Some(Timestamps {
            last_access: time(atime),
            last_modification: time(mtime),
        }),
        _ => None,
    };
    Ok((mode, times))
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

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        attrs: FileAttributes,
    ) -> Result<Handle, StatusReply> {
        if self.handles.len() >= MAX_HANDLES {
            return Err(StatusCode::Failure.with_message("too many open handles"));
        }
        let mut options = std::fs::OpenOptions::from(pflags);
        // Created with the client's mode, less the umask, as sftp-server
        // does. Non-blocking, so a FIFO cannot hold a thread forever.
        options
            .mode(attrs.permissions.map_or(0o666, |mode| mode & 0o7777))
            .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32);
        let file = tokio::fs::OpenOptions::from(options)
            .open(self.path(&filename))
            .await
            .map_err(failed)?;
        let file = Arc::new(file.into_std().await);
        let handle = self.add(Open::File(file))?;
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
        let file = self.file(&handle)?;
        let len = len.min(MAX_READ) as usize;
        let data = blocking(move || {
            let mut data = vec![0; len];
            let mut got = 0;
            while got < len {
                match file.read_at(&mut data[got..], offset + got as u64) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
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
        let file = self.file(&handle)?;
        blocking(move || file.write_all_at(&data, offset)).await?;
        Ok(ok(id))
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, StatusReply> {
        let metadata = tokio::fs::symlink_metadata(self.path(&path))
            .await
            .map_err(failed)?;
        Ok(Attrs {
            id,
            attrs: attrs(&metadata),
        })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, StatusReply> {
        let metadata = match self.get(&handle)? {
            Open::File(file) => {
                let file = file.clone();
                blocking(move || file.metadata()).await?
            }
            Open::Dir { path, .. } => tokio::fs::metadata(path).await.map_err(failed)?,
        };
        Ok(Attrs {
            id,
            attrs: attrs(&metadata),
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
        if let Some(mode) = mode {
            tokio::fs::set_permissions(&path, Permissions::from_mode(mode))
                .await
                .map_err(failed)?;
        }
        if let Some(times) = times {
            blocking(move || {
                rustix::fs::utimensat(rustix::fs::CWD, &path, &times, AtFlags::empty())
                    .map_err(io::Error::from)
            })
            .await?;
        }
        Ok(ok(id))
    }

    async fn fsetstat(
        &mut self,
        id: u32,
        handle: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusReply> {
        let (mode, times) = settable(&attrs)?;
        match self.get(&handle)? {
            Open::File(file) => {
                let file = file.clone();
                blocking(move || {
                    if let Some(mode) = mode {
                        rustix::fs::fchmod(&*file, Mode::from_raw_mode(mode))?;
                    }
                    if let Some(times) = times {
                        rustix::fs::futimens(&*file, &times)?;
                    }
                    Ok(())
                })
                .await?;
                Ok(ok(id))
            }
            Open::Dir { path, .. } => {
                let path = path.to_string_lossy().into_owned();
                self.setstat(id, path, attrs).await
            }
        }
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, StatusReply> {
        if self.handles.len() >= MAX_HANDLES {
            return Err(StatusCode::Failure.with_message("too many open handles"));
        }
        let path = self.path(&path);
        let entries = tokio::fs::read_dir(&path).await.map_err(failed)?;
        let handle = self.add(Open::Dir { path, entries })?;
        Ok(Handle { id, handle })
    }

    /// The next [`READDIR_PAGE`] names at most, then `Eof`.
    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, StatusReply> {
        let Some(Open::Dir { entries, .. }) = self.handles.get_mut(&handle) else {
            return Err(bad_handle());
        };
        let mut files = Vec::new();
        while files.len() < READDIR_PAGE {
            let Some(entry) = entries.next_entry().await.map_err(failed)? else {
                break;
            };
            // Gone since it was listed: leave it out.
            let Ok(metadata) = entry.metadata().await else {
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            files.push(self::entry(name, &metadata));
        }
        if files.is_empty() {
            return Err(StatusCode::Eof.into());
        }
        Ok(Name { id, files })
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, StatusReply> {
        tokio::fs::remove_file(self.path(&filename))
            .await
            .map_err(failed)?;
        Ok(ok(id))
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusReply> {
        tokio::fs::DirBuilder::new()
            .mode(attrs.permissions.map_or(0o777, |mode| mode & 0o7777))
            .create(self.path(&path))
            .await
            .map_err(failed)?;
        Ok(ok(id))
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, StatusReply> {
        tokio::fs::remove_dir(self.path(&path))
            .await
            .map_err(failed)?;
        Ok(ok(id))
    }

    /// The canonical path, or for a path that does not exist, the path with
    /// `.` and `..` worked out.
    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, StatusReply> {
        let path = self.path(&path);
        let real = match tokio::fs::canonicalize(&path).await {
            Ok(real) => real,
            Err(_) => normalize(&path),
        };
        Ok(name(id, &real))
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, StatusReply> {
        let metadata = tokio::fs::metadata(self.path(&path))
            .await
            .map_err(failed)?;
        Ok(Attrs {
            id,
            attrs: attrs(&metadata),
        })
    }

    /// As SFTP v3 says: an error if `newpath` exists.
    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, StatusReply> {
        let (from, to) = (self.path(&oldpath), self.path(&newpath));
        if tokio::fs::symlink_metadata(&to).await.is_ok() {
            return Err(StatusCode::Failure.with_message("the new name exists"));
        }
        tokio::fs::rename(from, to).await.map_err(failed)?;
        Ok(ok(id))
    }

    async fn readlink(&mut self, id: u32, path: String) -> Result<Name, StatusReply> {
        let target = tokio::fs::read_link(self.path(&path))
            .await
            .map_err(failed)?;
        Ok(name(id, &target))
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
        tokio::fs::symlink(target, self.path(&link))
            .await
            .map_err(failed)?;
        Ok(ok(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Fs::new(self.0.clone())
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
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

    #[tokio::test]
    async fn an_oversize_length_is_refused_before_reading_on() {
        // Only the length arrives: a reader that read on, or allocated for
        // it, would say Truncated (or run out of memory).
        let (mut client, mut server) = tokio::io::duplex(64);
        client
            .write_all(&u32::MAX.to_be_bytes())
            .await
            .expect("write");
        assert!(matches!(
            read_frame(&mut server).await,
            Err(Error::TooLong(u32::MAX))
        ));
        let mut input = &(MAX_FRAME + 1).to_be_bytes()[..];
        assert!(matches!(
            read_frame(&mut input).await,
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
        let mut fs = Fs::new("/".into());
        let mut output = Vec::new();
        let served = serve(&mut fs, &input[..], &mut output).await;
        assert!(matches!(served, Err(Error::Truncated)), "{served:?}");
        assert_eq!(output[4], 2, "a version reply");
    }

    #[tokio::test]
    async fn unknown_types_get_op_unsupported() {
        let mut fs = Fs::new("/".into());
        // A reply type from a client, and an unassigned type.
        for kind in [101, 42] {
            let mut input = frame(&[kind, 0, 0, 0, 9]);
            input.extend(frame(&[200, 0, 0, 0, 10, 0, 0, 0, 1, b'x']));
            let mut output = Vec::new();
            serve(&mut fs, &input[..], &mut output)
                .await
                .expect("served");
            let mut output = Bytes::from(output);
            for want in [9, 10] {
                let len = u32::from_be_bytes(output[..4].try_into().unwrap()) as usize;
                let mut reply = output.split_to(4 + len).split_off(4);
                let Packet::Status(status) = Packet::try_from(&mut reply).expect("parse") else {
                    panic!("not a status");
                };
                assert_eq!(
                    (status.id, status.status_code),
                    (want, StatusCode::OpUnsupported)
                );
            }
        }
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
        assert_eq!(pages, [128, 128, 46]);
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
        std::fs::set_permissions(&path, Permissions::from_mode(0o644)).expect("chmod");
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
        let Packet::Status(status) = reply else {
            panic!("{reply:?}");
        };
        assert_eq!(
            (status.id, status.status_code),
            (7, StatusCode::OpUnsupported)
        );
        assert_eq!(mode(&path), 0o640);
    }

    #[test]
    fn paths_are_relative_to_home_or_absolute() {
        let fs = Fs::new("/home/me".into());
        assert_eq!(fs.path("a/b"), Path::new("/home/me/a/b"));
        assert_eq!(fs.path(""), Path::new("/home/me"));
        assert_eq!(fs.path("/etc/hosts"), Path::new("/etc/hosts"));
        assert_eq!(normalize(Path::new("/a/./b/../c/")), Path::new("/a/c"));
        assert_eq!(normalize(Path::new("/../..")), Path::new("/"));
    }
}
