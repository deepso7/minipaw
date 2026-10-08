//! The chat's session I/O: channels standing in for stdin and stdout, the
//! input line editor, and turning the peer's bytes into lines that are safe
//! to put on a terminal.

use std::io::{self, Read, Write};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};

/// How many chunks of the peer's data may wait for the UI before the
/// session's writer blocks, so a slow UI slows the peer down instead of
/// queueing without bound (the session acks what was written).
pub const OUTPUT_CAPACITY: usize = 64;

/// The session's input: bytes the UI sends, read as a stream. Ends (reads
/// `Ok(0)`) once every sender is dropped and the queue is drained.
#[derive(Debug)]
pub struct ChannelReader {
    rx: Receiver<Vec<u8>>,
    /// The rest of the chunk a short read did not take.
    pending: Vec<u8>,
    at: usize,
}

impl ChannelReader {
    /// A reader over `rx`.
    pub fn new(rx: Receiver<Vec<u8>>) -> Self {
        ChannelReader {
            rx,
            pending: Vec::new(),
            at: 0,
        }
    }

    /// A reader and the sender that feeds it.
    pub fn channel() -> (Sender<Vec<u8>>, Self) {
        let (tx, rx) = mpsc::channel();
        (tx, ChannelReader::new(rx))
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.at == self.pending.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.pending = chunk;
                    self.at = 0;
                }
                // Every sender is gone: end of input.
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.pending.len() - self.at);
        buf[..n].copy_from_slice(&self.pending[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// The session's output: each write is sent to the UI as one chunk over a
/// bounded channel, blocking while it is full.
#[derive(Debug)]
pub struct ChannelWriter {
    tx: SyncSender<Vec<u8>>,
}

impl ChannelWriter {
    /// A writer over `tx`.
    pub fn new(tx: SyncSender<Vec<u8>>) -> Self {
        ChannelWriter { tx }
    }

    /// A writer and the receiver it feeds, holding at most `capacity`
    /// chunks.
    pub fn channel(capacity: usize) -> (Self, Receiver<Vec<u8>>) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (ChannelWriter::new(tx), rx)
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.tx
            .send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the chat window closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A single-line editor: the text and a cursor, counted in `char`s.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LineEdit {
    chars: Vec<char>,
    cursor: usize,
}

impl LineEdit {
    /// An empty line.
    pub fn new() -> Self {
        LineEdit::default()
    }

    /// The text.
    pub fn text(&self) -> String {
        self.chars.iter().collect()
    }

    /// The text as `char`s.
    pub fn chars(&self) -> &[char] {
        &self.chars
    }

    /// Where the cursor is, in `char`s from the start.
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Whether the line is empty.
    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// Inserts `c` before the cursor.
    pub fn insert(&mut self, c: char) {
        self.chars.insert(self.cursor, c);
        self.cursor += 1;
    }

    /// Moves the cursor one `char` left.
    pub fn left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    /// Moves the cursor one `char` right.
    pub fn right(&mut self) {
        self.cursor = (self.cursor + 1).min(self.chars.len());
    }

    /// Moves the cursor to the start.
    pub fn home(&mut self) {
        self.cursor = 0;
    }

    /// Moves the cursor to the end.
    pub fn end(&mut self) {
        self.cursor = self.chars.len();
    }

    /// Deletes the `char` before the cursor.
    pub fn backspace(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            self.chars.remove(self.cursor);
        }
    }

    /// Deletes the `char` under the cursor.
    pub fn delete(&mut self) {
        if self.cursor < self.chars.len() {
            self.chars.remove(self.cursor);
        }
    }

    /// Empties the line.
    pub fn clear(&mut self) {
        self.chars.clear();
        self.cursor = 0;
    }

    /// Empties the line, returning what it held.
    pub fn take(&mut self) -> String {
        let text = self.text();
        self.clear();
        text
    }
}

/// The longest line [`LineSplitter`] hands out, in bytes once sanitised;
/// longer ones are split. It also caps the partial line it holds, so a peer
/// that never sends `\n` cannot grow it without bound.
pub const MAX_LINE: usize = 16 * 1024;

/// How many spaces a tab becomes.
const TAB: &str = "    ";

/// Splits the peer's bytes into sanitised lines, holding a partial line
/// until the rest arrives.
#[derive(Debug, Default)]
pub struct LineSplitter {
    partial: Vec<u8>,
}

impl LineSplitter {
    /// An empty splitter.
    pub fn new() -> Self {
        LineSplitter::default()
    }

    /// Adds `bytes`, returning the lines they complete.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut lines = Vec::new();
        let mut rest = bytes;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            self.partial.extend_from_slice(&rest[..nl]);
            rest = &rest[nl + 1..];
            if self.partial.last() == Some(&b'\r') {
                // A CRLF line ending, not a carriage return to draw.
                self.partial.pop();
            }
            push_capped(&mut lines, sanitize(&self.partial));
            self.partial.clear();
        }
        self.partial.extend_from_slice(rest);
        while self.partial.len() > MAX_LINE {
            // Cut before a UTF-8 continuation byte, so no character splits.
            let mut cut = MAX_LINE;
            while cut > 0 && self.partial[cut] & 0xC0 == 0x80 {
                cut -= 1;
            }
            if cut == 0 {
                cut = MAX_LINE;
            }
            push_capped(&mut lines, sanitize(&self.partial[..cut]));
            self.partial.drain(..cut);
        }
        lines
    }

    /// The partial line left at the end of the stream, if any, split as
    /// [`push`](Self::push) splits lines.
    pub fn finish(&mut self) -> Vec<String> {
        let mut lines = Vec::new();
        if !self.partial.is_empty() {
            push_capped(&mut lines, sanitize(&self.partial));
            self.partial.clear();
        }
        lines
    }
}

/// Adds `line` to `lines` in pieces of at most [`MAX_LINE`] bytes, cut
/// between characters. Sanitising can grow text (a tab becomes four
/// spaces), so the cap applies after it.
fn push_capped(lines: &mut Vec<String>, mut line: String) {
    while line.len() > MAX_LINE {
        let mut cut = MAX_LINE;
        while !line.is_char_boundary(cut) {
            cut -= 1;
        }
        let rest = line.split_off(cut);
        lines.push(std::mem::replace(&mut line, rest));
    }
    lines.push(line);
}

/// `bytes` as text that is safe to draw: invalid UTF-8 and every control
/// character (C0 including ESC and CR, DEL, C1) and bidirectional override
/// become `�`, and tabs become spaces. A peer cannot move the cursor, clear
/// the screen or reorder text.
pub fn sanitize(bytes: &[u8]) -> String {
    sanitize_str(&String::from_utf8_lossy(bytes))
}

/// [`sanitize`] for text.
pub fn sanitize_str(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\t' => out.push_str(TAB),
            c if c.is_control() || is_bidi_control(c) => out.push('\u{FFFD}'),
            c => out.push(c),
        }
    }
    out
}

/// The invisible characters that reorder the text around them: embeddings,
/// overrides, isolates and the directional marks.
fn is_bidi_control(c: char) -> bool {
    matches!(
        c,
        '\u{061C}' | '\u{200E}' | '\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'
    )
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn reader_keeps_leftovers_across_reads_and_ends_when_senders_drop() {
        let (tx, mut reader) = ChannelReader::channel();
        tx.send(b"hello".to_vec()).unwrap();
        tx.send(Vec::new()).unwrap();
        tx.send(b" world".to_vec()).unwrap();
        drop(tx);
        let mut buf = [0u8; 3];
        let mut got = Vec::new();
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            assert!(n <= 3);
            got.extend_from_slice(&buf[..n]);
        }
        assert_eq!(got, b"hello world");
        // EOF stays EOF.
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn reader_blocks_until_data_arrives() {
        let (tx, mut reader) = ChannelReader::channel();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            tx.send(b"late".to_vec()).unwrap();
        });
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "late");
        sender.join().unwrap();
    }

    #[test]
    fn writer_sends_copies_and_fails_once_the_receiver_is_gone() {
        let (mut writer, rx) = ChannelWriter::channel(4);
        assert_eq!(writer.write(b"abc").unwrap(), 3);
        assert_eq!(writer.write(b"").unwrap(), 0);
        writer.flush().unwrap();
        assert_eq!(rx.recv().unwrap(), b"abc");
        assert!(rx.try_recv().is_err(), "empty writes send nothing");
        drop(rx);
        let err = writer.write(b"x").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn writer_blocks_while_the_channel_is_full() {
        let (mut writer, rx) = ChannelWriter::channel(2);
        let (done_tx, done) = mpsc::channel();
        let t = thread::spawn(move || {
            for i in 0..3u8 {
                writer.write_all(&[i]).unwrap();
            }
            done_tx.send(()).unwrap();
        });
        // Two chunks fit; the third waits for the UI.
        assert!(done.recv_timeout(Duration::from_millis(150)).is_err());
        assert_eq!(rx.recv().unwrap(), [0]);
        done.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(rx.recv().unwrap(), [1]);
        assert_eq!(rx.recv().unwrap(), [2]);
        t.join().unwrap();
    }

    #[test]
    fn line_edit_operations() {
        let mut e = LineEdit::new();
        assert!(e.is_empty());
        for c in "héllo".chars() {
            e.insert(c);
        }
        assert_eq!((e.text().as_str(), e.cursor()), ("héllo", 5));
        e.left();
        e.left();
        e.insert('🐾');
        assert_eq!((e.text().as_str(), e.cursor()), ("hél🐾lo", 4));
        e.backspace();
        assert_eq!((e.text().as_str(), e.cursor()), ("héllo", 3));
        e.home();
        e.left();
        assert_eq!(e.cursor(), 0);
        e.backspace();
        assert_eq!(e.text(), "héllo");
        e.delete();
        assert_eq!((e.text().as_str(), e.cursor()), ("éllo", 0));
        e.right();
        e.delete();
        assert_eq!((e.text().as_str(), e.cursor()), ("élo", 1));
        e.end();
        e.right();
        e.delete();
        assert_eq!((e.text().as_str(), e.cursor()), ("élo", 3));
        e.insert('!');
        assert_eq!(e.chars(), ['é', 'l', 'o', '!']);
        assert_eq!(e.take(), "élo!");
        assert!(e.is_empty());
        assert_eq!(e.cursor(), 0);
        e.insert('x');
        e.clear();
        assert_eq!(e, LineEdit::new());
    }

    #[test]
    fn splitter_joins_lines_across_chunks() {
        let mut s = LineSplitter::new();
        assert!(s.push(b"hel").is_empty());
        assert_eq!(s.push(b"lo\nwor"), ["hello"]);
        assert_eq!(s.push(b"ld\n\nthree\r\nfour"), ["world", "", "three"]);
        assert_eq!(s.finish(), ["four"]);
        assert!(s.finish().is_empty());
    }

    #[test]
    fn splitter_keeps_multibyte_characters_split_across_chunks() {
        let mut s = LineSplitter::new();
        let paw = "🐾".as_bytes();
        assert!(s.push(&paw[..2]).is_empty());
        let mut rest = paw[2..].to_vec();
        rest.push(b'\n');
        assert_eq!(s.push(&rest), ["🐾"]);
    }

    #[test]
    fn splitter_caps_a_line_that_never_ends() {
        let mut s = LineSplitter::new();
        // An odd start puts MAX_LINE inside a two-byte character.
        let long = format!("a{}", "é".repeat(MAX_LINE));
        let mut lines = s.push(long.as_bytes());
        assert_eq!(lines.len(), 2);
        assert!(lines.iter().all(|l| l.len() <= MAX_LINE));
        lines.extend(s.finish());
        assert!(
            lines.iter().all(|l| !l.contains('\u{FFFD}')),
            "no split chars"
        );
        assert_eq!(lines.concat(), long);
    }

    #[test]
    fn splitter_caps_complete_lines_after_expanding_tabs() {
        let mut s = LineSplitter::new();
        // 100 KiB of tabs, each four spaces once sanitised.
        let mut tabs = vec![b'\t'; 100 * 1024];
        tabs.push(b'\n');
        let lines = s.push(&tabs);
        assert!(lines.iter().all(|l| l.len() <= MAX_LINE));
        assert_eq!(lines.iter().map(String::len).sum::<usize>(), 400 * 1024);
        assert_eq!(lines.len(), 400 * 1024 / MAX_LINE);
        // Cut between characters, not inside one.
        let wide = format!("a{}\n", "é".repeat(MAX_LINE));
        let lines = s.push(wide.as_bytes());
        assert!(lines.iter().all(|l| l.len() <= MAX_LINE));
        assert_eq!(lines.concat(), wide.trim_end());
        // So is what is left at the end of the stream.
        assert!(s.push(&tabs[..MAX_LINE]).is_empty());
        let rest = s.finish();
        assert_eq!(rest.len(), 4);
        assert!(rest.iter().all(|l| l.len() <= MAX_LINE));
    }

    #[test]
    fn control_characters_are_neutralised() {
        let mut s = LineSplitter::new();
        assert_eq!(s.push(b"\x1b[2Jboom\n"), ["\u{FFFD}[2Jboom"]);
        assert_eq!(
            s.push(b"a\rb\x07c\x7fd\x00e\tf\n"),
            ["a\u{FFFD}b\u{FFFD}c\u{FFFD}d\u{FFFD}e    f"]
        );
        // C1 controls (here CSI, U+009B) and invalid UTF-8.
        let mut c1 = "x\u{9b}2Jy".as_bytes().to_vec();
        c1.extend_from_slice(b"\xff\n");
        assert_eq!(s.push(&c1), ["x\u{FFFD}2Jy\u{FFFD}"]);
        assert_eq!(sanitize_str("ab\u{202E}cd"), "ab\u{FFFD}cd");
        assert_eq!(
            sanitize_str("a\u{061C}b\u{200E}c\u{200F}d"),
            "a\u{FFFD}b\u{FFFD}c\u{FFFD}d"
        );
        assert_eq!(sanitize(b"plain text"), "plain text");
    }
}
