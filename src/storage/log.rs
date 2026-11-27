//! A segmented, append-only message log (P15).
//!
//! ```text
//!   data/
//!     00000000000000000000.log   offsets 0 ..
//!     00000000000000041233.log   offsets 41233 ..   (file name = base offset)
//!     00000000000000083019.log   ← active segment, appended to
//! ```
//!
//! Record layout (all integers big-endian):
//!
//! ```text
//!  +---------+---------+------------+---------------+----------+-------+---------+
//!  | len u32 | crc u32 | offset u64 | timestamp u64 | tlen u16 | topic | payload |
//!  +---------+---------+------------+---------------+----------+-------+---------+
//!            |<------------- crc32 covers everything after the crc ------------->|
//!  len = bytes after the len field
//! ```
//!
//! The checksum lets recovery tell a complete record from one that was only
//! half written when the process died (a "torn" write). Recovery keeps the
//! longest valid prefix of the active segment and truncates the rest.
//!
//! This type is synchronous on purpose. It runs on a dedicated thread
//! ([`super::persist`]), never on the async runtime.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::protocol::topic::matches;

/// crc + offset + timestamp + topic length.
const BODY_HEADER: usize = 4 + 8 + 8 + 2;
/// Upper bound on one record; anything larger is treated as corruption.
const MAX_RECORD: usize = 16 * 1024 * 1024;
const WRITE_BUFFER: usize = 64 * 1024;

/// One stored message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub topic: String,
    pub payload: Bytes,
}

/// A view of a record inside a segment buffer, without copying.
#[derive(Debug, Clone, Copy)]
pub struct RecordRef<'a> {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub topic: &'a str,
    pub payload: &'a [u8],
}

impl RecordRef<'_> {
    fn to_owned(self) -> Record {
        Record {
            offset: self.offset,
            timestamp_ms: self.timestamp_ms,
            topic: self.topic.to_owned(),
            payload: Bytes::copy_from_slice(self.payload),
        }
    }
}

/// Size limits for a log.
#[derive(Debug, Clone, Copy)]
pub struct LogLimits {
    /// Start a new segment once the active one reaches this size.
    pub segment_bytes: u64,
    /// Delete the oldest segments while the total exceeds this.
    pub retention_bytes: u64,
}

impl Default for LogLimits {
    fn default() -> Self {
        Self {
            segment_bytes: 8 * 1024 * 1024,
            retention_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Point-in-time description of a log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LogInfo {
    pub segments: u64,
    pub retained_bytes: u64,
    /// Offset of the oldest retained record (== `next_offset` if empty).
    pub first_offset: u64,
    /// Offset the next appended record will get.
    pub next_offset: u64,
    /// Bytes cut off the active segment by crash recovery when opened.
    pub recovered_bytes: u64,
}

#[derive(Debug)]
struct Segment {
    base: u64,
    path: PathBuf,
    len: u64,
}

/// The writable log. Not `Sync`: exactly one thread owns it.
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    limits: LogLimits,
    segments: VecDeque<Segment>,
    writer: BufWriter<File>,
    next_offset: u64,
    recovered_bytes: u64,
    scratch: Vec<u8>,
}

fn segment_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.log"))
}

/// Segments in `dir`, oldest first.
fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let base = path
            .extension()
            .filter(|e| *e == "log")
            .and_then(|_| path.file_stem())
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(base) = base {
            out.push((base, path));
        }
    }
    out.sort_unstable_by_key(|(b, _)| *b);
    Ok(out)
}

/// Make a newly created (or deleted) file's directory entry durable. Without
/// this, a crash right after creating a segment can lose the file itself
/// even though its contents were fsynced.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Walk the valid records at the start of `data`. Stops at the first record
/// that is incomplete, oversized, fails its checksum or has a bad topic.
/// Returns the number of bytes that form valid records.
pub fn parse_records(data: &[u8], mut f: impl FnMut(RecordRef<'_>)) -> usize {
    let mut pos = 0;
    while data.len() - pos >= 4 {
        let len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        if !(BODY_HEADER..=MAX_RECORD).contains(&len) || data.len() - pos - 4 < len {
            break;
        }
        let rec = &data[pos + 4..pos + 4 + len];
        let crc = u32::from_be_bytes(rec[0..4].try_into().unwrap());
        let body = &rec[4..];
        if crc32fast::hash(body) != crc {
            break;
        }
        let offset = u64::from_be_bytes(body[0..8].try_into().unwrap());
        let timestamp_ms = u64::from_be_bytes(body[8..16].try_into().unwrap());
        let tlen = u16::from_be_bytes(body[16..18].try_into().unwrap()) as usize;
        if body.len() - 18 < tlen {
            break;
        }
        let Ok(topic) = std::str::from_utf8(&body[18..18 + tlen]) else {
            break;
        };
        f(RecordRef {
            offset,
            timestamp_ms,
            topic,
            payload: &body[18 + tlen..],
        });
        pos += 4 + len;
    }
    pos
}

fn encode_record(buf: &mut Vec<u8>, offset: u64, ts: u64, topic: &[u8], payload: &[u8]) {
    buf.clear();
    let len = BODY_HEADER + topic.len() + payload.len();
    buf.reserve(4 + len);
    buf.extend_from_slice(&(len as u32).to_be_bytes());
    buf.extend_from_slice(&[0; 4]); // crc placeholder
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&ts.to_be_bytes());
    buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic);
    buf.extend_from_slice(payload);
    let crc = crc32fast::hash(&buf[8..]);
    buf[4..8].copy_from_slice(&crc.to_be_bytes());
}

impl Log {
    /// Open (or create) the log in `dir`, recovering the active segment.
    pub fn open(dir: impl AsRef<Path>, limits: LogLimits) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut segments: VecDeque<Segment> = VecDeque::new();
        for (base, path) in list_segments(&dir)? {
            let len = fs::metadata(&path)?.len();
            segments.push_back(Segment { base, path, len });
        }
        if segments.is_empty() {
            let path = segment_path(&dir, 0);
            File::create(&path)?;
            sync_dir(&dir)?;
            segments.push_back(Segment {
                base: 0,
                path,
                len: 0,
            });
        }

        // Recover the active (newest) segment: keep the valid prefix only.
        let active = segments.back_mut().expect("at least one segment");
        let data = fs::read(&active.path)?;
        let mut next_offset = active.base;
        let valid = parse_records(&data, |r| next_offset = r.offset + 1);
        let recovered_bytes = (data.len() - valid) as u64;
        let file = OpenOptions::new().append(true).open(&active.path)?;
        if recovered_bytes > 0 {
            file.set_len(valid as u64)?;
            file.sync_all()?;
            active.len = valid as u64;
        }

        Ok(Self {
            dir,
            limits,
            segments,
            writer: BufWriter::with_capacity(WRITE_BUFFER, file),
            next_offset,
            recovered_bytes,
            scratch: Vec::new(),
        })
    }

    /// Append one record to the write buffer. Not durable until
    /// [`commit`](Self::commit) with `sync = true`.
    pub fn append(&mut self, timestamp_ms: u64, topic: &str, payload: &[u8]) -> io::Result<u64> {
        let active_len = self.segments.back().map_or(0, |s| s.len);
        if active_len >= self.limits.segment_bytes {
            self.roll()?;
        }
        let offset = self.next_offset;
        encode_record(
            &mut self.scratch,
            offset,
            timestamp_ms,
            topic.as_bytes(),
            payload,
        );
        self.writer.write_all(&self.scratch)?;
        if let Some(seg) = self.segments.back_mut() {
            seg.len += self.scratch.len() as u64;
        }
        self.next_offset += 1;
        Ok(offset)
    }

    /// Push buffered records to the kernel and, if `sync`, to the device.
    pub fn commit(&mut self, sync: bool) -> io::Result<()> {
        self.writer.flush()?;
        if sync {
            // On macOS, Rust's sync_data uses F_FULLFSYNC: a real flush of
            // the drive's cache, not just a hand-off to it.
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Seal the active segment and start a new one at `next_offset`.
    fn roll(&mut self) -> io::Result<()> {
        self.commit(true)?;
        let path = segment_path(&self.dir, self.next_offset);
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        sync_dir(&self.dir)?;
        self.writer = BufWriter::with_capacity(WRITE_BUFFER, file);
        self.segments.push_back(Segment {
            base: self.next_offset,
            path,
            len: 0,
        });
        self.enforce_retention()
    }

    fn enforce_retention(&mut self) -> io::Result<()> {
        let mut total: u64 = self.segments.iter().map(|s| s.len).sum();
        let mut removed = false;
        while total > self.limits.retention_bytes && self.segments.len() > 1 {
            let old = self.segments.pop_front().expect("len > 1");
            total -= old.len;
            match fs::remove_file(&old.path) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        if removed {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    pub fn info(&self) -> LogInfo {
        LogInfo {
            segments: self.segments.len() as u64,
            retained_bytes: self.segments.iter().map(|s| s.len).sum(),
            first_offset: self.segments.front().map_or(self.next_offset, |s| s.base),
            next_offset: self.next_offset,
            recovered_bytes: self.recovered_bytes,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The newest `count` records whose topic matches `filter`, oldest first.
///
/// Reads segments newest → oldest and stops as soon as it has enough, so
/// asking for recent history costs one segment read, not the whole log. Safe
/// to call while the writer thread appends: a record still being written
/// fails the length/CRC check and is simply not returned yet.
pub fn read_tail(dir: &Path, filter: &str, count: usize) -> io::Result<Vec<Record>> {
    let mut out: VecDeque<Record> = VecDeque::new();
    if count == 0 {
        return Ok(Vec::new());
    }
    for (_, path) in list_segments(dir)?.into_iter().rev() {
        let data = match fs::read(&path) {
            Ok(d) => d,
            // Deleted by retention between listing and reading.
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let mut found: Vec<Record> = Vec::new();
        parse_records(&data, |r| {
            if matches(filter, r.topic) {
                found.push(r.to_owned());
            }
        });
        for r in found.into_iter().rev() {
            if out.len() == count {
                break;
            }
            out.push_front(r);
        }
        if out.len() == count {
            break;
        }
    }
    Ok(out.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> LogLimits {
        LogLimits {
            segment_bytes: 512,
            retention_bytes: 4096,
        }
    }

    #[test]
    fn append_commit_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogLimits::default()).unwrap();
        for i in 0..10u64 {
            assert_eq!(log.append(1000 + i, "a/b", &[i as u8; 3]).unwrap(), i);
        }
        log.commit(true).unwrap();
        let recs = read_tail(dir.path(), "a/#", 100).unwrap();
        assert_eq!(recs.len(), 10);
        assert_eq!(recs[3].offset, 3);
        assert_eq!(recs[3].timestamp_ms, 1003);
        assert_eq!(&recs[3].payload[..], &[3, 3, 3]);
    }

    #[test]
    fn read_tail_returns_newest_matching_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), small()).unwrap();
        for i in 0..200u64 {
            let topic = if i % 2 == 0 { "even" } else { "odd" };
            log.append(i, topic, &i.to_be_bytes()).unwrap();
        }
        log.commit(false).unwrap();
        let recs = read_tail(dir.path(), "odd", 5).unwrap();
        let offsets: Vec<u64> = recs.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![191, 193, 195, 197, 199]);
    }

    #[test]
    fn segments_roll_and_retention_deletes_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), small()).unwrap();
        for i in 0..500u64 {
            log.append(i, "t", &[0u8; 40]).unwrap();
        }
        log.commit(false).unwrap();
        let info = log.info();
        assert!(info.segments > 1, "should have rolled: {info:?}");
        assert!(info.retained_bytes <= small().retention_bytes + small().segment_bytes + 128);
        assert!(
            info.first_offset > 0,
            "oldest segments should be gone: {info:?}"
        );
        assert_eq!(info.next_offset, 500);
        // The oldest records are gone; the newest are still readable.
        let recs = read_tail(dir.path(), "#", 1000).unwrap();
        assert_eq!(recs.first().unwrap().offset, info.first_offset);
        assert_eq!(recs.last().unwrap().offset, 499);
        // Offsets are contiguous across segment boundaries.
        assert!(recs.windows(2).all(|w| w[1].offset == w[0].offset + 1));
    }

    #[test]
    fn reopen_continues_offsets() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), small()).unwrap();
            for i in 0..50u64 {
                log.append(i, "t", b"x").unwrap();
            }
            log.commit(true).unwrap();
        }
        let mut log = Log::open(dir.path(), small()).unwrap();
        assert_eq!(log.info().next_offset, 50);
        assert_eq!(log.append(0, "t", b"y").unwrap(), 50);
    }

    #[test]
    fn recovery_truncates_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogLimits::default()).unwrap();
            for i in 0..5u64 {
                log.append(i, "t", b"payload").unwrap();
            }
            log.commit(true).unwrap();
        }
        // Simulate a crash mid-write: half a record at the end of the file.
        let seg = segment_path(dir.path(), 0);
        let good_len = fs::metadata(&seg).unwrap().len();
        let mut torn = Vec::new();
        encode_record(&mut torn, 5, 5, b"t", b"this one never finished");
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&torn[..torn.len() / 2]).unwrap();
        drop(f);

        let mut log = Log::open(dir.path(), LogLimits::default()).unwrap();
        let info = log.info();
        assert_eq!(info.next_offset, 5, "torn record must not count");
        assert_eq!(info.recovered_bytes, (torn.len() / 2) as u64);
        assert_eq!(fs::metadata(&seg).unwrap().len(), good_len);
        // The log keeps working, and offset 5 is reused by the next append.
        assert_eq!(log.append(9, "t", b"after").unwrap(), 5);
        log.commit(true).unwrap();
        assert_eq!(read_tail(dir.path(), "t", 10).unwrap().len(), 6);
    }

    #[test]
    fn recovery_stops_at_corrupted_record() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogLimits::default()).unwrap();
            for i in 0..10u64 {
                log.append(i, "t", b"0123456789").unwrap();
            }
            log.commit(true).unwrap();
        }
        // Flip one byte inside the payload of record #7.
        let seg = segment_path(dir.path(), 0);
        let mut data = fs::read(&seg).unwrap();
        let rec_len = data.len() / 10;
        data[7 * rec_len + rec_len - 3] ^= 0xff;
        fs::write(&seg, &data).unwrap();

        let log = Log::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(
            log.info().next_offset,
            7,
            "records from the bad one on are dropped"
        );
    }

    #[test]
    fn parse_never_panics_on_garbage() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        for len in 0..300 {
            let data: Vec<u8> = (0..len)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed as u8
                })
                .collect();
            parse_records(&data, |_| {});
        }
    }
}
