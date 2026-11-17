//! `OP_MSG` framing and a BSON writer for command documents.
//!
//! A request is one message: the 16-byte header, the flag bits and a single
//! kind-0 section holding the command document, which [`DocWriter`] writes
//! element by element straight into the connection's reused buffer (the
//! filter and pipeline documents the compiler built are serialized in
//! place, without an intermediate command document).

use ::mongodb::bson::Document;
use ::mongodb::bson::raw::RawDocument;
use std::io::{Error, ErrorKind};

pub(super) type Res<T> = Result<T, Error>;

/// `OP_MSG` opcode.
const OP_MSG: i32 = 2013;
/// Message header: length, request id, response-to, opcode.
pub(super) const HEADER_LEN: usize = 16;
/// Header plus flag bits plus the section kind byte.
const PREFIX_LEN: usize = HEADER_LEN + 5;
/// Flag bit: a CRC-32C checksum follows the sections.
const CHECKSUM_PRESENT: u32 = 1;
/// Flag bit: another message follows without a request.
const MORE_TO_COME: u32 = 1 << 1;
/// Bits 0-15 are required: an unknown one must fail the message.
const REQUIRED_BITS: u32 = 0xffff;

const TYPE_STRING: u8 = 0x02;
const TYPE_DOCUMENT: u8 = 0x03;
const TYPE_ARRAY: u8 = 0x04;
const TYPE_BOOL: u8 = 0x08;
const TYPE_INT32: u8 = 0x10;
const TYPE_INT64: u8 = 0x12;

pub(super) fn protocol(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidData, message.into())
}

fn le_i32(bytes: &[u8], at: usize) -> Res<i32> {
    bytes
        .get(at..at + 4)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(i32::from_le_bytes)
        .ok_or_else(|| protocol("truncated MongoDB reply"))
}

/// Length announced by a message header (`bytes` holds at least 4 bytes).
pub(super) fn message_len(bytes: &[u8]) -> Res<usize> {
    let len = le_i32(bytes, 0)?;
    usize::try_from(len)
        .ok()
        .filter(|n| *n >= PREFIX_LEN)
        .ok_or_else(|| protocol(format!("invalid MongoDB length {len}")))
}

/// Clear `out` and write the request prefix; the command document follows
/// through [`DocWriter::begin`].
pub(super) fn begin_request(out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(&[0; PREFIX_LEN]);
}

/// Fill in the header of the request in `out`.
pub(super) fn finish_request(out: &mut [u8], request_id: i32) -> Res<()> {
    let too_large = |_| protocol("MongoDB request too large");
    let len = i32::try_from(out.len()).map_err(too_large)?;
    out[0..4].copy_from_slice(&len.to_le_bytes());
    out[4..8].copy_from_slice(&request_id.to_le_bytes());
    out[12..16].copy_from_slice(&OP_MSG.to_le_bytes());
    Ok(())
}

/// The command document of the request in `out`.
pub(super) fn request_body(out: &[u8]) -> &[u8] {
    out.get(PREFIX_LEN..).unwrap_or_default()
}

/// The body document of the reply `msg` (the whole message, header
/// included) to request `request_id`.
pub(super) fn reply_body(msg: &[u8], request_id: i32) -> Res<&RawDocument> {
    if le_i32(msg, 8)? != request_id {
        return Err(protocol("MongoDB reply to another request"));
    }
    let opcode = le_i32(msg, 12)?;
    if opcode != OP_MSG {
        return Err(protocol(format!("unexpected MongoDB opcode {opcode}")));
    }
    let flags = le_i32(msg, HEADER_LEN)?.cast_unsigned();
    let unknown = flags & REQUIRED_BITS & !CHECKSUM_PRESENT;
    if flags & MORE_TO_COME != 0 || unknown != 0 {
        return Err(protocol(format!("unsupported OP_MSG flags {flags:#x}")));
    }
    let end = if flags & CHECKSUM_PRESENT == 0 {
        msg.len()
    } else {
        msg.len().saturating_sub(4)
    };
    let mut at = HEADER_LEN + 4;
    while at < end {
        let kind = msg[at];
        let len = usize::try_from(le_i32(msg, at + 1)?)
            .map_err(|_| protocol("invalid OP_MSG section length"))?;
        if kind == 0 {
            let doc = msg
                .get(at + 1..at + 1 + len)
                .ok_or_else(|| protocol("truncated OP_MSG body"))?;
            let doc = RawDocument::from_bytes(doc);
            return doc.map_err(|e| protocol(e.to_string()));
        }
        // A kind-1 document sequence: its size counts itself, not the kind.
        at += 1 + len;
    }
    Err(protocol("OP_MSG reply without a body"))
}

/// Writes one BSON document (or array) into a buffer, element by element.
pub(super) struct DocWriter<'a> {
    buf: &'a mut Vec<u8>,
    start: usize,
    /// Next array index, for arrays.
    index: Option<usize>,
}

impl<'a> DocWriter<'a> {
    /// Start a document at the end of `buf`.
    pub(super) fn begin(buf: &'a mut Vec<u8>) -> Self {
        let start = buf.len();
        buf.extend_from_slice(&[0; 4]);
        Self {
            buf,
            start,
            index: None,
        }
    }

    fn key(&mut self, kind: u8, key: &str) -> Res<()> {
        if key.as_bytes().contains(&0) {
            return Err(protocol("BSON key contains NUL"));
        }
        self.buf.push(kind);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        Ok(())
    }

    fn index_key(&mut self, kind: u8) {
        let index = self.index.unwrap_or_default();
        self.index = Some(index + 1);
        self.buf.push(kind);
        self.buf.extend_from_slice(index.to_string().as_bytes());
        self.buf.push(0);
    }

    /// A string field.
    pub(super) fn str(&mut self, key: &str, value: &str) -> Res<()> {
        if value.as_bytes().contains(&0) {
            return Err(protocol("BSON string contains NUL"));
        }
        self.key(TYPE_STRING, key)?;
        let too_long = |_| protocol("BSON string too long");
        let len = i32::try_from(value.len() + 1).map_err(too_long)?;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(value.as_bytes());
        self.buf.push(0);
        Ok(())
    }

    /// An `int32` field.
    pub(super) fn i32(&mut self, key: &str, value: i32) -> Res<()> {
        self.key(TYPE_INT32, key)?;
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// An `int64` field.
    pub(super) fn i64(&mut self, key: &str, value: i64) -> Res<()> {
        self.key(TYPE_INT64, key)?;
        self.buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    /// A boolean field.
    pub(super) fn bool(&mut self, key: &str, value: bool) -> Res<()> {
        self.key(TYPE_BOOL, key)?;
        self.buf.push(u8::from(value));
        Ok(())
    }

    /// An embedded document, serialized from `doc`.
    pub(super) fn doc(&mut self, key: &str, doc: &Document) -> Res<()> {
        self.key(TYPE_DOCUMENT, key)?;
        write_document(self.buf, doc)
    }

    /// An embedded document to fill in; finish it before using `self` again.
    pub(super) fn sub(&mut self, key: &str) -> Res<DocWriter<'_>> {
        self.key(TYPE_DOCUMENT, key)?;
        Ok(DocWriter::begin(self.buf))
    }

    /// An array to fill in with [`push_doc`](Self::push_doc).
    pub(super) fn array(&mut self, key: &str) -> Res<DocWriter<'_>> {
        self.key(TYPE_ARRAY, key)?;
        let mut array = DocWriter::begin(self.buf);
        array.index = Some(0);
        Ok(array)
    }

    /// Append `doc` to an array.
    pub(super) fn push_doc(&mut self, doc: &Document) -> Res<()> {
        self.index_key(TYPE_DOCUMENT);
        write_document(self.buf, doc)
    }

    /// Append an `int64` to an array.
    pub(super) fn push_i64(&mut self, value: i64) {
        self.index_key(TYPE_INT64);
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    /// Close the document and fill in its length.
    pub(super) fn finish(self) -> Res<()> {
        self.buf.push(0);
        let len = i32::try_from(self.buf.len() - self.start)
            .map_err(|_| protocol("BSON document too large"))?;
        let at = self.start;
        self.buf[at..at + 4].copy_from_slice(&len.to_le_bytes());
        Ok(())
    }
}

fn write_document(buf: &mut Vec<u8>, doc: &Document) -> Res<()> {
    doc.to_writer(buf).map_err(|e| protocol(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mongodb::bson::{doc, rawdoc};

    #[test]
    fn writer_matches_the_bson_crate() {
        let mut buf = Vec::new();
        let mut w = DocWriter::begin(&mut buf);
        w.str("find", "todos").unwrap();
        w.doc("filter", &doc! { "_id": 1_i64 }).unwrap();
        w.i64("limit", 2).unwrap();
        w.i32("batchSize", 3).unwrap();
        w.bool("singleBatch", false).unwrap();
        let mut a = w.array("pipeline").unwrap();
        a.push_doc(&doc! { "$match": {} }).unwrap();
        a.push_i64(7);
        a.finish().unwrap();
        let mut s = w.sub("cursor").unwrap();
        s.i32("batchSize", 0).unwrap();
        s.finish().unwrap();
        w.finish().unwrap();
        let expected = rawdoc! {
            "find": "todos", "filter": { "_id": 1_i64 }, "limit": 2_i64,
            "batchSize": 3_i32, "singleBatch": false,
            "pipeline": [{ "$match": {} }, 7_i64],
            "cursor": { "batchSize": 0_i32 },
        };
        assert_eq!(buf, expected.as_bytes());
    }

    #[test]
    fn nul_bytes_are_rejected() {
        let mut buf = Vec::new();
        let mut w = DocWriter::begin(&mut buf);
        assert!(w.str("a\0", "x").is_err());
        assert!(w.str("a", "x\0").is_err());
    }

    #[test]
    fn frames_round_trip() {
        let mut out = Vec::new();
        begin_request(&mut out);
        let mut w = DocWriter::begin(&mut out);
        w.i32("ok", 1).unwrap();
        w.finish().unwrap();
        finish_request(&mut out, 7).unwrap();
        assert_eq!(message_len(&out).unwrap(), out.len());
        // Answer it: same bytes, responseTo = 7.
        let mut reply = out.clone();
        reply[8..12].copy_from_slice(&7_i32.to_le_bytes());
        let body = reply_body(&reply, 7).unwrap();
        assert_eq!(body.get_i32("ok").unwrap(), 1);
        assert_eq!(request_body(&out), body.as_bytes());
        assert!(reply_body(&reply, 8).is_err());
        reply[16] = 2; // moreToCome
        assert!(reply_body(&reply, 7).is_err());
    }
}
