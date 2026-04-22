use crate::ast::Value;
use crate::error::RustqlError;
use serde::{Deserialize, Serialize};

pub const BTREE_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageKind {
    Meta,
    Internal,
    Leaf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub page_id: u64,

    pub kind: PageKind,

    pub entry_count: u16,

    pub reserved: u16,
}

impl PageHeader {
    pub fn new(page_id: u64, kind: PageKind) -> Self {
        PageHeader {
            page_id,
            kind,
            entry_count: 0,
            reserved: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreePage {
    pub header: PageHeader,

    pub entries: Vec<BTreeEntry>,
}

impl BTreePage {
    pub fn new(page_id: u64, kind: PageKind) -> Self {
        BTreePage {
            header: PageHeader::new(page_id, kind),
            entries: Vec::new(),
        }
    }

    fn stores_inline_leaf_data(&self) -> bool {
        self.header.kind == PageKind::Leaf && self.header.reserved & LEAF_INLINE_DATA_FLAG != 0
    }

    pub fn can_accept_entry(&self, entry: &BTreeEntry) -> bool {
        let current_size = self.estimated_size();
        let added = entry.estimated_size(self.header.kind, self.header.reserved);
        current_size + added <= BTREE_PAGE_SIZE
    }

    fn estimated_size(&self) -> usize {
        let header_size = 16usize;
        let entries_size: usize = self
            .entries
            .iter()
            .map(|e| e.estimated_size(self.header.kind, self.header.reserved))
            .sum();
        header_size + entries_size
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeEntry {
    pub key: Value,

    pub pointer: u64,

    #[serde(default)]
    pub inline_data: Option<String>,
}

impl BTreeEntry {
    pub fn new(key: Value, pointer: u64) -> Self {
        BTreeEntry {
            key,
            pointer,
            inline_data: None,
        }
    }

    pub fn with_inline_data(key: Value, inline_data: String) -> Self {
        BTreeEntry {
            key,
            pointer: 0,
            inline_data: Some(inline_data),
        }
    }

    fn estimated_size(&self, kind: PageKind, reserved: u16) -> usize {
        let key_size = match &self.key {
            Value::Null => 1,
            Value::Integer(_) => 9,
            Value::Float(_) => 9,
            Value::Boolean(_) => 2,
            Value::Text(s) | Value::Date(s) | Value::Time(s) | Value::DateTime(s) => 1 + s.len(),
        };
        let value_size = if kind == PageKind::Leaf && reserved & LEAF_INLINE_DATA_FLAG != 0 {
            4 + self
                .inline_data
                .as_ref()
                .map_or(0, |inline_data| inline_data.len())
        } else {
            8
        };
        key_size + value_size
    }
}

const TAG_NULL: u8 = 0x00;
const TAG_INTEGER: u8 = 0x01;
const TAG_FLOAT: u8 = 0x02;
const TAG_TEXT: u8 = 0x03;
const TAG_BOOLEAN: u8 = 0x04;
const TAG_DATE: u8 = 0x05;
const TAG_TIME: u8 = 0x06;
const TAG_DATETIME: u8 = 0x07;
pub(super) const LEAF_INLINE_DATA_FLAG: u16 = 0x0001;

fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Integer(i) => {
            buf.push(TAG_INTEGER);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Boolean(b) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Date(s) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Time(s) => {
            buf.push(TAG_TIME);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::DateTime(s) => {
            buf.push(TAG_DATETIME);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

fn decode_value(data: &[u8], offset: &mut usize) -> Result<Value, RustqlError> {
    if *offset >= data.len() {
        return Err(RustqlError::StorageError(
            "Unexpected end of binary entry data".to_string(),
        ));
    }
    let tag = data[*offset];
    *offset += 1;
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_INTEGER => {
            if *offset + 8 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated integer in binary entry".to_string(),
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[*offset..*offset + 8]);
            let val = i64::from_le_bytes(bytes);
            *offset += 8;
            Ok(Value::Integer(val))
        }
        TAG_FLOAT => {
            if *offset + 8 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated float in binary entry".to_string(),
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[*offset..*offset + 8]);
            let val = f64::from_le_bytes(bytes);
            *offset += 8;
            Ok(Value::Float(val))
        }
        TAG_TEXT | TAG_DATE | TAG_TIME | TAG_DATETIME => {
            if *offset + 4 > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated string length in binary entry".to_string(),
                ));
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&data[*offset..*offset + 4]);
            let len = u32::from_le_bytes(len_bytes) as usize;
            *offset += 4;
            if *offset + len > data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated string data in binary entry".to_string(),
                ));
            }
            let s = std::str::from_utf8(&data[*offset..*offset + len])
                .map_err(|e| {
                    RustqlError::StorageError(format!("Invalid UTF-8 in binary entry: {}", e))
                })?
                .to_owned();
            *offset += len;
            match tag {
                TAG_TEXT => Ok(Value::Text(s)),
                TAG_DATE => Ok(Value::Date(s)),
                TAG_TIME => Ok(Value::Time(s)),
                TAG_DATETIME => Ok(Value::DateTime(s)),
                other => Err(RustqlError::StorageError(format!(
                    "Invalid string-like binary entry tag: {}",
                    other
                ))),
            }
        }
        TAG_BOOLEAN => {
            if *offset >= data.len() {
                return Err(RustqlError::StorageError(
                    "Truncated boolean in binary entry".to_string(),
                ));
            }
            let val = data[*offset] != 0;
            *offset += 1;
            Ok(Value::Boolean(val))
        }
        other => Err(RustqlError::StorageError(format!(
            "Unknown value type tag: 0x{:02x}",
            other
        ))),
    }
}

impl BTreePage {
    pub fn to_bytes(&self) -> Result<[u8; BTREE_PAGE_SIZE], RustqlError> {
        let mut buf = [0u8; BTREE_PAGE_SIZE];

        let kind_byte = match self.header.kind {
            PageKind::Meta => 0u8,
            PageKind::Internal => 1u8,
            PageKind::Leaf => 2u8,
        };

        buf[0..8].copy_from_slice(&self.header.page_id.to_le_bytes());
        buf[8] = kind_byte;
        buf[9..11].copy_from_slice(&self.header.entry_count.to_le_bytes());
        buf[11..13].copy_from_slice(&self.header.reserved.to_le_bytes());

        let mut payload = Vec::new();
        for entry in &self.entries {
            encode_value(&mut payload, &entry.key);
            if self.stores_inline_leaf_data() {
                let inline_data = entry.inline_data.as_ref().ok_or_else(|| {
                    RustqlError::StorageError(
                        "Leaf entry is missing inline data for inline leaf page".to_string(),
                    )
                })?;
                payload.extend_from_slice(&(inline_data.len() as u32).to_le_bytes());
                payload.extend_from_slice(inline_data.as_bytes());
            } else {
                payload.extend_from_slice(&entry.pointer.to_le_bytes());
            }
        }

        let header_size = 16usize;
        if header_size + payload.len() > BTREE_PAGE_SIZE {
            return Err(RustqlError::StorageError(
                "BTreePage too large to fit in fixed page size".to_string(),
            ));
        }

        buf[header_size..header_size + payload.len()].copy_from_slice(&payload);
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8; BTREE_PAGE_SIZE]) -> Result<Self, RustqlError> {
        let page_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let kind = match buf[8] {
            0 => PageKind::Meta,
            1 => PageKind::Internal,
            2 => PageKind::Leaf,
            other => {
                return Err(RustqlError::StorageError(format!(
                    "Unknown BTree page kind byte: {}",
                    other
                )));
            }
        };
        let entry_count = u16::from_le_bytes([buf[9], buf[10]]);
        let reserved = u16::from_le_bytes([buf[11], buf[12]]);

        let header = PageHeader {
            page_id,
            kind,
            entry_count,
            reserved,
        };

        let header_size = 16usize;

        if entry_count == 0 {
            return Ok(BTreePage {
                header,
                entries: Vec::new(),
            });
        }

        if buf[header_size] == b'[' {
            let mut payload = &buf[header_size..];
            if let Some(last) = payload.iter().rposition(|b| *b != 0) {
                payload = &payload[..=last];
            } else {
                payload = &[];
            }
            let entries: Vec<BTreeEntry> = if payload.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(payload).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to decode legacy JSON BTree entries: {}",
                        e
                    ))
                })?
            };
            return Ok(BTreePage { header, entries });
        }

        let mut entries = Vec::with_capacity(entry_count as usize);
        let mut offset = header_size;
        for _ in 0..entry_count {
            let key = decode_value(buf, &mut offset)?;
            if kind == PageKind::Leaf && reserved & LEAF_INLINE_DATA_FLAG != 0 {
                if offset + 4 > buf.len() {
                    return Err(RustqlError::StorageError(
                        "Truncated inline data length in binary entry".to_string(),
                    ));
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&buf[offset..offset + 4]);
                let len = u32::from_le_bytes(len_bytes) as usize;
                offset += 4;
                if offset + len > buf.len() {
                    return Err(RustqlError::StorageError(
                        "Truncated inline data in binary entry".to_string(),
                    ));
                }
                let inline_data = std::str::from_utf8(&buf[offset..offset + len])
                    .map_err(|e| {
                        RustqlError::StorageError(format!(
                            "Invalid UTF-8 in inline leaf payload: {}",
                            e
                        ))
                    })?
                    .to_owned();
                offset += len;
                entries.push(BTreeEntry::with_inline_data(key, inline_data));
                continue;
            }
            if offset + 8 > buf.len() {
                return Err(RustqlError::StorageError(
                    "Truncated pointer in binary entry".to_string(),
                ));
            }
            let mut pointer_bytes = [0u8; 8];
            pointer_bytes.copy_from_slice(&buf[offset..offset + 8]);
            let pointer = u64::from_le_bytes(pointer_bytes);
            offset += 8;
            entries.push(BTreeEntry::new(key, pointer));
        }

        Ok(BTreePage { header, entries })
    }
}
