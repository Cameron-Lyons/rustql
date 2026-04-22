use crate::error::RustqlError;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) const FILE_HEADER_SIZE: usize = 16;
pub(super) const HEADER_RESERVED: u32 = 0;

pub(super) enum VersionedFileState {
    Missing,
    Empty,
    Valid { version: u32 },
}

pub(super) fn read_versioned_header(
    path: &Path,
    expected_magic: [u8; 8],
    expected_version: u32,
    label: &str,
) -> Result<VersionedFileState, RustqlError> {
    read_versioned_header_with_versions(path, expected_magic, &[expected_version], label)
}

pub(super) fn read_versioned_header_with_versions(
    path: &Path,
    expected_magic: [u8; 8],
    supported_versions: &[u32],
    label: &str,
) -> Result<VersionedFileState, RustqlError> {
    if !path.exists() {
        return Ok(VersionedFileState::Missing);
    }

    let metadata = fs::metadata(path)
        .map_err(|e| RustqlError::StorageError(format!("Failed to stat {}: {}", label, e)))?;

    if metadata.len() == 0 {
        return Ok(VersionedFileState::Empty);
    }

    if metadata.len() < FILE_HEADER_SIZE as u64 {
        return Err(RustqlError::StorageError(format!(
            "{} header is truncated",
            label
        )));
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| RustqlError::StorageError(format!("Failed to open {}: {}", label, e)))?;

    let mut header = [0u8; FILE_HEADER_SIZE];
    file.read_exact(&mut header).map_err(|e| {
        RustqlError::StorageError(format!("Failed to read {} header: {}", label, e))
    })?;

    if header[0..8] != expected_magic {
        return Err(RustqlError::StorageError(format!(
            "{} has an invalid header",
            label
        )));
    }

    let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    if !supported_versions.contains(&version) {
        return Err(RustqlError::StorageError(format!(
            "Unsupported {} format version: {}",
            label, version
        )));
    }

    Ok(VersionedFileState::Valid { version })
}

pub(super) fn write_versioned_header(
    file: &mut std::fs::File,
    magic: [u8; 8],
    version: u32,
    label: &str,
) -> Result<(), RustqlError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|e| RustqlError::StorageError(format!("Failed to seek {}: {}", label, e)))?;

    let mut header = Vec::with_capacity(FILE_HEADER_SIZE);
    header.extend_from_slice(&magic);
    header.extend_from_slice(&version.to_le_bytes());
    header.extend_from_slice(&HEADER_RESERVED.to_le_bytes());

    file.write_all(&header)
        .map_err(|e| RustqlError::StorageError(format!("Failed to write {} header: {}", label, e)))
}
