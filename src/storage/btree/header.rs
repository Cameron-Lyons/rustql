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

pub(super) fn read_versioned_header_with_versions(
    path: &Path,
    expected_magic: [u8; 8],
    supported_versions: &[u32],
    label: &str,
) -> Result<VersionedFileState, RustqlError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(VersionedFileState::Missing);
        }
        Err(error) => {
            return Err(RustqlError::StorageError(format!(
                "Failed to stat {}: {}",
                label, error
            )));
        }
    };

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

    let mut header = [0u8; FILE_HEADER_SIZE];
    header[..8].copy_from_slice(&magic);
    header[8..12].copy_from_slice(&version.to_le_bytes());
    header[12..16].copy_from_slice(&HEADER_RESERVED.to_le_bytes());

    file.write_all(&header)
        .map_err(|e| RustqlError::StorageError(format!("Failed to write {} header: {}", label, e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "rustql_{name}_{}_{}_{}",
            std::process::id(),
            timestamp,
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        fn new(name: &str) -> Self {
            Self {
                path: temp_path(name),
            }
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn missing_versioned_header_returns_missing_state() {
        let temp_file = TempFile::new("missing_header");
        let state = read_versioned_header_with_versions(
            &temp_file.path,
            *b"TESTHDR\0",
            &[1],
            "test header",
        )
        .expect("missing header should not error");

        assert!(matches!(state, VersionedFileState::Missing));
    }

    #[test]
    fn write_versioned_header_writes_fixed_header_bytes() {
        let temp_file = TempFile::new("write_header");
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temp_file.path)
            .expect("failed to create temporary header file");

        write_versioned_header(&mut file, *b"TESTHDR\0", 0x0102_0304, "test header")
            .expect("failed to write header");
        drop(file);

        let bytes = std::fs::read(&temp_file.path).expect("failed to read header file");
        assert_eq!(bytes.len(), FILE_HEADER_SIZE);
        assert_eq!(&bytes[..8], b"TESTHDR\0");
        assert_eq!(&bytes[8..12], &0x0102_0304u32.to_le_bytes());
        assert_eq!(&bytes[12..16], &HEADER_RESERVED.to_le_bytes());
    }
}
