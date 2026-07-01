use crate::error::RustqlError;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(super) fn storage_temp_path(path: &Path) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let mut file_name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| "rustql_storage".into());
    file_name.push(format!(
        ".tmp.{}.{}.{}",
        std::process::id(),
        timestamp,
        TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));

    match path.parent() {
        Some(parent) => parent.join(file_name),
        None => PathBuf::from(file_name),
    }
}

pub(super) fn sync_parent_dir(path: &Path) -> Result<(), RustqlError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to sync storage directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
    }
    Ok(())
}

pub(super) fn rename_synced(temp_path: &Path, path: &Path) -> Result<(), RustqlError> {
    fs::rename(temp_path, path).map_err(|e| {
        RustqlError::StorageError(format!(
            "Failed to atomically replace storage file '{}' with '{}': {}",
            path.display(),
            temp_path.display(),
            e
        ))
    })?;
    sync_parent_dir(path)
}

pub(super) fn cleanup_temp_file(temp_path: &Path) {
    let _ = fs::remove_file(temp_path);
}

pub(super) fn atomic_write(path: &Path, data: &[u8]) -> Result<(), RustqlError> {
    let temp_path = storage_temp_path(path);
    atomic_write_with_temp_path(path, &temp_path, data)
}

fn atomic_write_with_temp_path(
    path: &Path,
    temp_path: &Path,
    data: &[u8],
) -> Result<(), RustqlError> {
    let mut temp_created = false;
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temp_path)
            .map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to create temporary storage file '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;
        temp_created = true;

        file.write_all(data).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to write temporary storage file '{}': {}",
                temp_path.display(),
                e
            ))
        })?;
        file.sync_all().map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to sync temporary storage file '{}': {}",
                temp_path.display(),
                e
            ))
        })?;
        drop(file);

        rename_synced(temp_path, path)
    })();

    if result.is_err() && temp_created {
        cleanup_temp_file(temp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn unique_test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rustql_atomic_file_test_{}_{}_{}",
            name,
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).expect("failed to create test directory");
        dir
    }

    fn cleanup_test_dir(dir: &Path) {
        match fs::remove_dir_all(dir) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => panic!(
                "failed to remove test directory '{}': {}",
                dir.display(),
                err
            ),
        }
    }

    #[test]
    fn atomic_write_preserves_preexisting_temp_file_on_create_collision() {
        let dir = unique_test_dir("collision");
        let target_path = dir.join("database.json");
        let temp_path = dir.join("database.json.tmp");
        fs::write(&temp_path, b"owned by another writer").expect("failed to seed temp file");

        let result = atomic_write_with_temp_path(&target_path, &temp_path, b"new data");

        assert!(result.is_err());
        assert_eq!(
            fs::read(&temp_path).expect("preexisting temp file should remain"),
            b"owned by another writer"
        );
        assert!(!target_path.exists());
        cleanup_test_dir(&dir);
    }
}
