use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn with_temp_storage<R>(label: &str, f: impl FnOnce(&Path) -> R) -> R {
    let path = unique_temp_path(label);
    cleanup_storage_files(&path);
    let result = f(&path);
    cleanup_storage_files(&path);
    result
}

pub fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn unique_temp_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rustql-fuzz-{}-{}-{}-{}",
        label,
        std::process::id(),
        timestamp,
        counter
    ))
}

fn cleanup_storage_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(wal_path(path));
}
