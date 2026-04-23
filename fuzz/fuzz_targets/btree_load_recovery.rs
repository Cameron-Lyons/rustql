#![no_main]

mod common;

use libfuzzer_sys::fuzz_target;
use rustql::storage::{BTreeStorageEngine, StorageEngine};

fuzz_target!(|data: &[u8]| {
    let (flags, payload) = data
        .split_first()
        .map_or((0u8, &[][..]), |(&flags, rest)| (flags, rest));
    let write_journal = flags & 1 != 0;
    let split = if write_journal {
        ((flags as usize) >> 1) % (payload.len() + 1)
    } else {
        payload.len()
    };
    let (storage_bytes, journal_bytes) = payload.split_at(split);

    common::with_temp_storage("btree-load-recovery", |path| {
        let _ = std::fs::write(path, storage_bytes);
        if write_journal {
            let _ = std::fs::write(common::wal_path(path), journal_bytes);
        }

        let engine = BTreeStorageEngine::new(path);
        let _ = engine.load();
        let _ = engine.load();
    });
});
