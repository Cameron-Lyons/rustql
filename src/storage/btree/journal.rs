use super::super::atomic_file::{
    atomic_write, cleanup_temp_file, rename_synced, storage_temp_path, sync_parent_dir,
};
use super::BTreeStorageEngine;
use super::file::BTreeFile;
use super::header::{
    FILE_HEADER_SIZE, HEADER_RESERVED, VersionedFileState, read_versioned_header_with_versions,
};
use super::page::BTREE_PAGE_SIZE;
use crate::database::Database;
use crate::error::RustqlError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub(super) const JOURNAL_MAGIC: [u8; 8] = *b"RSTQLJW\0";
pub(super) const LEGACY_JOURNAL_VERSION: u32 = 1;
pub(super) const JOURNAL_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
pub(super) enum TransactionJournal {
    Pending,
    Committed { redo: BTreeRedoJournal },
}

#[derive(Serialize, Deserialize)]
pub(super) struct BTreeRedoJournal {
    pub(super) storage_header: Vec<u8>,
    pub(super) page_size: usize,
    pub(super) base_file_len: u64,
    pub(super) target_file_len: u64,
    pub(super) base_checksum: u64,
    pub(super) target_checksum: u64,
    pub(super) frames: Vec<BTreePageFrame>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct BTreePageFrame {
    pub(super) page_id: u64,
    pub(super) checksum: u64,
    pub(super) bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(super) enum LegacyTransactionJournal {
    Pending,
    Committed { database: Database },
}

pub(super) enum LoadedTransactionJournal {
    Pending,
    Committed { redo: BTreeRedoJournal },
}

impl BTreeRedoJournal {
    pub(super) fn from_file_images(
        base_bytes: &[u8],
        target_bytes: &[u8],
    ) -> Result<Self, RustqlError> {
        if target_bytes.len() < FILE_HEADER_SIZE {
            return Err(RustqlError::StorageError(
                "Cannot build redo journal from truncated target BTree file".to_string(),
            ));
        }

        if target_bytes[0..8] != BTreeFile::MAGIC {
            return Err(RustqlError::StorageError(
                "Cannot build redo journal from target file with invalid BTree header".to_string(),
            ));
        }

        let storage_version = u32::from_le_bytes([
            target_bytes[8],
            target_bytes[9],
            target_bytes[10],
            target_bytes[11],
        ]);
        if storage_version != BTreeFile::VERSION {
            return Err(RustqlError::StorageError(format!(
                "Cannot build redo journal for unsupported BTree storage version: {}",
                storage_version
            )));
        }

        let payload_len = target_bytes.len() - FILE_HEADER_SIZE;
        if payload_len % BTREE_PAGE_SIZE != 0 {
            return Err(RustqlError::StorageError(
                "Cannot build redo journal from target BTree file with partial page".to_string(),
            ));
        }

        let mut frames = Vec::new();
        for page_id in 0..(payload_len / BTREE_PAGE_SIZE) {
            let target_page =
                page_bytes_at(target_bytes, page_id, BTREE_PAGE_SIZE).ok_or_else(|| {
                    RustqlError::StorageError(format!(
                        "Target BTree page {} is missing while building redo journal",
                        page_id
                    ))
                })?;
            if page_bytes_at(base_bytes, page_id, BTREE_PAGE_SIZE) == Some(target_page) {
                continue;
            }

            frames.push(BTreePageFrame {
                page_id: page_id as u64,
                checksum: storage_checksum(target_page),
                bytes: target_page.to_vec(),
            });
        }

        Ok(Self {
            storage_header: target_bytes[..FILE_HEADER_SIZE].to_vec(),
            page_size: BTREE_PAGE_SIZE,
            base_file_len: base_bytes.len() as u64,
            target_file_len: target_bytes.len() as u64,
            base_checksum: storage_checksum(base_bytes),
            target_checksum: storage_checksum(target_bytes),
            frames,
        })
    }

    pub(super) fn validate(&self) -> Result<(), RustqlError> {
        if self.storage_header.len() != FILE_HEADER_SIZE {
            return Err(RustqlError::StorageError(format!(
                "Redo journal storage header has invalid length: {}",
                self.storage_header.len()
            )));
        }
        if self.storage_header[0..8] != BTreeFile::MAGIC {
            return Err(RustqlError::StorageError(
                "Redo journal target storage header is invalid".to_string(),
            ));
        }
        let storage_version = u32::from_le_bytes([
            self.storage_header[8],
            self.storage_header[9],
            self.storage_header[10],
            self.storage_header[11],
        ]);
        if storage_version != BTreeFile::VERSION {
            return Err(RustqlError::StorageError(format!(
                "Redo journal targets unsupported BTree storage version: {}",
                storage_version
            )));
        }
        if self.page_size != BTREE_PAGE_SIZE {
            return Err(RustqlError::StorageError(format!(
                "Redo journal page size {} does not match BTree page size {}",
                self.page_size, BTREE_PAGE_SIZE
            )));
        }
        if self.target_file_len < FILE_HEADER_SIZE as u64 {
            return Err(RustqlError::StorageError(
                "Redo journal target file length is shorter than the storage header".to_string(),
            ));
        }

        let target_payload_len = self.target_file_len - FILE_HEADER_SIZE as u64;
        if target_payload_len % self.page_size as u64 != 0 {
            return Err(RustqlError::StorageError(
                "Redo journal target file length ends with a partial page".to_string(),
            ));
        }

        let target_page_count = target_payload_len / self.page_size as u64;
        let mut seen_pages = std::collections::HashSet::new();
        for frame in &self.frames {
            if !seen_pages.insert(frame.page_id) {
                return Err(RustqlError::StorageError(format!(
                    "Redo journal contains duplicate frame for page {}",
                    frame.page_id
                )));
            }
            if frame.page_id >= target_page_count {
                return Err(RustqlError::StorageError(format!(
                    "Redo journal frame page {} is outside target file",
                    frame.page_id
                )));
            }
            if frame.bytes.len() != self.page_size {
                return Err(RustqlError::StorageError(format!(
                    "Redo journal frame {} has invalid page length {}",
                    frame.page_id,
                    frame.bytes.len()
                )));
            }
            let checksum = storage_checksum(&frame.bytes);
            if checksum != frame.checksum {
                return Err(RustqlError::StorageError(format!(
                    "Redo journal frame {} checksum mismatch",
                    frame.page_id
                )));
            }
        }

        Ok(())
    }

    pub(super) fn current_matches_base(&self, bytes: &[u8]) -> bool {
        file_identity_matches(bytes, self.base_file_len, self.base_checksum)
    }

    pub(super) fn current_matches_target(&self, bytes: &[u8]) -> bool {
        file_identity_matches(bytes, self.target_file_len, self.target_checksum)
    }
}

pub(super) fn storage_checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(super) fn file_identity_matches(
    bytes: &[u8],
    expected_len: u64,
    expected_checksum: u64,
) -> bool {
    bytes.len() as u64 == expected_len && storage_checksum(bytes) == expected_checksum
}

pub(super) fn page_bytes_at(bytes: &[u8], page_id: usize, page_size: usize) -> Option<&[u8]> {
    let start = FILE_HEADER_SIZE.checked_add(page_id.checked_mul(page_size)?)?;
    let end = start.checked_add(page_size)?;
    bytes.get(start..end)
}

pub(super) fn read_storage_file_bytes(path: &Path) -> Result<Vec<u8>, RustqlError> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(RustqlError::StorageError(format!(
            "Failed to read BTree storage file '{}': {}",
            path.display(),
            error
        ))),
    }
}

impl BTreeStorageEngine {
    pub(super) fn journal_path(&self) -> PathBuf {
        let mut path = self.data_path.as_os_str().to_os_string();
        path.push(".wal");
        PathBuf::from(path)
    }

    pub(super) fn write_journal_locked(
        &self,
        journal: &TransactionJournal,
    ) -> Result<(), RustqlError> {
        let path = self.journal_path();
        let payload = serde_json::to_vec(journal).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to serialize transaction journal '{}': {}",
                path.display(),
                e
            ))
        })?;
        let mut data = Vec::with_capacity(FILE_HEADER_SIZE + payload.len());
        data.extend_from_slice(&JOURNAL_MAGIC);
        data.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        data.extend_from_slice(&HEADER_RESERVED.to_le_bytes());
        data.extend_from_slice(&payload);

        atomic_write(&path, &data)
    }

    pub(super) fn read_journal_locked(
        &self,
    ) -> Result<Option<LoadedTransactionJournal>, RustqlError> {
        let path = self.journal_path();
        match read_versioned_header_with_versions(
            &path,
            JOURNAL_MAGIC,
            &[LEGACY_JOURNAL_VERSION, JOURNAL_VERSION],
            "transaction journal",
        )? {
            VersionedFileState::Missing | VersionedFileState::Empty => Ok(None),
            VersionedFileState::Valid { version } => {
                let mut file = std::fs::OpenOptions::new()
                    .read(true)
                    .open(&path)
                    .map_err(|e| {
                        RustqlError::StorageError(format!(
                            "Failed to open transaction journal '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                file.seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))
                    .map_err(|e| {
                        RustqlError::StorageError(format!(
                            "Failed to seek transaction journal '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                let mut payload = Vec::new();
                file.read_to_end(&mut payload).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to read transaction journal '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
                if payload.is_empty() {
                    return Ok(None);
                }
                match version {
                    LEGACY_JOURNAL_VERSION => {
                        let journal: LegacyTransactionJournal = serde_json::from_slice(&payload)
                            .map_err(|e| {
                                RustqlError::StorageError(format!(
                                    "Failed to decode legacy transaction journal '{}': {}",
                                    path.display(),
                                    e
                                ))
                            })?;
                        match journal {
                            LegacyTransactionJournal::Pending => {
                                Ok(Some(LoadedTransactionJournal::Pending))
                            }
                            LegacyTransactionJournal::Committed { database } => {
                                Ok(Some(LoadedTransactionJournal::Committed {
                                    redo: self.build_redo_journal_locked(&database)?,
                                }))
                            }
                        }
                    }
                    JOURNAL_VERSION => {
                        let journal: TransactionJournal = serde_json::from_slice(&payload)
                            .map_err(|e| {
                                RustqlError::StorageError(format!(
                                    "Failed to decode transaction journal '{}': {}",
                                    path.display(),
                                    e
                                ))
                            })?;
                        match journal {
                            TransactionJournal::Pending => {
                                Ok(Some(LoadedTransactionJournal::Pending))
                            }
                            TransactionJournal::Committed { redo } => {
                                Ok(Some(LoadedTransactionJournal::Committed { redo }))
                            }
                        }
                    }
                    other => Err(RustqlError::StorageError(format!(
                        "Unsupported transaction journal format version: {}",
                        other
                    ))),
                }
            }
        }
    }

    pub(super) fn build_redo_journal_locked(
        &self,
        db: &Database,
    ) -> Result<BTreeRedoJournal, RustqlError> {
        let base_bytes = read_storage_file_bytes(&self.data_path)?;
        let target_path = storage_temp_path(&self.data_path);
        let result = (|| {
            let mut db = db.clone();
            db.normalize_row_ids();
            let mut file = BTreeFile::create(&target_path)?;
            file.write_database_via_pages(&db)?;
            file.sync_all()?;
            drop(file);

            let target_bytes = fs::read(&target_path).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to read prepared BTree image '{}': {}",
                    target_path.display(),
                    e
                ))
            })?;

            BTreeRedoJournal::from_file_images(&base_bytes, &target_bytes)
        })();
        cleanup_temp_file(&target_path);
        result
    }

    pub(super) fn apply_redo_journal_locked(
        &self,
        redo: &BTreeRedoJournal,
    ) -> Result<(), RustqlError> {
        redo.validate()?;

        let current_bytes = read_storage_file_bytes(&self.data_path)?;
        if redo.current_matches_target(&current_bytes) {
            self.clear_cache();
            return Ok(());
        }
        if !redo.current_matches_base(&current_bytes) {
            return Err(RustqlError::StorageError(
                "Cannot replay transaction journal: BTree storage file does not match the journal base or target image".to_string(),
            ));
        }

        let temp_path = storage_temp_path(&self.data_path);
        let result = (|| {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&temp_path)
                .map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to create temporary BTree recovery file '{}': {}",
                        temp_path.display(),
                        e
                    ))
                })?;

            if !current_bytes.is_empty() {
                file.write_all(&current_bytes).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to seed temporary BTree recovery file '{}': {}",
                        temp_path.display(),
                        e
                    ))
                })?;
            }

            file.seek(SeekFrom::Start(0)).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to seek temporary BTree recovery file '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;
            file.write_all(&redo.storage_header).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to write BTree recovery header '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;

            for frame in &redo.frames {
                let offset = FILE_HEADER_SIZE as u64 + frame.page_id * redo.page_size as u64;
                file.seek(SeekFrom::Start(offset)).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to seek BTree recovery frame {} in '{}': {}",
                        frame.page_id,
                        temp_path.display(),
                        e
                    ))
                })?;
                file.write_all(&frame.bytes).map_err(|e| {
                    RustqlError::StorageError(format!(
                        "Failed to write BTree recovery frame {} in '{}': {}",
                        frame.page_id,
                        temp_path.display(),
                        e
                    ))
                })?;
            }

            file.set_len(redo.target_file_len).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to set BTree recovery file length '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;
            file.sync_all().map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to sync BTree recovery file '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;
            drop(file);

            let recovered_bytes = fs::read(&temp_path).map_err(|e| {
                RustqlError::StorageError(format!(
                    "Failed to verify BTree recovery file '{}': {}",
                    temp_path.display(),
                    e
                ))
            })?;
            if !redo.current_matches_target(&recovered_bytes) {
                return Err(RustqlError::StorageError(
                    "Recovered BTree image does not match transaction journal target checksum"
                        .to_string(),
                ));
            }

            rename_synced(&temp_path, &self.data_path)
        })();
        if result.is_err() {
            cleanup_temp_file(&temp_path);
        }
        self.clear_cache();
        result
    }

    pub(super) fn clear_journal_locked(&self) -> Result<(), RustqlError> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(());
        }
        fs::remove_file(&path).map_err(|e| {
            RustqlError::StorageError(format!(
                "Failed to remove transaction journal '{}': {}",
                path.display(),
                e
            ))
        })?;
        sync_parent_dir(&path)
    }

    pub(super) fn recover_if_needed_locked(&self) -> Result<(), RustqlError> {
        let Some(journal) = self.read_journal_locked()? else {
            return Ok(());
        };

        match journal {
            LoadedTransactionJournal::Pending => {
                self.clear_journal_locked()?;
            }
            LoadedTransactionJournal::Committed { redo } => {
                self.apply_redo_journal_locked(&redo)?;
                self.clear_journal_locked()?;
            }
        }

        Ok(())
    }
}
