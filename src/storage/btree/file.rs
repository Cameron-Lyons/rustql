use super::BTreeStorageEngine;
use super::header::{
    FILE_HEADER_SIZE, VersionedFileState, read_versioned_header_with_versions,
    write_versioned_header,
};
use super::page::{BTREE_PAGE_SIZE, BTreeEntry, BTreePage, LEAF_INLINE_DATA_FLAG, PageKind};
use super::records::{
    TableStorageRecord, format_row_storage_key, insert_loaded_row, parse_row_storage_key,
};
use crate::ast::Value;
use crate::database::{Database, RowId};
use crate::error::RustqlError;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) struct CachedBTreeFile<'a> {
    pub(super) engine: &'a BTreeStorageEngine,
}

impl<'a> CachedBTreeFile<'a> {
    pub(super) fn read_page(&self, page_id: u64) -> Result<BTreePage, RustqlError> {
        self.engine.read_page_cached(page_id)
    }

    pub(super) fn read_database_via_pages(&mut self) -> Result<Database, RustqlError> {
        match read_versioned_header_with_versions(
            &self.engine.data_path,
            BTreeFile::MAGIC,
            &[2, BTreeFile::VERSION],
            "BTree storage file",
        )? {
            VersionedFileState::Missing | VersionedFileState::Empty => return Ok(Database::new()),
            VersionedFileState::Valid { .. } => {}
        }

        let file_size = fs::metadata(&self.engine.data_path)
            .map_err(|e| RustqlError::StorageError(format!("Failed to get file metadata: {}", e)))?
            .len();

        if file_size < FILE_HEADER_SIZE as u64 + BTREE_PAGE_SIZE as u64 {
            return Ok(Database::new());
        }

        let meta_page = match self.read_page(0) {
            Ok(page) => page,
            Err(_) => return Ok(Database::new()),
        };
        let root_page_id = meta_page
            .entries
            .iter()
            .find(|e| {
                if let Value::Text(ref s) = e.key {
                    s == "root"
                } else {
                    false
                }
            })
            .map(|e| e.pointer)
            .unwrap_or(1);

        self.load_database_from_rows(root_page_id)
    }

    fn load_database_from_rows(&self, root_page_id: u64) -> Result<Database, RustqlError> {
        let mut db = Database::new();
        let mut pending_rows: HashMap<String, Vec<(RowId, Vec<Value>)>> = HashMap::new();

        for entry in self.range_scan_entries(None, None, root_page_id)? {
            let Value::Text(key_str) = &entry.key else {
                continue;
            };

            if let Some(table_name) = key_str.strip_prefix("schema:") {
                let schema: TableStorageRecord =
                    self.read_data_from_entry(&entry, format!("schema for table {}", table_name))?;

                db.tables.insert(
                    table_name.to_string(),
                    crate::database::Table::with_rows_and_ids(
                        schema.columns,
                        Vec::new(),
                        Vec::new(),
                        schema.next_row_id,
                        schema.constraints,
                    ),
                );
                if let Some(mut rows) = pending_rows.remove(table_name) {
                    rows.sort_by_key(|(row_id, _)| *row_id);
                    if let Some(table_ref) = db.tables.get_mut(table_name) {
                        for (row_id, row) in rows {
                            insert_loaded_row(table_ref, row_id, row);
                        }
                    }
                }
                continue;
            }

            if let Some(index_name) = key_str.strip_prefix("index:") {
                let index: crate::database::Index =
                    self.read_data_from_entry(&entry, format!("index {}", index_name))?;
                db.indexes.insert(index_name.to_string(), index);
                continue;
            }

            if let Some(index_name) = key_str.strip_prefix("cindex:") {
                let index: crate::database::CompositeIndex =
                    self.read_data_from_entry(&entry, format!("composite index {}", index_name))?;
                db.composite_indexes.insert(index_name.to_string(), index);
                continue;
            }

            if let Some(view_name) = key_str.strip_prefix("view:") {
                let view: crate::database::View =
                    self.read_data_from_entry(&entry, format!("view {}", view_name))?;
                db.views.insert(view_name.to_string(), view);
                continue;
            }

            if let Some((table_name, row_id, can_insert_in_order)) = parse_row_storage_key(key_str)
            {
                let row: Vec<Value> = self.read_data_from_entry(
                    &entry,
                    format!("row {} for table {}", row_id.0, table_name),
                )?;
                if can_insert_in_order && let Some(table_ref) = db.tables.get_mut(table_name) {
                    insert_loaded_row(table_ref, row_id, row);
                    continue;
                }
                pending_rows
                    .entry(table_name.to_string())
                    .or_default()
                    .push((row_id, row));
            }
        }

        for (table_name, mut row_indices) in pending_rows {
            row_indices.sort_by_key(|(row_id, _)| *row_id);
            if let Some(table_ref) = db.tables.get_mut(&table_name) {
                for (row_id, row) in row_indices {
                    insert_loaded_row(table_ref, row_id, row);
                }
            }
        }

        db.normalize_row_ids();
        Ok(db)
    }

    fn read_data_from_entry<T>(
        &self,
        entry: &BTreeEntry,
        label: impl Into<String>,
    ) -> Result<T, RustqlError>
    where
        T: DeserializeOwned,
    {
        let label = label.into();
        if let Some(inline_data) = entry.inline_data.as_deref() {
            return serde_json::from_str(inline_data).map_err(|e| {
                RustqlError::StorageError(format!("Failed to deserialize {}: {}", label, e))
            });
        }
        self.read_data_from_pointer(entry.pointer, label)
    }

    fn read_data_from_pointer<T>(
        &self,
        pointer: u64,
        label: impl Into<String>,
    ) -> Result<T, RustqlError>
    where
        T: DeserializeOwned,
    {
        let label = label.into();
        let data_page = self.read_page(pointer)?;

        if let Some(entry) = data_page.entries.first()
            && let Value::Text(ref json_str) = entry.key
        {
            return serde_json::from_str(json_str).map_err(|e| {
                RustqlError::StorageError(format!("Failed to deserialize {}: {}", label, e))
            });
        }

        Err(RustqlError::StorageError(format!(
            "Data page for {} does not contain valid JSON",
            label
        )))
    }

    fn range_scan_entries(
        &self,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<Vec<BTreeEntry>, RustqlError> {
        scan_pages_in_order_entries(
            |page_id| self.read_page(page_id),
            start_key,
            end_key,
            root_page_id,
        )
    }
}

pub(super) struct BTreeFile {
    pub(super) file: std::fs::File,
}

impl BTreeFile {
    pub(super) const MAGIC: [u8; 8] = *b"RSTQLBT\0";
    pub(super) const VERSION: u32 = 3;

    pub(super) fn open(path: &Path) -> Result<Self, RustqlError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| {
                RustqlError::StorageError(format!("Failed to open BTree storage file: {}", e))
            })?;
        Ok(BTreeFile { file })
    }

    pub(super) fn create(path: &Path) -> Result<Self, RustqlError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| {
                RustqlError::StorageError(format!("Failed to create BTree storage file: {}", e))
            })?;
        Ok(BTreeFile { file })
    }

    pub(super) fn sync_all(&self) -> Result<(), RustqlError> {
        self.file.sync_all().map_err(|e| {
            RustqlError::StorageError(format!("Failed to sync BTree storage file: {}", e))
        })
    }

    #[cfg(test)]
    pub(super) fn write_data_to_pointer(&mut self, data: &str) -> Result<u64, RustqlError> {
        let page_id = self.get_next_page_id()?;
        let mut data_page = BTreePage::new(page_id, PageKind::Leaf);

        data_page
            .entries
            .push(BTreeEntry::new(Value::Text(data.to_string()), 0));
        data_page.header.entry_count = data_page.entries.len() as u16;

        self.write_page(&data_page)?;
        Ok(page_id)
    }

    pub(super) fn write_database_via_pages(&mut self, db: &Database) -> Result<(), RustqlError> {
        // This serializes the full logical database into a new B-tree image.
        // It uses page primitives internally, but callers should treat it as a
        // snapshot writer rather than an incremental mutation path.
        write_versioned_header(
            &mut self.file,
            Self::MAGIC,
            Self::VERSION,
            "BTree storage file",
        )?;

        let mut meta_page = BTreePage::new(0, PageKind::Meta);
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
        meta_page
            .entries
            .push(BTreeEntry::new(Value::Text("next_page_id".to_string()), 2));
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        self.write_page(&meta_page)?;

        let root_page_id = 1;
        let mut root_page = BTreePage::new(root_page_id, PageKind::Leaf);
        root_page.header.reserved = LEAF_INLINE_DATA_FLAG;
        self.write_page(&root_page)?;

        let mut current_root_id = root_page_id;

        for (table_name, table) in &db.tables {
            let schema_key = Value::Text(format!("schema:{}", table_name));
            let schema_json = serde_json::to_string(&TableStorageRecord {
                columns: table.columns.clone(),
                constraints: table.constraints.clone(),
                next_row_id: table.next_row_id,
            })
            .map_err(|e| format!("Failed to serialize schema for table {}: {}", table_name, e))?;
            current_root_id = self.insert_entry(
                BTreeEntry::with_inline_data(schema_key, schema_json),
                current_root_id,
            )?;
        }

        for (table_name, table) in &db.tables {
            for (row_id, row) in table.iter_rows_with_ids() {
                let row_key = Value::Text(format_row_storage_key(table_name, row_id));
                let row_json = serde_json::to_string(row).map_err(|e| {
                    format!(
                        "Failed to serialize row {} for table {}: {}",
                        row_id.0, table_name, e
                    )
                })?;
                current_root_id = self.insert_entry(
                    BTreeEntry::with_inline_data(row_key, row_json),
                    current_root_id,
                )?;
            }
        }

        for (index_name, index) in &db.indexes {
            let index_key = Value::Text(format!("index:{}", index_name));
            let index_json = serde_json::to_string(index)
                .map_err(|e| format!("Failed to serialize index {}: {}", index_name, e))?;
            current_root_id = self.insert_entry(
                BTreeEntry::with_inline_data(index_key, index_json),
                current_root_id,
            )?;
        }

        for (index_name, index) in &db.composite_indexes {
            let index_key = Value::Text(format!("cindex:{}", index_name));
            let index_json = serde_json::to_string(index).map_err(|e| {
                format!("Failed to serialize composite index {}: {}", index_name, e)
            })?;
            current_root_id = self.insert_entry(
                BTreeEntry::with_inline_data(index_key, index_json),
                current_root_id,
            )?;
        }

        for (view_name, view) in &db.views {
            let view_key = Value::Text(format!("view:{}", view_name));
            let view_json = serde_json::to_string(view)
                .map_err(|e| format!("Failed to serialize view {}: {}", view_name, e))?;
            current_root_id = self.insert_entry(
                BTreeEntry::with_inline_data(view_key, view_json),
                current_root_id,
            )?;
        }

        let mut meta_page = self.read_page(0)?;
        if let Some(root_entry) = meta_page
            .entries
            .iter_mut()
            .find(|e| matches!(&e.key, Value::Text(value) if value == "root"))
        {
            root_entry.pointer = current_root_id;
        }
        meta_page.header.entry_count = meta_page.entries.len() as u16;
        self.write_page(&meta_page)?;

        self.file.flush().map_err(|e| {
            RustqlError::StorageError(format!("Failed to flush BTree storage file: {}", e))
        })
    }
}

impl BTreeFile {
    pub(super) fn read_page(&mut self, page_id: u64) -> Result<BTreePage, RustqlError> {
        let header_size = 16u64;
        let offset = header_size + page_id * BTREE_PAGE_SIZE as u64;

        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            RustqlError::StorageError(format!("Failed to seek to BTree page: {}", e))
        })?;

        let mut buf = [0u8; BTREE_PAGE_SIZE];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| RustqlError::StorageError(format!("Failed to read BTree page: {}", e)))?;

        BTreePage::from_bytes(&buf)
    }

    pub(super) fn write_page(&mut self, page: &BTreePage) -> Result<(), RustqlError> {
        let header_size = 16u64;
        let offset = header_size + page.header.page_id * BTREE_PAGE_SIZE as u64;

        let buf = page.to_bytes()?;

        self.file.seek(SeekFrom::Start(offset)).map_err(|e| {
            RustqlError::StorageError(format!("Failed to seek to BTree page for write: {}", e))
        })?;

        self.file
            .write_all(&buf)
            .map_err(|e| RustqlError::StorageError(format!("Failed to write BTree page: {}", e)))?;

        self.file
            .flush()
            .map_err(|e| RustqlError::StorageError(format!("Failed to flush BTree page: {}", e)))
    }

    fn get_next_page_id(&mut self) -> Result<u64, RustqlError> {
        let meta_page = self.read_page(0)?;

        for entry in &meta_page.entries {
            if let Value::Text(ref s) = entry.key
                && s == "next_page_id"
            {
                let next_id = entry.pointer;
                let mut new_meta = meta_page.clone();
                if let Some(e) = new_meta.entries.iter_mut().find(|e| {
                    if let Value::Text(ref s) = e.key {
                        s == "next_page_id"
                    } else {
                        false
                    }
                }) {
                    e.pointer = next_id + 1;
                } else {
                    new_meta.entries.push(BTreeEntry::new(
                        Value::Text("next_page_id".to_string()),
                        next_id + 1,
                    ));
                }
                new_meta.header.entry_count = new_meta.entries.len() as u16;
                self.write_page(&new_meta)?;
                return Ok(next_id);
            }
        }

        let next_id = 2;
        let mut new_meta = meta_page;
        new_meta.entries.push(BTreeEntry::new(
            Value::Text("next_page_id".to_string()),
            next_id + 1,
        ));
        new_meta.header.entry_count = new_meta.entries.len() as u16;
        self.write_page(&new_meta)?;
        Ok(next_id)
    }

    #[cfg(test)]
    pub fn search(
        &mut self,
        key: &Value,
        root_page_id: u64,
    ) -> Result<Option<(u64, usize)>, RustqlError> {
        let mut current_page_id = root_page_id;

        loop {
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    for (idx, entry) in page.entries.iter().enumerate() {
                        if &entry.key == key {
                            return Ok(Some((current_page_id, idx)));
                        }
                    }
                    return Ok(None);
                }
                PageKind::Internal => {
                    let mut found = false;
                    for entry in &page.entries {
                        if &entry.key > key {
                            current_page_id = entry.pointer;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        if let Some(last_entry) = page.entries.last() {
                            current_page_id = last_entry.pointer;
                        } else {
                            return Ok(None);
                        }
                    }
                }
                PageKind::Meta => {
                    return Err(RustqlError::StorageError(
                        "Cannot search in meta page".to_string(),
                    ));
                }
            }
        }
    }

    fn find_path_to_leaf(
        &mut self,
        key: &Value,
        root_page_id: u64,
    ) -> Result<Vec<u64>, RustqlError> {
        let mut path = Vec::new();
        let mut current_page_id = root_page_id;

        loop {
            path.push(current_page_id);
            let page = self.read_page(current_page_id)?;

            match page.header.kind {
                PageKind::Leaf => {
                    return Ok(path);
                }
                PageKind::Internal => {
                    let mut next_page_id = None;
                    for entry in &page.entries {
                        if &entry.key > key {
                            next_page_id = Some(entry.pointer);
                            break;
                        }
                    }
                    current_page_id = next_page_id
                        .or_else(|| page.entries.last().map(|e| e.pointer))
                        .ok_or_else(|| "Invalid internal page structure".to_string())?;
                }
                PageKind::Meta => {
                    return Err(RustqlError::StorageError(
                        "Cannot traverse meta page for insert".to_string(),
                    ));
                }
            }
        }
    }

    #[cfg(test)]
    pub fn insert(
        &mut self,
        key: Value,
        data_pointer: u64,
        root_page_id: u64,
    ) -> Result<u64, RustqlError> {
        self.insert_entry(BTreeEntry::new(key, data_pointer), root_page_id)
    }

    fn insert_entry(
        &mut self,
        new_entry: BTreeEntry,
        root_page_id: u64,
    ) -> Result<u64, RustqlError> {
        let path = self.find_path_to_leaf(&new_entry.key, root_page_id)?;
        let leaf_page_id = path.last().copied().ok_or_else(|| {
            RustqlError::StorageError("BTree insert could not locate a leaf page".to_string())
        })?;
        let mut leaf_page = self.read_page(leaf_page_id)?;

        for (idx, entry) in leaf_page.entries.iter().enumerate() {
            if entry.key == new_entry.key {
                leaf_page.entries[idx] = new_entry;
                self.write_page(&leaf_page)?;
                return Ok(root_page_id);
            }
        }

        if leaf_page.can_accept_entry(&new_entry) {
            let insert_pos = leaf_page
                .entries
                .binary_search_by(|e| e.key.cmp(&new_entry.key))
                .unwrap_or_else(|pos| pos);
            leaf_page.entries.insert(insert_pos, new_entry);
            leaf_page.header.entry_count = leaf_page.entries.len() as u16;
            self.write_page(&leaf_page)?;
            Ok(root_page_id)
        } else {
            self.split_and_insert_recursive(leaf_page, new_entry, path, root_page_id)
        }
    }

    fn split_and_insert_recursive(
        &mut self,
        mut page: BTreePage,
        new_entry: BTreeEntry,
        mut path: Vec<u64>,
        root_page_id: u64,
    ) -> Result<u64, RustqlError> {
        let insert_pos = page
            .entries
            .binary_search_by(|e| e.key.cmp(&new_entry.key))
            .unwrap_or_else(|pos| pos);
        page.entries.insert(insert_pos, new_entry);

        let mid = page.entries.len() / 2;
        let right_entries = page.entries.split_off(mid);
        let split_key = right_entries[0].key.clone();

        page.header.entry_count = page.entries.len() as u16;
        self.write_page(&page)?;

        let right_page_id = self.get_next_page_id()?;
        let mut right_page = BTreePage::new(right_page_id, page.header.kind);
        right_page.header.reserved = page.header.reserved;
        right_page.entries = right_entries;
        right_page.header.entry_count = right_page.entries.len() as u16;
        self.write_page(&right_page)?;

        if page.header.page_id == root_page_id {
            let new_root_id = self.get_next_page_id()?;
            let mut new_root = BTreePage::new(new_root_id, PageKind::Internal);

            let left_key = page.entries[0].key.clone();

            new_root
                .entries
                .push(BTreeEntry::new(left_key, page.header.page_id));
            new_root
                .entries
                .push(BTreeEntry::new(split_key, right_page_id));
            new_root.header.entry_count = new_root.entries.len() as u16;
            self.write_page(&new_root)?;

            let mut meta_page = self.read_page(0)?;
            if let Some(root_entry) = meta_page.entries.iter_mut().find(|e| {
                if let Value::Text(ref s) = e.key {
                    s == "root"
                } else {
                    false
                }
            }) {
                root_entry.pointer = new_root_id;
            } else {
                meta_page.entries.push(BTreeEntry::new(
                    Value::Text("root".to_string()),
                    new_root_id,
                ));
            }
            meta_page.header.entry_count = meta_page.entries.len() as u16;
            self.write_page(&meta_page)?;

            return Ok(new_root_id);
        }

        path.pop();
        if let Some(parent_page_id) = path.last().copied() {
            let mut parent_page = self.read_page(parent_page_id)?;

            let parent_entry = BTreeEntry::new(split_key, right_page_id);

            if parent_page.can_accept_entry(&parent_entry) {
                let insert_pos = parent_page
                    .entries
                    .binary_search_by(|e| e.key.cmp(&parent_entry.key))
                    .unwrap_or_else(|pos| pos);
                parent_page.entries.insert(insert_pos, parent_entry);
                parent_page.header.entry_count = parent_page.entries.len() as u16;
                self.write_page(&parent_page)?;
                Ok(root_page_id)
            } else {
                self.split_and_insert_recursive(parent_page, parent_entry, path, root_page_id)
            }
        } else {
            Ok(root_page_id)
        }
    }

    #[cfg(test)]
    pub fn delete(&mut self, key: &Value, root_page_id: u64) -> Result<bool, RustqlError> {
        match self.search(key, root_page_id)? {
            Some((page_id, entry_idx)) => {
                let mut page = self.read_page(page_id)?;
                page.entries.remove(entry_idx);
                page.header.entry_count = page.entries.len() as u16;
                self.write_page(&page)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[cfg(test)]
    pub fn range_scan(
        &mut self,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
        root_page_id: u64,
    ) -> Result<Vec<(Value, u64)>, RustqlError> {
        scan_pages_in_order_entries(
            |page_id| self.read_page(page_id),
            start_key,
            end_key,
            root_page_id,
        )
        .map(|entries| {
            entries
                .into_iter()
                .map(|entry| (entry.key, entry.pointer))
                .collect()
        })
    }
}

fn scan_pages_in_order_entries<F>(
    mut read_page: F,
    start_key: Option<&Value>,
    end_key: Option<&Value>,
    root_page_id: u64,
) -> Result<Vec<BTreeEntry>, RustqlError>
where
    F: FnMut(u64) -> Result<BTreePage, RustqlError>,
{
    let mut entries = Vec::new();
    scan_page_in_order_recursive_entries(
        &mut read_page,
        root_page_id,
        start_key,
        end_key,
        &mut entries,
    )?;
    Ok(entries)
}

fn scan_page_in_order_recursive_entries<F>(
    read_page: &mut F,
    page_id: u64,
    start_key: Option<&Value>,
    end_key: Option<&Value>,
    entries: &mut Vec<BTreeEntry>,
) -> Result<(), RustqlError>
where
    F: FnMut(u64) -> Result<BTreePage, RustqlError>,
{
    let page = read_page(page_id)?;

    match page.header.kind {
        PageKind::Leaf => {
            for entry in page.entries {
                if start_key.is_some_and(|start| &entry.key < start) {
                    continue;
                }
                if end_key.is_some_and(|end| &entry.key > end) {
                    break;
                }
                entries.push(entry);
            }
            Ok(())
        }
        PageKind::Internal => {
            for (idx, entry) in page.entries.iter().enumerate() {
                if end_key.is_some_and(|end| &entry.key > end) {
                    break;
                }

                let next_min = page.entries.get(idx + 1).map(|next| &next.key);
                if start_key.is_some_and(|start| next_min.is_some_and(|min| min <= start)) {
                    continue;
                }

                scan_page_in_order_recursive_entries(
                    read_page,
                    entry.pointer,
                    start_key,
                    end_key,
                    entries,
                )?;
            }
            Ok(())
        }
        PageKind::Meta => Err(RustqlError::StorageError(
            "Cannot scan entries from meta page".to_string(),
        )),
    }
}
