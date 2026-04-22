use crate::ast::{ColumnDefinition, TableConstraint, Value};
use crate::database::RowId;
use serde::{Deserialize, Serialize};

pub(super) const LEGACY_ROW_KEY_PREFIX: &str = "row:";
pub(super) const ROW_KEY_PREFIX: &str = "table_row:";
pub(super) const ROW_ID_KEY_WIDTH: usize = 20;

#[derive(Serialize, Deserialize)]
pub(super) struct TableStorageRecord {
    pub(super) columns: Vec<ColumnDefinition>,
    pub(super) constraints: Vec<TableConstraint>,
    pub(super) next_row_id: u64,
}

pub(super) fn format_row_storage_key(table_name: &str, row_id: RowId) -> String {
    format!(
        "{ROW_KEY_PREFIX}{table_name}:{:0width$}",
        row_id.0,
        width = ROW_ID_KEY_WIDTH
    )
}

pub(super) fn parse_row_storage_key(key: &str) -> Option<(&str, RowId, bool)> {
    if let Some(row_key) = key.strip_prefix(ROW_KEY_PREFIX) {
        let (table_name, row_id_str) = row_key.rsplit_once(':')?;
        let row_id = row_id_str.parse::<u64>().ok()?;
        return Some((table_name, RowId(row_id), true));
    }

    let row_key = key.strip_prefix(LEGACY_ROW_KEY_PREFIX)?;
    let (table_name, row_id_str) = row_key.rsplit_once(':')?;
    let row_id = row_id_str.parse::<u64>().ok()?;
    Some((table_name, RowId(row_id), false))
}

pub(super) fn insert_loaded_row(
    table: &mut crate::database::Table,
    row_id: RowId,
    row: Vec<Value>,
) {
    if table
        .row_ids
        .last()
        .is_none_or(|last_row_id| *last_row_id < row_id)
    {
        table.row_ids.push(row_id);
        table.rows.push(row);
    } else {
        let position = table
            .row_ids
            .binary_search(&row_id)
            .unwrap_or_else(|pos| pos);
        table.row_ids.insert(position, row_id);
        table.rows.insert(position, row);
    }

    if table.next_row_id <= row_id.0 {
        table.next_row_id = row_id.0 + 1;
    }
}
