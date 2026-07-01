use crate::ast::{ColumnDefinition, TableConstraint, Value};
use crate::database::RowId;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

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
    let mut key =
        String::with_capacity(ROW_KEY_PREFIX.len() + table_name.len() + 1 + ROW_ID_KEY_WIDTH);
    key.push_str(ROW_KEY_PREFIX);
    key.push_str(table_name);
    key.push(':');
    write!(&mut key, "{:0width$}", row_id.0, width = ROW_ID_KEY_WIDTH)
        .expect("writing to String cannot fail");
    key
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_row_storage_key_uses_fixed_width_row_id() {
        let key = format_row_storage_key("users", RowId(42));

        assert_eq!(key, "table_row:users:00000000000000000042");
        assert_eq!(
            parse_row_storage_key(&key),
            Some(("users", RowId(42), true))
        );
    }

    #[test]
    fn parse_row_storage_key_preserves_legacy_flag() {
        assert_eq!(
            parse_row_storage_key("row:users:7"),
            Some(("users", RowId(7), false))
        );
    }

    #[test]
    fn parse_row_storage_key_rejects_invalid_row_id() {
        assert_eq!(parse_row_storage_key("table_row:users:not-a-row-id"), None);
    }
}
