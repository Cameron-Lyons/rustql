use crate::ast::*;
use crate::error::RustqlError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    pub indexes: HashMap<String, Index>,
    #[serde(default)]
    pub views: HashMap<String, View>,
    #[serde(default)]
    pub composite_indexes: HashMap<String, CompositeIndex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowId(pub u64);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct View {
    pub name: String,
    pub query_sql: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub column: String,
    #[serde(with = "index_entries")]
    pub entries: BTreeMap<Value, Vec<RowId>>,
    #[serde(default, with = "optional_filter_expression")]
    pub filter_expr: Option<Expression>,
}

mod index_entries {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        entries: &BTreeMap<Value, Vec<RowId>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serde_json::Map::new();
        for (key, value) in entries {
            let key_str = value_to_string(key);
            map.insert(key_str, serde_json::to_value(value).unwrap());
        }
        map.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<Value, Vec<RowId>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: serde_json::Map<String, serde_json::Value> =
            serde_json::Map::deserialize(deserializer)?;
        let mut entries = BTreeMap::new();
        for (key_str, value) in map {
            let key = string_to_value(&key_str);
            let indices: Vec<RowId> = serde_json::from_value(value).unwrap();
            entries.insert(key, indices);
        }
        Ok(entries)
    }

    fn value_to_string(v: &Value) -> String {
        match v {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => format!("I:{}", i),
            Value::Float(f) => format!("F:{}", f),
            Value::Text(s) => format!("S:{}", s),
            Value::Boolean(b) => format!("B:{}", b),
            Value::Date(d) => format!("D:{}", d),
            Value::Time(t) => format!("TM:{}", t),
            Value::DateTime(dt) => format!("DT:{}", dt),
        }
    }

    fn string_to_value(s: &str) -> Value {
        if s == "NULL" {
            return Value::Null;
        }
        if let Some(rest) = s.strip_prefix("I:")
            && let Ok(i) = rest.parse::<i64>()
        {
            return Value::Integer(i);
        }
        if let Some(rest) = s.strip_prefix("F:")
            && let Ok(f) = rest.parse::<f64>()
        {
            return Value::Float(f);
        }
        if let Some(rest) = s.strip_prefix("S:") {
            return Value::Text(rest.to_string());
        }
        if let Some(rest) = s.strip_prefix("B:")
            && let Ok(b) = rest.parse::<bool>()
        {
            return Value::Boolean(b);
        }
        if let Some(rest) = s.strip_prefix("D:") {
            return Value::Date(rest.to_string());
        }
        if let Some(rest) = s.strip_prefix("TM:") {
            return Value::Time(rest.to_string());
        }
        if let Some(rest) = s.strip_prefix("DT:") {
            return Value::DateTime(rest.to_string());
        }
        Value::Text(s.to_string())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Table {
    pub columns: Vec<ColumnDefinition>,
    pub rows: Vec<Vec<Value>>,
    #[serde(default)]
    pub row_ids: Vec<RowId>,
    #[serde(default = "default_next_row_id")]
    pub next_row_id: u64,
    #[serde(default)]
    pub constraints: Vec<TableConstraint>,
}

fn default_next_row_id() -> u64 {
    1
}

impl Table {
    pub fn new(
        columns: Vec<ColumnDefinition>,
        rows: Vec<Vec<Value>>,
        constraints: Vec<TableConstraint>,
    ) -> Self {
        let mut table = Self {
            columns,
            rows,
            row_ids: Vec::new(),
            next_row_id: default_next_row_id(),
            constraints,
        };
        table.ensure_row_ids();
        table
    }

    pub fn ensure_row_ids(&mut self) {
        if self.row_ids.len() != self.rows.len() {
            self.row_ids = (1..=self.rows.len() as u64).map(RowId).collect();
        }

        let max_existing = self
            .row_ids
            .iter()
            .map(|row_id| row_id.0)
            .max()
            .unwrap_or(0);
        if self.next_row_id <= max_existing {
            self.next_row_id = max_existing + 1;
        }
        if self.next_row_id == 0 {
            self.next_row_id = default_next_row_id();
        }
    }

    pub fn iter_rows_with_ids(&self) -> impl Iterator<Item = (RowId, &Vec<Value>)> {
        self.row_ids.iter().copied().zip(self.rows.iter())
    }

    pub fn row_id_at(&self, position: usize) -> Option<RowId> {
        self.row_ids.get(position).copied()
    }

    pub fn position_of_row_id(&self, row_id: RowId) -> Option<usize> {
        self.row_ids
            .iter()
            .position(|candidate| *candidate == row_id)
    }

    pub fn row_by_id(&self, row_id: RowId) -> Option<&Vec<Value>> {
        self.position_of_row_id(row_id)
            .and_then(|position| self.rows.get(position))
    }

    pub fn row_mut_by_id(&mut self, row_id: RowId) -> Option<&mut Vec<Value>> {
        let position = self.position_of_row_id(row_id)?;
        self.rows.get_mut(position)
    }

    pub fn insert_row(&mut self, row: Vec<Value>) -> RowId {
        self.ensure_row_ids();
        let row_id = RowId(self.next_row_id);
        self.next_row_id += 1;
        self.rows.push(row);
        self.row_ids.push(row_id);
        row_id
    }

    pub fn insert_row_at(&mut self, position: usize, row_id: RowId, row: Vec<Value>) {
        self.ensure_row_ids();
        let position = position.min(self.rows.len());
        self.rows.insert(position, row);
        self.row_ids.insert(position, row_id);
        if self.next_row_id <= row_id.0 {
            self.next_row_id = row_id.0 + 1;
        }
    }

    pub fn remove_row_by_id(&mut self, row_id: RowId) -> Option<(usize, Vec<Value>)> {
        self.ensure_row_ids();
        let position = self.position_of_row_id(row_id)?;
        self.row_ids.remove(position);
        let row = self.rows.remove(position);
        Some((position, row))
    }

    pub fn set_row_by_id(&mut self, row_id: RowId, row: Vec<Value>) -> Option<()> {
        let position = self.position_of_row_id(row_id)?;
        self.rows[position] = row;
        Some(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompositeIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    #[serde(with = "composite_index_entries")]
    pub entries: BTreeMap<Vec<Value>, Vec<RowId>>,
    #[serde(default, with = "optional_filter_expression")]
    pub filter_expr: Option<Expression>,
}

mod optional_filter_expression {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum StoredFilter {
        Expression(Expression),
        LegacyDebugString(String),
    }

    pub fn serialize<S>(filter: &Option<Expression>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        filter.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Expression>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match Option::<StoredFilter>::deserialize(deserializer)? {
            Some(StoredFilter::Expression(expr)) => Some(expr),
            Some(StoredFilter::LegacyDebugString(_)) | None => None,
        })
    }
}

mod composite_index_entries {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        entries: &BTreeMap<Vec<Value>, Vec<RowId>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let converted: Vec<(Vec<Value>, Vec<RowId>)> = entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        converted.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<Vec<Value>, Vec<RowId>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let converted: Vec<(Vec<Value>, Vec<RowId>)> = Vec::deserialize(deserializer)?;
        Ok(converted.into_iter().collect())
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn normalize_row_ids(&mut self) {
        for table in self.tables.values_mut() {
            table.ensure_row_ids();
        }
    }

    pub fn load() -> Self {
        crate::storage::load_database()
    }

    pub fn save(&self) -> Result<(), RustqlError> {
        crate::storage::save_database(self)
    }
}
