use crate::ast::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    pub indexes: HashMap<String, Index>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub column: String,
    #[serde(with = "index_entries")]
    pub entries: BTreeMap<Value, Vec<usize>>,
}

mod index_entries {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        entries: &BTreeMap<Value, Vec<usize>>,
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

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<Value, Vec<usize>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: serde_json::Map<String, serde_json::Value> =
            serde_json::Map::deserialize(deserializer)?;
        let mut entries = BTreeMap::new();
        for (key_str, value) in map {
            let key = string_to_value(&key_str);
            let indices: Vec<usize> = serde_json::from_value(value).unwrap();
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
        if let Some(rest) = s.strip_prefix("I:") {
            if let Ok(i) = rest.parse::<i64>() {
                return Value::Integer(i);
            }
        }
        if let Some(rest) = s.strip_prefix("F:") {
            if let Ok(f) = rest.parse::<f64>() {
                return Value::Float(f);
            }
        }
        if let Some(rest) = s.strip_prefix("S:") {
            return Value::Text(rest.to_string());
        }
        if let Some(rest) = s.strip_prefix("B:") {
            if let Ok(b) = rest.parse::<bool>() {
                return Value::Boolean(b);
            }
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
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load() -> Self {
        // Delegate persistence to the configured storage engine.
        crate::storage::storage_engine().load()
    }

    pub fn save(&self) -> Result<(), String> {
        // Delegate persistence to the configured storage engine.
        crate::storage::storage_engine().save(self)
    }
}
