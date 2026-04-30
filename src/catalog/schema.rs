//! Schema manager for rustdb

use crate::common::types::Column;
use crate::common::{Error, Result};
use crate::parser::ast::Expression;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: String,
    pub expr: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueConstraintDef {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyConstraintDef {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    /// When empty, resolved at runtime to the referenced table's primary key columns.
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<Column>,
    /// `(constraint_name, column list)` — at most one primary key.
    pub primary_key: Option<(String, Vec<String>)>,
    pub unique_constraints: Vec<UniqueConstraintDef>,
    pub foreign_keys: Vec<ForeignKeyConstraintDef>,
    pub check_constraints: Vec<CheckConstraint>,
}

/// Registered table names and simple ordinal ids (for tests and tooling).
#[derive(Debug, Clone, Default)]
pub struct SchemaManager {
    table_ids: HashMap<String, u32>,
    next_id: u32,
    schemas: HashMap<String, TableSchema>,
}

impl SchemaManager {
    pub fn new() -> Result<Self> {
        Ok(Self {
            table_ids: HashMap::new(),
            next_id: 1,
            schemas: HashMap::new(),
        })
    }

    pub fn register_table(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.table_ids.get(name) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.table_ids.insert(name.to_string(), id);
        id
    }

    pub fn table_id(&self, name: &str) -> Option<u32> {
        self.table_ids.get(name).copied()
    }

    pub fn register_schema(&mut self, schema: TableSchema) -> u32 {
        let id = self.register_table(&schema.table_name);
        self.schemas.insert(schema.table_name.clone(), schema);
        id
    }

    pub fn schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }

    pub fn schema_mut(&mut self, table: &str) -> Option<&mut TableSchema> {
        self.schemas.get_mut(table)
    }

    /// Sorted list of registered table names (for dependency ordering, tests, etc.).
    pub fn table_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.schemas.keys().cloned().collect();
        v.sort();
        v
    }

    /// Tables that declare a foreign key referencing `parent_table`.
    pub fn tables_with_fk_to(&self, parent_table: &str) -> Vec<String> {
        let mut out = Vec::new();
        for (tname, sch) in &self.schemas {
            if sch
                .foreign_keys
                .iter()
                .any(|fk| fk.referenced_table == parent_table)
            {
                out.push(tname.clone());
            }
        }
        out.sort();
        out
    }

    pub fn drop_table(&mut self, table: &str) {
        self.table_ids.remove(table);
        self.schemas.remove(table);
    }

    /// Rename a registered table (updates [`TableSchema::table_name`] and foreign keys that reference the old name).
    pub fn rename_table(&mut self, old: &str, new: &str) -> Result<()> {
        if old == new {
            return Ok(());
        }
        if self.schemas.contains_key(new) {
            return Err(Error::validation(format!(
                "cannot rename table {old}: name `{new}` already exists"
            )));
        }
        let mut sch = self
            .schemas
            .remove(old)
            .ok_or_else(|| Error::validation(format!("table {old} does not exist")))?;
        let id = self
            .table_ids
            .remove(old)
            .ok_or_else(|| Error::validation(format!("internal: missing table_id for {old}")))?;
        sch.table_name = new.to_string();
        self.table_ids.insert(new.to_string(), id);
        self.schemas.insert(new.to_string(), sch);
        for (_t, s) in self.schemas.iter_mut() {
            for fk in s.foreign_keys.iter_mut() {
                if fk.referenced_table == old {
                    fk.referenced_table = new.to_string();
                }
            }
        }
        Ok(())
    }

    /// Persist registered schemas under `data_dir/.rustdb/catalog.json` (v1 JSON snapshot).
    pub fn save_catalog_to_data_dir(&self, data_dir: &Path) -> Result<()> {
        let dir = data_dir.join(".rustdb");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("catalog.json");
        let tmp_path = dir.join("catalog.json.tmp");
        let file = self.build_catalog_file()?;
        let json = serde_json::to_string_pretty(&file).map_err(Error::from)?;
        // Atomic-ish update: write to a temp file, fsync it (best-effort), then rename.
        // This reduces the chance of leaving a truncated `catalog.json` on crash/power loss.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            f.write_all(json.as_bytes())?;
            // If requested, make the file contents durable before renaming.
            if matches!(std::env::var("RUSTDB_FSYNC_COMMIT").as_deref(), Ok("1")) {
                let _ = f.sync_all();
            }
        }
        std::fs::rename(&tmp_path, &path)?;
        // Best-effort directory fsync on Unix so the rename is durable too.
        #[cfg(unix)]
        if matches!(std::env::var("RUSTDB_FSYNC_COMMIT").as_deref(), Ok("1")) {
            if let Ok(d) = std::fs::File::open(&dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    /// Load a v1 catalog snapshot if `catalog.json` exists; otherwise `Ok(None)`.
    pub fn try_load_catalog_from_data_dir(data_dir: &Path) -> Result<Option<Self>> {
        let path = data_dir.join(".rustdb").join("catalog.json");
        if !path.is_file() {
            return Ok(None);
        }
        let s = std::fs::read_to_string(&path).map_err(Error::from)?;
        let file: SchemaCatalogFile = serde_json::from_str(&s).map_err(Error::from)?;
        if file.version != 1 {
            return Err(Error::database(format!(
                "unsupported catalog.json version {}",
                file.version
            )));
        }
        let mut table_ids = HashMap::new();
        let mut schemas = HashMap::new();
        for t in file.tables {
            table_ids.insert(t.name.clone(), t.table_id);
            schemas.insert(t.name, t.schema);
        }
        Ok(Some(Self {
            table_ids,
            next_id: file.next_id,
            schemas,
        }))
    }

    fn build_catalog_file(&self) -> Result<SchemaCatalogFile> {
        let mut tables: Vec<SchemaCatalogTable> = self
            .schemas
            .iter()
            .map(|(name, sch)| SchemaCatalogTable {
                name: name.clone(),
                table_id: self.table_ids.get(name).copied().unwrap_or(0),
                schema: sch.clone(),
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalogFile {
            version: 1,
            next_id: self.next_id,
            tables,
        })
    }
}

/// On-disk catalog snapshot (v1) for [`SchemaManager`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaCatalogFile {
    version: u32,
    next_id: u32,
    tables: Vec<SchemaCatalogTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaCatalogTable {
    name: String,
    table_id: u32,
    schema: TableSchema,
}
