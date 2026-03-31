//! Query results for rustdb

use crate::common::Result;

/// Tabular query result (column names and stringified row values).
#[derive(Debug, Clone, Default)]
pub struct ResultSet {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl ResultSet {
    pub fn new() -> Result<Self> {
        Ok(Self {
            column_names: Vec::new(),
            rows: Vec::new(),
        })
    }

    pub fn with_columns(columns: Vec<String>) -> Result<Self> {
        Ok(Self {
            column_names: columns,
            rows: Vec::new(),
        })
    }

    pub fn push_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }
}

#[cfg(test)]
mod tests {
    use super::ResultSet;
    use crate::common::Result;

    #[test]
    fn test_result_set_new() -> Result<()> {
        let _rs = ResultSet::new()?;
        Ok(())
    }
}
