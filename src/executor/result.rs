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

    #[test]
    fn test_result_set_with_columns_and_rows() -> Result<()> {
        let mut rs = ResultSet::with_columns(vec!["a".into(), "b".into()])?;
        assert_eq!(rs.column_names.len(), 2);
        rs.push_row(vec!["1".into(), "2".into()]);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], "2");
        Ok(())
    }

    #[test]
    fn test_result_set_default() {
        let rs = ResultSet::default();
        assert!(rs.column_names.is_empty());
        assert!(rs.rows.is_empty());
    }
}
