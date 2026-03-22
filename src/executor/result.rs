//! Query results for rustdb

use crate::common::{Error, Result};

// TODO: Implement query results
pub struct ResultSet {
    // TODO: Implement structure
}

impl ResultSet {
    pub fn new() -> Result<Self> {
        // TODO: Implement initialization
        Ok(Self {})
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
