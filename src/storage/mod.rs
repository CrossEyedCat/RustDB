//! Data storage for rustdb

pub mod advanced_file_manager;
pub mod block;
pub mod block_io;
pub mod cached_file_manager;
pub mod database_file;
pub mod file_manager;
pub mod index;
pub mod index_registry;
pub mod io_optimization;
pub mod optimized_file_manager;
pub mod page;
pub mod page_manager;
pub mod row;
pub mod schema_manager;
pub mod tuple;

#[cfg(test)]
pub mod tests;

// Page store, file managers, indexes, and tuple layer — modules above.
