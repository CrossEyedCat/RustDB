//! Хранилище данных RustBD

pub mod block;
pub mod page;
pub mod page_manager;
pub mod tuple;
pub mod row;
pub mod schema_manager;
pub mod file_manager;
pub mod database_file;
pub mod advanced_file_manager;
pub mod io_optimization;
pub mod optimized_file_manager;

#[cfg(test)]
pub mod tests;

// TODO: Реализовать компоненты хранения данных
