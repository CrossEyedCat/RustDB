//! Хранилище данных rustdb

pub mod advanced_file_manager;
pub mod block;
pub mod database_file;
pub mod file_manager;
pub mod index;
pub mod io_optimization;
pub mod optimized_file_manager;
pub mod page;
pub mod page_manager;
pub mod row;
pub mod schema_manager;
pub mod tuple;

#[cfg(test)]
pub mod tests;

// TODO: Реализовать компоненты хранения данных
