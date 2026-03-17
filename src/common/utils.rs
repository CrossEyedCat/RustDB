//! Utilities for rustdb

use crate::common::constants::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Calculates a hash for the given value
pub fn calculate_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Checks if the page size is supported
pub fn is_valid_page_size(size: usize) -> bool {
    SUPPORTED_PAGE_SIZES.contains(&size)
}

/// Checks if the table name is valid
pub fn is_valid_table_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_TABLE_NAME_LENGTH {
        return false;
    }

    // Check that the name starts with a letter or underscore
    if !name.chars().next().unwrap().is_alphabetic() && !name.starts_with('_') {
        return false;
    }

    // Check that the name contains only letters, digits, and underscores
    name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Checks if the column name is valid
pub fn is_valid_column_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_COLUMN_NAME_LENGTH {
        return false;
    }

    // Check that the name starts with a letter or underscore
    if !name.chars().next().unwrap().is_alphabetic() && !name.starts_with('_') {
        return false;
    }

    // Check that the name contains only letters, digits, and underscores
    name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Checks if the index name is valid
pub fn is_valid_index_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_INDEX_NAME_LENGTH {
        return false;
    }

    // Check that the name starts with a letter or underscore
    if !name.chars().next().unwrap().is_alphabetic() && !name.starts_with('_') {
        return false;
    }

    // Check that the name contains only letters, digits, and underscores
    name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Calculates the page header size based on the page size
pub fn calculate_page_header_size(page_size: usize) -> usize {
    (page_size as f64 * PAGE_HEADER_PERCENTAGE) as usize
}

/// Calculates the maximum record size on a page
pub fn calculate_max_record_size(page_size: usize) -> usize {
    page_size - calculate_page_header_size(page_size)
}

/// Checks if a record can fit on a page
pub fn can_fit_record_on_page(record_size: usize, page_size: usize) -> bool {
    record_size <= calculate_max_record_size(page_size)
}

/// Calculates the number of pages needed to store data
pub fn calculate_pages_needed(data_size: usize, page_size: usize) -> usize {
    let max_record_size = calculate_max_record_size(page_size);
    if max_record_size == 0 {
        return 0;
    }

    data_size.div_ceil(max_record_size)
}

/// Calculates the optimal page size for the given data size
pub fn calculate_optimal_page_size(record_size: usize) -> usize {
    for &page_size in SUPPORTED_PAGE_SIZES {
        if can_fit_record_on_page(record_size, page_size) {
            return page_size;
        }
    }

    // If we can't find an optimal size, return the maximum
    MAX_PAGE_SIZE
}

/// Checks if a page should be split
pub fn should_split_page(used_space: usize, page_size: usize) -> bool {
    let threshold = (page_size as f64 * PAGE_SPLIT_THRESHOLD) as usize;
    used_space >= threshold
}

/// Checks if pages should be merged
pub fn should_merge_pages(used_space: usize, page_size: usize) -> bool {
    let threshold = (page_size as f64 * PAGE_MERGE_THRESHOLD) as usize;
    used_space <= threshold
}

/// Calculates the optimal B+ tree order for the given key size
pub fn calculate_optimal_btree_order(key_size: usize) -> usize {
    let page_size = DEFAULT_PAGE_SIZE;
    let header_size = calculate_page_header_size(page_size);
    let available_space = page_size - header_size;

    // Each node contains keys and pointers
    // Simplified formula: (available_space - sizeof(pointer)) / (key_size + sizeof(pointer))
    let pointer_size = std::mem::size_of::<usize>();
    let order = (available_space - pointer_size) / (key_size + pointer_size);

    // Clamp order to valid values
    order.clamp(MIN_BTREE_ORDER, MAX_BTREE_ORDER)
}

/// Calculates the optimal hash table size for the given number of elements
pub fn calculate_optimal_hash_table_size(element_count: usize) -> usize {
    let load_factor = DEFAULT_HASH_LOAD_FACTOR;
    let optimal_size = (element_count as f64 / load_factor) as usize;

    // Round to nearest power of 2
    let mut size = 1;
    while size < optimal_size {
        size *= 2;
    }

    // Clamp size to valid values
    size.clamp(DEFAULT_HASH_TABLE_SIZE, MAX_HASH_TABLE_SIZE)
}

/// Checks if a hash table should be expanded
pub fn should_expand_hash_table(current_size: usize, element_count: usize) -> bool {
    let load_factor = element_count as f64 / current_size as f64;
    load_factor >= MAX_HASH_LOAD_FACTOR
}

/// Checks if a hash table should be shrunk
pub fn should_shrink_hash_table(current_size: usize, element_count: usize) -> bool {
    let load_factor = element_count as f64 / current_size as f64;
    load_factor <= MIN_HASH_LOAD_FACTOR
}

/// Formats bytes size into a readable format
pub fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

/// Formats time in seconds into a readable format
pub fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let remaining_seconds = seconds % 60;
        if remaining_seconds == 0 {
            format!("{}m", minutes)
        } else {
            format!("{}m {}s", minutes, remaining_seconds)
        }
    } else {
        let hours = seconds / 3600;
        let remaining_minutes = (seconds % 3600) / 60;
        if remaining_minutes == 0 {
            format!("{}h", hours)
        } else {
            format!("{}h {}m", hours, remaining_minutes)
        }
    }
}

/// Checks if a number is a power of 2
pub fn is_power_of_two(n: usize) -> bool {
    n > 0 && (n & (n - 1)) == 0
}

/// Calculates the nearest power of 2 greater than or equal to the given number
pub fn next_power_of_two(n: usize) -> usize {
    if n == 0 {
        return 1;
    }

    let mut power = 1;
    while power < n {
        power *= 2;
    }
    power
}

/// Calculates the nearest power of 2 less than or equal to the given number
pub fn prev_power_of_two(n: usize) -> usize {
    if n == 0 {
        return 0;
    }

    let mut power = 1;
    while power * 2 <= n {
        power *= 2;
    }
    power
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_table_name() {
        assert!(is_valid_table_name("users"));
        assert!(is_valid_table_name("user_profiles"));
        assert!(is_valid_table_name("_temp"));
        assert!(!is_valid_table_name(""));
        assert!(!is_valid_table_name("123table"));
        assert!(!is_valid_table_name("table-name"));
    }

    #[test]
    fn test_is_valid_column_name() {
        assert!(is_valid_column_name("id"));
        assert!(is_valid_column_name("user_name"));
        assert!(is_valid_column_name("_hidden"));
        assert!(!is_valid_column_name(""));
        assert!(!is_valid_column_name("123column"));
        assert!(!is_valid_column_name("column-name"));
    }

    #[test]
    fn test_calculate_page_header_size() {
        assert_eq!(calculate_page_header_size(4096), 409);
        assert_eq!(calculate_page_header_size(8192), 819);
    }

    #[test]
    fn test_calculate_max_record_size() {
        assert_eq!(calculate_max_record_size(4096), 3687);
        assert_eq!(calculate_max_record_size(8192), 7373);
    }

    #[test]
    fn test_can_fit_record_on_page() {
        assert!(can_fit_record_on_page(1000, 4096));
        assert!(!can_fit_record_on_page(5000, 4096));
    }

    #[test]
    fn test_is_power_of_two() {
        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(is_power_of_two(4));
        assert!(is_power_of_two(8));
        assert!(!is_power_of_two(3));
        assert!(!is_power_of_two(5));
        assert!(!is_power_of_two(0));
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(next_power_of_two(0), 1);
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);
        assert_eq!(next_power_of_two(8), 8);
    }

    #[test]
    fn test_prev_power_of_two() {
        assert_eq!(prev_power_of_two(0), 0);
        assert_eq!(prev_power_of_two(1), 1);
        assert_eq!(prev_power_of_two(2), 2);
        assert_eq!(prev_power_of_two(3), 2);
        assert_eq!(prev_power_of_two(5), 4);
        assert_eq!(prev_power_of_two(8), 8);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(512), "512 B");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
    }
}
