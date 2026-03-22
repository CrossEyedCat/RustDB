//! Bincode helpers using the **legacy** wire format (compatible with bincode 1.x on-disk data).
//!
//! Crates.io lists a `bincode` 3.0.0 that is not a usable library; this project uses **bincode 2**
//! with [`bincode::config::legacy()`] for serde-backed persistence.

use bincode::config::legacy;

/// Serialize with bincode 1–compatible wire format.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, legacy())
}

/// Deserialize with bincode 1–compatible wire format.
pub fn deserialize<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    let (value, _) = bincode::serde::decode_from_slice(bytes, legacy())?;
    Ok(value)
}

/// Deserialize from a reader (e.g. [`std::io::Cursor`] over `&[u8]`).
pub fn deserialize_from_reader<T: serde::de::DeserializeOwned, R: std::io::Read>(
    reader: &mut R,
) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_std_read(reader, legacy())
}
