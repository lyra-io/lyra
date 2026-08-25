//! CRC32C calculation for physical WAL record fragments.

const MASK_DELTA: u32 = 0xa282_ead8;

pub(super) fn physical_checksum(record_type: u8, segment_number: u32, payload: &[u8]) -> u32 {
    let checksum = crc32c::crc32c(&[record_type]);
    let checksum = crc32c::crc32c_append(checksum, &segment_number.to_le_bytes());
    let checksum = crc32c::crc32c_append(checksum, payload);
    mask(checksum)
}

fn mask(checksum: u32) -> u32 {
    checksum.rotate_right(15).wrapping_add(MASK_DELTA)
}
