use std::io::{Error, ErrorKind};
use xxhash_rust::xxh32::xxh32;

const CRC_SIZE: usize = 4;
const SIZE_FIELD_SIZE: usize = 2;
const TYPE_SIZE: usize = 1;
const LOG_NUMBER_SIZE: usize = 4;
pub const RECORD_HEADER_SIZE: usize = CRC_SIZE + SIZE_FIELD_SIZE + TYPE_SIZE + LOG_NUMBER_SIZE;
pub const BLOCK_SIZE: usize = 32 * 1024;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl RecordType {
    fn from_u8(value: u8) -> Result<Self, Error> {
        match value {
            0 => Ok(RecordType::Zero),
            1 => Ok(RecordType::Full),
            2 => Ok(RecordType::First),
            3 => Ok(RecordType::Middle),
            4 => Ok(RecordType::Last),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid record type: {}", value),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Record {
    pub record_type: RecordType,
    pub log_number: u32,
    pub data: Vec<u8>,
}

impl Record {
    pub fn new(data: Vec<u8>) -> Self {
        Record {
            record_type: RecordType::Full,
            log_number: 0,
            data,
        }
    }

    pub fn new_with_type(record_type: RecordType, data: Vec<u8>) -> Self {
        Record {
            record_type,
            log_number: 0,
            data,
        }
    }

    pub fn new_with_log_number(record_type: RecordType, log_number: u32, data: Vec<u8>) -> Self {
        Record {
            record_type,
            log_number,
            data,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        if self.data.len() > u16::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Record data too large: {} bytes (max: {})",
                    self.data.len(),
                    u16::MAX
                ),
            ));
        }

        let size = self.data.len() as u16;
        let record_type = self.record_type as u8;

        let mut crc_data = Vec::with_capacity(1 + 4 + self.data.len());
        crc_data.push(record_type);
        crc_data.extend_from_slice(&self.log_number.to_le_bytes());
        crc_data.extend_from_slice(&self.data);
        let crc = xxh32(&crc_data, 0);

        let mut encoded = Vec::with_capacity(RECORD_HEADER_SIZE + self.data.len());
        encoded.extend_from_slice(&crc.to_le_bytes());
        encoded.extend_from_slice(&size.to_le_bytes());
        encoded.push(record_type);
        encoded.extend_from_slice(&self.log_number.to_le_bytes());
        encoded.extend_from_slice(&self.data);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        if bytes.len() < RECORD_HEADER_SIZE {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "insufficient bytes for record header",
            ));
        }

        let expected_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let size = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
        let record_type_byte = bytes[6];
        let log_number = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);

        let record_type = RecordType::from_u8(record_type_byte)?;

        let total_size = RECORD_HEADER_SIZE + size;
        if bytes.len() < total_size {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "insufficient bytes for record data: expected {}, got {}",
                    total_size,
                    bytes.len()
                ),
            ));
        }

        let data = &bytes[RECORD_HEADER_SIZE..total_size];

        let mut crc_data = Vec::with_capacity(1 + 4 + size);
        crc_data.push(record_type_byte);
        crc_data.extend_from_slice(&log_number.to_le_bytes());
        crc_data.extend_from_slice(data);
        let actual_crc = xxh32(&crc_data, 0);

        if actual_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "checksum mismatch: expected {}, got {}",
                    expected_crc, actual_crc
                ),
            ));
        }

        Ok((
            Record {
                record_type,
                log_number,
                data: data.to_vec(),
            },
            total_size,
        ))
    }
}

pub struct RecordBatch {
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn new() -> Self {
        RecordBatch {
            records: Vec::new(),
        }
    }

    pub fn add(&mut self, record: Record) {
        self.records.push(record);
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut encoded = Vec::new();
        for record in &self.records {
            encoded.extend_from_slice(&record.encode()?);
        }
        Ok(encoded)
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let data = b"hello world".to_vec();
        let record = Record::new(data.clone());
        let encoded = record.encode().unwrap();

        let (decoded, size) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.record_type, RecordType::Full);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_record_types() {
        for (record_type, type_val) in [
            (RecordType::Full, 1u8),
            (RecordType::First, 2u8),
            (RecordType::Middle, 3u8),
            (RecordType::Last, 4u8),
        ] {
            let data = b"test data".to_vec();
            let record = Record::new_with_type(record_type, data.clone());
            let encoded = record.encode().unwrap();

            let (decoded, _) = Record::decode(&encoded).unwrap();
            assert_eq!(decoded.data, data);
            assert_eq!(decoded.record_type, record_type);
            assert_eq!(encoded[6], type_val);
        }
    }

    #[test]
    fn test_record_checksum_validation() {
        let data = b"hello world".to_vec();
        let record = Record::new(data);
        let mut encoded = record.encode().unwrap();

        encoded[RECORD_HEADER_SIZE] = encoded[RECORD_HEADER_SIZE].wrapping_add(1);

        let result = Record::decode(&encoded);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new();
        batch.add(Record::new(b"record1".to_vec()));
        batch.add(Record::new(b"record2".to_vec()));
        batch.add(Record::new(b"record3".to_vec()));

        let encoded = batch.encode().unwrap();
        assert!(!encoded.is_empty());
        assert_eq!(batch.len(), 3);

        let mut offset = 0;
        for i in 0..3 {
            let (record, size) = Record::decode(&encoded[offset..]).unwrap();
            assert_eq!(record.data, format!("record{}", i + 1).as_bytes());
            assert_eq!(record.record_type, RecordType::Full);
            offset += size;
        }
    }

    #[test]
    fn test_incomplete_record() {
        let data = b"hello".to_vec();
        let record = Record::new(data);
        let encoded = record.encode().unwrap();

        let result = Record::decode(&encoded[..5]);
        assert!(result.is_err());
    }

    #[test]
    fn test_crc_computed_over_type_log_number_and_data() {
        let data = b"test".to_vec();
        let record = Record::new(data.clone());
        let encoded = record.encode().unwrap();

        let mut crc_data = vec![RecordType::Full as u8];
        crc_data.extend_from_slice(&0u32.to_le_bytes());
        crc_data.extend_from_slice(&data);
        let expected_crc = xxh32(&crc_data, 0);
        let encoded_crc = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);

        assert_eq!(encoded_crc, expected_crc);
    }

    #[test]
    fn test_header_size() {
        assert_eq!(RECORD_HEADER_SIZE, 11);
    }

    #[test]
    fn test_log_number() {
        let data = b"test data".to_vec();
        let log_number = 12345u32;
        let record = Record::new_with_log_number(RecordType::Full, log_number, data.clone());
        let encoded = record.encode().unwrap();

        let (decoded, _) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.log_number, log_number);
        assert_eq!(decoded.record_type, RecordType::Full);
    }

    #[test]
    fn test_large_record() {
        let data = vec![0xAB; 32 * 1024];
        let record = Record::new(data.clone());
        let encoded = record.encode().unwrap();

        let (decoded, size) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data, data);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_max_size_record() {
        let data = vec![0xFF; u16::MAX as usize];
        let record = Record::new(data.clone());
        let encoded = record.encode().unwrap();

        let (decoded, _) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data.len(), u16::MAX as usize);
    }
}
