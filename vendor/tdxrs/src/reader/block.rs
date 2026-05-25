use encoding_rs::GBK;

use crate::error::{Result, TdxError};

#[derive(Debug, Clone, serde::Serialize)]
pub struct BlockRecord {
    pub blockname: String,
    pub block_type: u16,
    pub code_index: u16,
    pub code: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BlockGroup {
    pub blockname: String,
    pub block_type: u16,
    pub stock_count: u16,
    pub code_list: String,
}

const HEADER_SIZE: usize = 384;
const CODE_SIZE: usize = 7;
const BLOCK_STOCK_AREA: usize = 2800;

pub fn parse_block(data: &[u8]) -> Result<Vec<BlockRecord>> {
    if data.len() < HEADER_SIZE + 2 {
        return Err(TdxError::InvalidData("Block file too small".into()));
    }

    let mut pos = HEADER_SIZE;
    let num_blocks = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;

    let mut result = Vec::new();

    for _ in 0..num_blocks {
        if pos + 9 + 4 > data.len() {
            return Err(TdxError::InvalidData("Truncated block header".into()));
        }

        // Block name: 9 bytes GBK, null-padded
        let name_bytes = &data[pos..pos + 9];
        pos += 9;

        let (name, _, _) = GBK.decode(name_bytes);
        let block_name = name.trim_end_matches('\0').to_string();

        // stock_count (u16) + block_type (u16)
        let stock_count = u16::from_le_bytes([data[pos], data[pos + 1]]);
        let block_type = u16::from_le_bytes([data[pos + 2], data[pos + 3]]);
        pos += 4;

        let block_stock_begin = pos;

        for code_index in 0..stock_count {
            let code_end = pos + CODE_SIZE;
            if code_end > data.len() {
                return Err(TdxError::InvalidData("Truncated stock code".into()));
            }
            let code_bytes = &data[pos..code_end];
            let (code, _, _) = GBK.decode(code_bytes);
            let code = code.trim_end_matches('\0').to_string();

            result.push(BlockRecord {
                blockname: block_name.clone(),
                block_type,
                code_index,
                code,
            });

            pos += CODE_SIZE;
        }

        // Skip to next block (padded to 2800 bytes)
        pos = block_stock_begin + BLOCK_STOCK_AREA;
    }

    Ok(result)
}

pub fn parse_block_group(data: &[u8]) -> Result<Vec<BlockGroup>> {
    if data.len() < HEADER_SIZE + 2 {
        return Err(TdxError::InvalidData("Block file too small".into()));
    }

    let mut pos = HEADER_SIZE;
    let num_blocks = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
    pos += 2;

    let mut result = Vec::new();

    for _ in 0..num_blocks {
        if pos + 9 + 4 > data.len() {
            return Err(TdxError::InvalidData("Truncated block header".into()));
        }

        let name_bytes = &data[pos..pos + 9];
        pos += 9;

        let (name, _, _) = GBK.decode(name_bytes);
        let block_name = name.trim_end_matches('\0').to_string();

        let stock_count = u16::from_le_bytes([data[pos], data[pos + 1]]);
        let block_type = u16::from_le_bytes([data[pos + 2], data[pos + 3]]);
        pos += 4;

        let block_stock_begin = pos;
        let mut codes = Vec::new();

        for _ in 0..stock_count {
            let code_end = pos + CODE_SIZE;
            if code_end > data.len() {
                return Err(TdxError::InvalidData("Truncated stock code".into()));
            }
            let code_bytes = &data[pos..code_end];
            let (code, _, _) = GBK.decode(code_bytes);
            let code = code.trim_end_matches('\0').to_string();
            codes.push(code);
            pos += CODE_SIZE;
        }

        result.push(BlockGroup {
            blockname: block_name,
            block_type,
            stock_count,
            code_list: codes.join(","),
        });

        pos = block_stock_begin + BLOCK_STOCK_AREA;
    }

    Ok(result)
}

pub fn read_block_file(filename: &str) -> Result<Vec<BlockRecord>> {
    let data = std::fs::read(filename)?;
    parse_block(&data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn build_test_block() -> Vec<u8> {
        let mut data = vec![0u8; HEADER_SIZE];

        // 1 block
        data.extend_from_slice(&1u16.to_le_bytes());

        // Block name "Test" in GBK (4 bytes) + padding
        let name = b"Test\x00\x00\x00\x00\x00";
        data.extend_from_slice(name);

        // stock_count=2, block_type=1
        data.extend_from_slice(&2u16.to_le_bytes());
        data.extend_from_slice(&1u16.to_le_bytes());

        // 2 stock codes
        data.extend_from_slice(b"600519\x00");
        data.extend_from_slice(b"000858\x00");

        // Pad to 2800
        let padding = BLOCK_STOCK_AREA - 2 * CODE_SIZE;
        data.extend(std::iter::repeat(0u8).take(padding));

        data
    }

    #[test]
    fn test_parse_block_flat() {
        let data = build_test_block();
        let records = parse_block(&data).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].blockname, "Test");
        assert_eq!(records[0].code, "600519");
        assert_eq!(records[0].code_index, 0);
        assert_eq!(records[1].code, "000858");
        assert_eq!(records[1].code_index, 1);
    }

    #[test]
    fn test_parse_block_group() {
        let data = build_test_block();
        let groups = parse_block_group(&data).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].blockname, "Test");
        assert_eq!(groups[0].stock_count, 2);
        assert_eq!(groups[0].code_list, "600519,000858");
    }

    #[test]
    fn test_read_block_file() {
        let data = build_test_block();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_block.dat");
        let mut f = File::create(&path).unwrap();
        f.write_all(&data).unwrap();
        drop(f);

        let records = read_block_file(path.to_str().unwrap()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].code, "600519");
    }
}
