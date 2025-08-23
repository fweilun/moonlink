use bincode::enc::{write::Writer, Encode, Encoder};
use bincode::error::EncodeError;
use more_asserts as ma;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeletionVector {
    pub data_file_number: u32,
    pub puffin_file_number: u32,
    pub offset: u32,
    pub size: u32,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct PositionDelete {
    pub data_file_number: u32,
    pub data_file_row_number: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MooncakeTableMetadata {
    pub data_files: Vec<String>,
    pub puffin_files: Vec<String>,
    pub deletion_vectors: Vec<DeletionVector>,
    pub position_deletes: Vec<PositionDelete>,
}

impl Encode for MooncakeTableMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let writer = encoder.writer();

        // Write data filepaths offsets.
        write_usize(writer, self.data_files.len())?;
        let mut offset = 0;
        for data_file in &self.data_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(data_file.len());
        }
        write_usize(writer, offset)?;

        // Write deletion vector puffin blob filepaths offsets.
        // Arrange all offsets together (instead of mixing with blob start offset and blob size), so decode side could directly operate on `uint32_t` pointers.
        write_usize(writer, self.puffin_files.len())?;
        let mut offset = 0;
        for puffin_file in &self.puffin_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(puffin_file.len());
        }
        write_usize(writer, offset)?;

        // Used to check deletion vector ordering.
        let mut prev_data_file_number = 0;
        // Write deletion vector puffin blob information.
        write_usize(writer, self.deletion_vectors.len())?;
        for deletion_vector in &self.deletion_vectors {
            ma::assert_ge!(deletion_vector.data_file_number, prev_data_file_number);
            prev_data_file_number = deletion_vector.data_file_number;

            write_u32(writer, deletion_vector.data_file_number)?;
            write_u32(writer, deletion_vector.puffin_file_number)?;
            write_u32(writer, deletion_vector.offset)?;
            write_u32(writer, deletion_vector.size)?;
        }

        // Used to check positional deletes ordering.
        let mut prev_position_delete_data_file_number = 0;
        // Write positional deletion records.
        write_usize(writer, self.position_deletes.len())?;
        for position_delete in &self.position_deletes {
            ma::assert_ge!(
                position_delete.data_file_number,
                prev_position_delete_data_file_number
            );
            prev_position_delete_data_file_number = position_delete.data_file_number;

            write_u32(writer, position_delete.data_file_number)?;
            write_u32(writer, position_delete.data_file_row_number)?;
        }

        // Write data filepaths.
        for data_file in &self.data_files {
            writer.write(data_file.as_bytes())?;
        }

        // Write puffin filepaths.
        for puffin_file in &self.puffin_files {
            writer.write(puffin_file.as_bytes())?;
        }

        Ok(())
    }
}

fn write_u32<W: Writer>(writer: &mut W, value: u32) -> Result<(), EncodeError> {
    writer.write(&value.to_ne_bytes())
}

fn write_usize<W: Writer>(writer: &mut W, value: usize) -> Result<(), EncodeError> {
    let value = u32::try_from(value).map_err(|_| EncodeError::Other("out of range"))?;
    write_u32(writer, value)
}

impl MooncakeTableMetadata {
    pub fn decode(data: &[u8]) -> Self {
        use std::convert::TryInto;

        let mut cursor = 0;

        fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
            let val = u32::from_ne_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            val
        }

        fn read_usize(data: &[u8], cursor: &mut usize) -> usize {
            read_u32(data, cursor) as usize
        }

        // Read data filepath offsets.
        let data_files_len = read_usize(data, &mut cursor);
        let mut data_file_offsets = Vec::with_capacity(data_files_len + 1);
        for _ in 0..=data_files_len {
            let data_file_offset = read_usize(data, &mut cursor);
            data_file_offsets.push(data_file_offset);
        }

        // Read puffin filepath offsets.
        let puffin_files_len = read_usize(data, &mut cursor);
        let mut puffin_file_offsets = Vec::with_capacity(puffin_files_len + 1);
        for _ in 0..=puffin_files_len {
            let puffin_file_offset = read_usize(data, &mut cursor);
            puffin_file_offsets.push(puffin_file_offset);
        }

        // Read deletion vector blobs.
        let puffin_blob_len = read_usize(data, &mut cursor);
        let mut deletion_vectors_blobs = Vec::with_capacity(puffin_blob_len);
        for _ in 0..puffin_blob_len {
            let data_file_number = read_u32(data, &mut cursor);
            let puffin_file_number = read_u32(data, &mut cursor);
            let offset = read_u32(data, &mut cursor);
            let size = read_u32(data, &mut cursor);
            deletion_vectors_blobs.push(DeletionVector {
                data_file_number,
                puffin_file_number,
                offset,
                size,
            });
        }

        // Read positional delete records.
        let position_deletes_len = read_usize(data, &mut cursor);
        let mut position_deletes = Vec::with_capacity(position_deletes_len);
        for _ in 0..position_deletes_len {
            let data_file_number = read_u32(data, &mut cursor);
            let data_file_row_number = read_u32(data, &mut cursor);
            position_deletes.push(PositionDelete {
                data_file_number,
                data_file_row_number,
            });
        }

        // Read data filepaths.
        let mut data_files = Vec::with_capacity(data_files_len);
        for i in 0..data_files_len {
            let start = data_file_offsets[i];
            let end = data_file_offsets[i + 1];
            let s = String::from_utf8(data[cursor + start..cursor + end].to_vec()).unwrap();
            data_files.push(s);
        }
        if data_files_len > 0 {
            cursor += data_file_offsets.last().unwrap();
        }

        // Read puffin filepaths.
        let mut puffin_files = Vec::with_capacity(puffin_files_len);
        for i in 0..puffin_files_len {
            let start = puffin_file_offsets[i];
            let end = puffin_file_offsets[i + 1];
            let cur_puffin_filepath =
                String::from_utf8(data[cursor + start..cursor + end].to_vec()).unwrap();
            puffin_files.push(cur_puffin_filepath);
        }
        if data_files_len > 0 {
            cursor += puffin_file_offsets.last().unwrap();
        }

        MooncakeTableMetadata {
            data_files,
            puffin_files,
            deletion_vectors: deletion_vectors_blobs,
            position_deletes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::config;

    const BINCODE_CONFIG: config::Configuration = config::standard();

    /// Util function to create a puffin deletion blob.
    fn create_puffin_deletion_blob_1() -> (String /*puffin filepath*/, DeletionVector) {
        let deletion_blob = DeletionVector {
            data_file_number: 0,
            puffin_file_number: 0,
            offset: 4,
            size: 10,
        };
        let puffin_filepath = "/tmp/iceberg_test/1-puffin.bin".to_string();
        (puffin_filepath, deletion_blob)
    }
    fn create_puffin_deletion_blob_2() -> (String /*puffin filepath*/, DeletionVector) {
        let deletion_blob = DeletionVector {
            data_file_number: 0,
            puffin_file_number: 1,
            offset: 4,
            size: 20,
        };
        let puffin_filepath = "/tmp/iceberg_test/2-puffin.bin".to_string();
        (puffin_filepath, deletion_blob)
    }

    #[test]
    fn test_table_metadata_serde() {
        let (puffin_file_1, deletion_blob_1) = create_puffin_deletion_blob_1();
        let (puffin_file_2, deletion_blob_2) = create_puffin_deletion_blob_2();
        let table_metadata = MooncakeTableMetadata {
            data_files: vec![
                "/tmp/iceberg_test/data/1.parquet".to_string(),
                "/tmp/iceberg_test/data/2.parquet".to_string(),
                "/tmp/iceberg-rust/data/temp.parquet".to_string(), // associate file
            ],
            puffin_files: vec![puffin_file_1, puffin_file_2],
            deletion_vectors: vec![deletion_blob_1, deletion_blob_2],
            position_deletes: vec![PositionDelete {
                data_file_number: 2,
                data_file_row_number: 2,
            }],
        };
        let data = bincode::encode_to_vec(table_metadata.clone(), BINCODE_CONFIG).unwrap();

        let decoded_metadata = MooncakeTableMetadata::decode(data.as_slice());
        assert_eq!(table_metadata, decoded_metadata);
    }
}
