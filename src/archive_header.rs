use std::collections::HashMap;
use std::io::{Error, Read};
use std::io::ErrorKind::UnexpectedEof;
use byteorder::{LittleEndian, ReadBytesExt};
use encoding_rs::{Encoding};

#[derive(Debug, Clone)]
pub(crate) struct FileDescriptor {
    name: String,
    offset: u32,
    real_size: u32,
    compressed_size: u32,
    crc: u32,
}

pub(crate) fn read_file_descriptors<T: Read>(reader: &mut T, encoding: &'static Encoding) -> Result<HashMap<String, FileDescriptor>, Error> {
    let mut file_descriptors = HashMap::new();

    let mut name_buf = [0u8; 260 * 2]; // MAX_PATH * 2

    loop {
        const ELEMENTS_SIZE: u16 = 16;

        let header_size = match reader.read_u16::<LittleEndian>() {
            Ok(data) => data,
            Err(err) if err.kind() == UnexpectedEof => break,
            Err(err) => return Err(err)
        };

        let real_size = reader.read_u32::<LittleEndian>().unwrap();
        let compressed_size = reader.read_u32::<LittleEndian>().unwrap();
        let crc = reader.read_u32::<LittleEndian>().unwrap();

        let name_size = header_size - ELEMENTS_SIZE;

        let name_bytes = {
            assert!((name_size as usize) < name_buf.len(), "Name is too long");

            reader.read_exact(& mut name_buf[..(name_size as usize)]).expect("Unable to read file name from header");
            &name_buf[..(name_size as usize)]
        };

        let offset = reader.read_u32::<LittleEndian>().unwrap();

        let (name, had_errors) = encoding.decode_without_bom_handling(&name_bytes);

        if had_errors {
            panic!("Had errors decoding file name '{}' raw bytes: {:?}", name, &name_bytes);
        }

        file_descriptors.insert(name.to_string(), FileDescriptor { name: name.to_string(), offset, real_size, compressed_size, crc });
    }

    Ok(file_descriptors)
}
