use std::collections::HashMap;
use regex::Regex;
use encoding_rs::Encoding;
use std::path::PathBuf;
use tokio::fs;
use std::io::ErrorKind::UnexpectedEof;
use std::io::{Cursor, SeekFrom};
use std::sync::Arc;
use delharc::decode::{Decoder, Lh1Decoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use crate::archive_header::{FileDescriptor, read_file_descriptors};

const CHUNK_ID_COMPRESSED_MASK: u32 = 1 << 31;
const CHUNK_ID_MASK: u32 = !(1 << 31);

#[derive(Debug, Clone)]
pub struct ArchiveHeader {
    pub archive_path: PathBuf,
    pub output_root_path: String,
    pub files: HashMap<Arc<String>, FileDescriptor>,
}

pub struct ArchiveReader {
    section_regex: Regex,
    variable_regex: Regex,
    root_regex: Regex,
    encoding: &'static Encoding,
}

impl ArchiveReader {
    pub fn new(encoding: &'static Encoding) -> Self {
        Self {
            section_regex: Regex::new(r"^.*\[(?P<name>\w*)\]$").unwrap(),
            variable_regex: Regex::new(r"^\s*(?P<name>\w+)\s*=\s*(?P<value>.+)\s*$").unwrap(),
            root_regex: Regex::new(r"^\$\w+?\$\\").unwrap(),
            encoding,
        }
    }

    pub async fn read_archive_header(&self, archive_file_name: PathBuf) -> Option<ArchiveHeader> {
        //println!("Processing archive: {}", archive_file_name.to_str().unwrap());

        let mut file = fs::File::options()
            .read(true)
            .write(false)
            .open(archive_file_name.as_path()).await.expect(format!("Failed to open archive {}", archive_file_name.to_str().unwrap()).as_str());

        let mut file_descriptors = None;
        let mut root_path = "".to_string();

        loop {
            let raw_chunk_id = match file.read_u32_le().await {
                Ok(data) => data,
                Err(err) if err.kind() == UnexpectedEof => break,
                Err(err) => panic!("Error reading file: {}", err)
            };
            let chunk_size = file.read_u32_le().await.unwrap();
            let chunk_usize = usize::try_from(chunk_size).unwrap();

            let chunk_id = raw_chunk_id & CHUNK_ID_MASK;
            let compressed = (raw_chunk_id & CHUNK_ID_COMPRESSED_MASK) != 0;
            //println!("archive_pos={archive_pos} chunk_id={chunk_id} compressed={compressed} chunk_size={chunk_size}");

            match chunk_id {
                0x1 | 0x86 => { // File descriptors list
                    let chunk_data = read_chunk(&mut file, chunk_usize, compressed).await;

                    let mut reader = Cursor::new(chunk_data.as_slice());

                    file_descriptors = Some(read_file_descriptors(&mut reader, &self.encoding).expect("Expecting a valid file descriptors chunk"));
                }
                666 | 1337 => { // Metadata header
                    let chunk_data = read_chunk(&mut file, chunk_usize, compressed).await;

                    root_path = self.read_root_path(chunk_data.as_slice()).expect("[header].entry_point must be specified in header chunk when it exists");
                }
                _ => {
                    // Skip
                    file.seek(SeekFrom::Current(i64::try_from(chunk_size).unwrap())).await.unwrap();
                }
            }
        }

        return match file_descriptors {
            Some(file_descriptors) => Some(ArchiveHeader { archive_path: archive_file_name, output_root_path: root_path, files: file_descriptors }),
            _ => None
        };
    }

    fn read_root_path(&self, chunk_data: &[u8]) -> Option<String> {
        // let section_regex= Regex::new(r"^.*\[(?P<name>\w*)\]$").unwrap();
        // let variable_regex= Regex::new(r"^\s*(?P<name>\w+)\s*=\s*(?P<value>.+)\s*$").unwrap();
        // let root_regex = Regex::new(r"^\$\w+?\$\\").unwrap();

        let section_regex = &self.section_regex;
        let variable_regex = &self.variable_regex;
        let root_regex = &self.root_regex;

        let (text, had_errors) = self.encoding.decode_without_bom_handling(chunk_data);

        if had_errors {
            panic!("Unable to decode header: {}", text);
        }

        let mut last_section_name = "".to_string();
        for line in text.lines() {
            let section_captures = section_regex.captures(line);
            match (section_captures, last_section_name.as_str()) {
                (None, "header") => {
                    let variable_captures = variable_regex.captures(line);
                    match variable_captures {
                        Some(captures) => {
                            if &captures["name"] == "entry_point" {
                                let entry_point = captures["value"].to_string();
                                return Some(root_regex.replace(entry_point.as_str(), "").to_string());
                            }
                        }
                        _ => {}
                    }
                }
                (Some(capture), _) => {
                    last_section_name = capture["name"].to_string();
                }
                _ => {}
            }
        }

        None
    }
}

async fn read_chunk<T: AsyncRead + Unpin>(file: &mut T, chunk_usize: usize, compressed: bool) -> Vec<u8> {
    match compressed {
        true => {
            let decoded_len = file.read_u32_le().await.unwrap();
            let mut compressed_buf = vec![0u8; chunk_usize - 4usize];
            file.read_exact(&mut compressed_buf.as_mut_slice()).await.unwrap();

            let mut res = Lh1Decoder::new(compressed_buf.as_slice());

            let mut decompressed_buf = vec![0u8; decoded_len as usize];
            res.fill_buffer(&mut decompressed_buf).unwrap();

            decompressed_buf
        }
        false => {
            let mut raw_buf = vec![0u8; chunk_usize];
            file.read_exact(&mut raw_buf.as_mut_slice()).await.unwrap();

            raw_buf
        }
    }
}