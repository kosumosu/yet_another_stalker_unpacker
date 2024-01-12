use std::collections::HashMap;
use std::io::{Cursor, SeekFrom};
use std::io::ErrorKind::UnexpectedEof;
use std::path::{PathBuf};
use std::sync::Arc;
use std::time::Instant;
use clap::{arg, Parser};
use delharc::decode::{Decoder, Lh1Decoder};
use encoding_rs::{Encoding, UTF_8};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use futures::future::join_all;
use regex::{Captures, Regex};
use crate::archive_header::{FileDescriptor, read_file_descriptors};

mod archive_header;

const CHUNK_ID_COMPRESSED_MASK: u32 = 1 << 31;
const CHUNK_ID_MASK: u32 = !(1 << 31);

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input_archive: Option<String>,

    #[arg(short, long)]
    source_dir: Option<String>,

    #[arg(short, long)]
    output_dir: String,

    #[arg(short, long)]
    encoding: Option<String>,
}

#[derive(Debug, Clone)]
struct ArchiveHeader {
    pub(crate) archive_path: PathBuf,
    pub(crate) output_root_path: String,
    pub(crate) files: HashMap<String, FileDescriptor>,
}

struct ArchiveReader {
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
            encoding
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
                0x1 | 0x86 => {
                    // File descriptors list
                    let chunk_data = read_chunk(&mut file, chunk_usize, compressed).await;

                    let mut reader = Cursor::new(chunk_data.as_slice());

                    file_descriptors = Some(read_file_descriptors(&mut reader, &self.encoding).expect("Cannot parse header chunk"));
                }
                666 | 1337 => {
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

        //let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
        //let mut last_section: HashMap<String, String> = HashMap::new();
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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let encoding = args.encoding.map_or(UTF_8, |enc| Encoding::for_label(enc.as_bytes()).expect("Specified encoding not found"));

    let start_instant = Instant::now();

    let archive_reader = Arc::new(ArchiveReader::new(encoding));

    if args.source_dir.is_some() {
        println!("Scanning directory");

        let mut entries = std::fs::read_dir(args.source_dir.unwrap().as_str())
            .expect("Can't get directory contents")
            .filter(|i| {
                let entry = i.as_ref().unwrap();
                entry.file_type().unwrap().is_file()
                    && entry.path().extension()
                    .map(|ext| ext.to_str().unwrap())
                    .map(|ext| ext.starts_with("db") || ext.starts_with("xdb"))
                    .unwrap_or(false)
            })
            .map(|i| i.unwrap().path())
            //.flat_map(|i| (0..100).map(move |_| i.clone()))
            .collect::<Vec<_>>()
            ;

        entries.sort();

        println!("Reading archive headers");

        const PARALLEL: bool = true;
        let archive_headers = match PARALLEL {
            true => {
                let archive_tasks = entries.iter()
                    .map(|i| {
                        let archive_reader = archive_reader.clone();
                        let i = i.clone();
                        tokio::spawn(async move { archive_reader.read_archive_header(i).await })
                    })
                    .collect::<Vec<_>>()
                    ;

                join_all(archive_tasks).await.into_iter().map(|i| i.unwrap()).collect::<Vec<_>>()
            }
            false => {
                let mut archives = Vec::with_capacity(entries.len());
                for entry in entries.into_iter() {
                    archives.push(archive_reader.read_archive_header(entry.clone()).await);
                }
                archives
            }
        };

        for archive_header in archive_headers.iter()

            .filter(|i| i.is_some())
            .map(|i| i.as_ref().unwrap())
        {
            //println!("{:#?}", archive_header.unwrap());
            println!("Archive: {} root: {} files: {}", archive_header.archive_path.to_str().unwrap(), archive_header.output_root_path, archive_header.files.len());
        }

        let total_file_count = archive_headers.iter().fold(0, |acc, i| acc + i.as_ref().map_or(0, |x| x.files.len()));

        println!("Total files: {total_file_count}");
    } else if args.input_archive.is_some() {
        let path_str = args.input_archive.unwrap();
        let path = PathBuf::from(path_str);
        let result = archive_reader.read_archive_header(path).await;
        println!("result: {:#?}", result);
    } else {
        panic!("No inputs specified")
    }

    println!("Took {} sec", start_instant.elapsed().as_secs_f32());
}
