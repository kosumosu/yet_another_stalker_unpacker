use std::collections::HashMap;
use std::error::Error;
use std::io::{Cursor, SeekFrom};
use std::ops::Deref;
use std::path::{PathBuf};
use std::time::Instant;
use clap::{arg, Parser};
use delharc::decode::{Decoder, Lh1Decoder};
use encoding_rs::{Encoding, UTF_8};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use futures::future::join_all;
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
pub(crate) struct ArchiveHeader {
    pub(crate) archive_path: PathBuf,
    pub(crate) output_root_path: String,
    pub(crate) files: HashMap<String, FileDescriptor>,
}

async fn read_archive_header(archive_file_name: PathBuf, encoding: &'static Encoding) -> Option<ArchiveHeader> {
    let mut file = fs::File::options()
        .read(true)
        .write(false)
        .open(archive_file_name.as_path()).await.expect(format!("Failed to open archive {}", archive_file_name.to_str().unwrap()).as_str());

    let archive_len = file.metadata().await.unwrap().len();

    let mut archive_pos = 0u64;

    let mut file_descriptors = None;
    let mut root_path = "".to_string();

    while archive_pos < archive_len {
        let raw_chunk_id = file.read_u32_le().await.unwrap();
        let chunk_size = file.read_u32_le().await.unwrap();
        let chunk_usize = usize::try_from(chunk_size).unwrap();

        let chunk_id = raw_chunk_id & CHUNK_ID_MASK;
        let compressed = (raw_chunk_id & CHUNK_ID_COMPRESSED_MASK) != 0;
        //println!("archive_pos={archive_pos} chunk_id={chunk_id} compressed={compressed} chunk_size={chunk_size}");

        match chunk_id {
            0x1 | 0x86 => {
                // File descriptors list
                let chunk_data = match compressed {
                    true => {
                        let decoded_len = file.read_u32_le().await.unwrap();
                        let mut compressed_buf = vec![0u8; chunk_usize - 4usize];
                        file.read_exact(&mut compressed_buf.as_mut_slice()).await.unwrap();

                        archive_pos += 12u64; // chunk id (aka dwType), size, and decoded size fields
                        archive_pos += chunk_size as u64;

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
                };

                let mut reader = Cursor::new(chunk_data.as_slice());

                file_descriptors = Some(read_file_descriptors(&mut reader, encoding).expect("Cannot parse header chunk"));
            }
            // 666 | 0x1337 => {
            //     panic!("TODO")
            // }
            _ => {
                // Skip
                let seek_operation = file.seek(SeekFrom::Current(i64::try_from(chunk_size).unwrap()));
                archive_pos = seek_operation.await.unwrap();
            }
        }
    }

    return  match file_descriptors {
        Some(file_descriptors) => Some(ArchiveHeader { archive_path: archive_file_name, output_root_path: root_path, files: file_descriptors }),
        _ => None
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let encoding = args.encoding.map_or(UTF_8, |enc| Encoding::for_label(enc.as_bytes()).expect("Specified encoding not found"));

    let start_instant = Instant::now();

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
                    .map(|i| tokio::spawn(read_archive_header(i.clone(), encoding)))
                    .collect::<Vec<_>>()
                    ;

                join_all(archive_tasks).await.into_iter().map(|i| i.unwrap()).collect::<Vec<_>>()
            }
            false => {
                let mut archives = Vec::with_capacity(entries.len());
                for entry in entries.into_iter() {
                    archives.push(read_archive_header(entry.clone(), encoding).await);
                }
                archives
            }
        };

        for archive_header in archive_headers.iter()

            .filter(|i| i.is_some())
            .map(|i| i.as_ref().unwrap())
        {
            //println!("{:#?}", archive_header.unwrap());
            println!("Archive: {} files: {}", archive_header.archive_path.to_str().unwrap(), archive_header.files.len());
        }

        let total_file_count = archive_headers.iter().fold(0, |acc, i| acc + i.as_ref().map_or(0, |x| x.files.len()));

        println!("Total files: {total_file_count}");

    } else if args.input_archive.is_some() {
        let path_str = args.input_archive.unwrap();
        let path = PathBuf::from(path_str);
        let result = read_archive_header(path, encoding).await;
        println!("result: {:#?}", result);
    } else {
        panic!("No inputs specified")
    }

    println!("Took {} sec", start_instant.elapsed().as_secs_f32());
}
