use std::collections::{HashMap, HashSet};
use std::io::Error;
use std::io::ErrorKind::AlreadyExists;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use clap::{arg, Parser, Subcommand};
use encoding_rs::Encoding;
use futures::future::join_all;
use archive_reader::ArchiveReader;
use log::{debug, info, Level};
use std_err_logger::StdErrLogger;
use crate::archive_header::FileDescriptor;
use crate::archive_reader::ArchiveHeader;

mod archive_header;
mod archive_reader;
mod std_err_logger;


#[derive(Subcommand, Debug, Clone)]
enum InputTypes {
    Archives { input: Vec<String> },
    Dirs { input: Vec<String> },
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    mode: InputTypes,

    #[arg(short, long, help = "Output directory")]
    output_dir: String,

    #[arg(short, long, default_value = "utf-8", help = "Encoding to use. For cases when archive contains non-ascii symbols in file names and headers. Examples: \"utf-8\", \"cp1251\"")]
    encoding: String,

    #[arg(short, long, default_value_t = false, help = "Don't use multithreading. Can reduce peak memory usage.")]
    sequential: bool,

    #[arg(short, long, default_value_t = Level::Warn, help = "Sets logging level for debug purposes")]
    log_level: Level,
}

#[derive(Debug, Clone)]
pub struct ShortArchiveHeader {
    pub archive_path: PathBuf,
    pub output_root_path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    log::set_boxed_logger(Box::new(StdErrLogger::new(args.log_level))).unwrap();

    let encoding = Encoding::for_label(args.encoding.as_bytes()).expect("Specified encoding not found");

    let start_instant = Instant::now();

    let archive_reader = Arc::new(ArchiveReader::new(encoding));

    let files: Vec<_> = match args.mode {
        InputTypes::Archives { input } => {
            input
                .iter().map(|file| PathBuf::from(file))
                .collect()
        }
        InputTypes::Dirs { input } => {
            info!("Scanning directory");

            input.iter().flat_map(|dir| {
                let mut files = std::fs::read_dir(dir)
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

                files.sort();

                files
            })
                .collect()
        }
    };

    info!("Reading archive headers");

    let archive_headers = read_headers(archive_reader, files, args.sequential).await;

    let total_file_count = archive_headers.iter().fold(0, |acc, i| acc + i.as_ref().map_or(0, |x| x.files.len()));

    info!("Finding overridden files");

    let mut files = HashMap::new();

    for archive_header in archive_headers.into_iter()
        .filter(|i| i.is_some())
        .map(|i| i.unwrap())
    {
        let short_archive_header = Arc::new(ShortArchiveHeader { archive_path: archive_header.archive_path, output_root_path: archive_header.output_root_path });

        let archive_name = short_archive_header.archive_path.file_name().unwrap().to_string_lossy();
        debug!("Archive: {} root: {} files: {}", archive_name, short_archive_header.output_root_path, archive_header.files.len());

        for (file_name, desc) in archive_header.files.into_iter() {
            match files.insert(file_name, (short_archive_header.clone(), desc)) {
                None => {}
                Some((old_archive_header, old_desc)) => {
                    debug!("File [{}] from archive [{}] overrides a file from archive [{}]", old_desc.name, archive_name, old_archive_header.archive_path.file_name().unwrap().to_string_lossy());
                }
            };
        }
    }

    info!("Creating directory structure");

    create_directory_structure(&args.output_dir, &mut files).await;

    info!("Unpacking files");

    let unpack_tasks: Vec<_> = files.into_iter().map(|(_file_name, (archive_header, desc))| {
        tokio::spawn(async move {
            unpack_file(&archive_header, &desc).await
        })
    }).collect();

    join_all(unpack_tasks).await;

    info!("Total files: {total_file_count}");

    eprintln!("Done. Took {} sec", start_instant.elapsed().as_secs_f32());
}

async fn create_directory_structure(output_dir: &str, files: &mut HashMap<Arc<String>, (Arc<ShortArchiveHeader>, FileDescriptor)>) {
    tokio::fs::create_dir_all(output_dir.clone()).await.expect("Output directory must exist or be creatable");

    let mut dirs = files.iter()
        .map(|(file_name, (archive_header, desc))| {
            let mut path = PathBuf::from_str(archive_header.output_root_path.as_str()).expect("Valid output root path");
            path.push(match desc.real_size {
                0 => PathBuf::from_str(file_name.as_str()).unwrap(),
                _ => PathBuf::from_str(file_name.as_str()).unwrap().parent().unwrap_or(Path::new("")).to_path_buf()
            });
            path
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        ;

    dirs.sort();

    for dir in dirs.into_iter() {
        let absolute_path: PathBuf = [output_dir, dir.to_str().unwrap()].into_iter().collect();
        match tokio::fs::create_dir(absolute_path.as_path()).await {
            Ok(_) => {},
            Err(e) if e.kind() == AlreadyExists => {},
            Err(e) => panic!("Can't create directory [{}]. Error: {}", absolute_path.to_str().unwrap(), e)
        };
    }
}

async fn read_headers(archive_reader: Arc<ArchiveReader>, files: Vec<PathBuf>, sequential: bool) -> Vec<Option<ArchiveHeader>> {
    match sequential {
        false => {
            let archive_tasks = files.iter()
                .map(|i| {
                    let archive_reader = archive_reader.clone();
                    let i = i.clone();
                    tokio::spawn(async move { archive_reader.read_archive_header(i).await })
                })
                .collect::<Vec<_>>()
                ;

            join_all(archive_tasks).await.into_iter().map(|i| i.unwrap()).collect::<Vec<_>>()
        }
        true => {
            let mut archives = Vec::with_capacity(files.len());
            for entry in files.into_iter() {
                archives.push(archive_reader.read_archive_header(entry.clone()).await);
            }
            archives
        }
    }
}

async fn unpack_file(archive_header: &ShortArchiveHeader, file_descriptor: &FileDescriptor) {}