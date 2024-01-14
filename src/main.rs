use std::cmp::{min};
use std::collections::{HashMap, HashSet};
use std::io::{SeekFrom};
use std::io::ErrorKind::AlreadyExists;
use std::path::{PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use clap::{arg, Parser, Subcommand};
use encoding_rs::Encoding;
use futures::future::join_all;
use archive_reader::ArchiveReader;
use log::{debug, info, LevelFilter};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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

    #[arg(short, long, default_value_t = LevelFilter::Warn, help = "Sets logging level for debug purposes")]
    log_level: LevelFilter,
}

#[derive(Debug, Clone)]
pub struct ShortArchiveHeader {
    pub archive_path: PathBuf,
    pub output_root_path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    log::set_boxed_logger(Box::new(StdErrLogger::new())).unwrap();
    log::set_max_level(args.log_level);

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

    let mut files_and_dirs = HashMap::new();

    for archive_header in archive_headers.into_iter()
        .filter(|i| i.is_some())
        .map(|i| i.unwrap())
    {
        let short_archive_header = Arc::new(ShortArchiveHeader { archive_path: archive_header.archive_path, output_root_path: archive_header.output_root_path });

        let archive_name = short_archive_header.archive_path.file_name().unwrap().to_string_lossy();
        debug!("Archive: {} root: {} files: {}", archive_name, short_archive_header.output_root_path, archive_header.files.len());

        for (file_name, desc) in archive_header.files.into_iter() {
            match files_and_dirs.insert(file_name, (short_archive_header.clone(), desc)) {
                None => {}
                Some((old_archive_header, old_desc)) => {
                    debug!("File [{}] from archive [{}] overrides a file from archive [{}]", old_desc.name, archive_name, old_archive_header.archive_path.file_name().unwrap().to_string_lossy());
                }
            };
        }
    }

    info!("Creating directory structure");

    create_directory_structure(&args.output_dir, &mut files_and_dirs).await;

    info!("Unpacking files");

    let parallel = !args.sequential;

    let output_dir = Arc::new(args.output_dir.clone());

    let files_only = files_and_dirs.into_iter().filter(|(_, (_, desc))| desc.real_size != 0);

    let lzo = Arc::new(minilzo_rs::LZO::init().unwrap());

    match parallel {
        true => {
            let mut tasks_set = bounded_join_set::JoinSet::new(64);

            files_only.into_iter().for_each(|(_file_name, (archive_header, desc))| {
                let output_dir = output_dir.clone();
                let lzo = lzo.clone();
                tasks_set.spawn(async move {
                    unpack_file(&lzo, output_dir.as_str(), &archive_header, &desc).await
                });
            });

            while tasks_set.join_next().await.is_some() {}

            // let unpack_tasks: Vec<_> = files_only.into_iter().map(|(_file_name, (archive_header, desc))| {
            //     let output_dir = output_dir.clone();
            //     let lzo = lzo.clone();
            //     tokio::spawn(async move {
            //         unpack_file(&lzo, output_dir.as_str(), &archive_header, &desc).await
            //     })
            // }).collect();
            //
            // join_all(unpack_tasks).await;
        }
        false => {
            for (_file_name, (archive_header, desc)) in files_only.into_iter() {
                unpack_file(&lzo, output_dir.clone().as_str(), &archive_header, &desc).await
            }
        }
    }


    info!("Total files: {total_file_count}");

    eprintln!("Done. Took {} sec", start_instant.elapsed().as_secs_f32());
}

async fn create_directory_structure(output_dir: &str, files: &mut HashMap<Arc<String>, (Arc<ShortArchiveHeader>, FileDescriptor)>) {
    tokio::fs::create_dir_all(output_dir).await.expect("Output directory must exist or be creatable");

    let mut dirs = files.iter()
        .flat_map(|(file_name, (archive_header, desc))| {
            let dir = match desc.real_size {
                0 => [&archive_header.output_root_path, file_name.as_str()].iter().collect::<PathBuf>(),
                _ => [&archive_header.output_root_path, file_name.as_str()].iter().collect::<PathBuf>().parent().map_or(PathBuf::from(""), |i| i.to_path_buf())
            };

            let mut dir = dir.as_path();

            let mut parents = Vec::new();
            parents.push(dir.to_path_buf());
            while let Some(parent) = dir.parent() {
                parents.push(parent.to_path_buf());
                dir = parent;
            }

            parents

        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        ;

    dirs.sort();

    //let str = format!( "{:#?}", dirs);
    //tokio::fs::write( [output_dir, "dirs.txt"].into_iter().collect::<PathBuf>(), str.into_bytes()).await.unwrap();

    for dir in dirs.into_iter() {
        let absolute_path: PathBuf = [PathBuf::from_str(output_dir).unwrap(), dir].into_iter().collect();
        match tokio::fs::create_dir(absolute_path.as_path()).await {
            Ok(_) => {}
            Err(e) if e.kind() == AlreadyExists => {}
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
                    //tokio::spawn(async move { archive_reader.read_archive_header(i).await })
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

async fn unpack_file(lzo: &minilzo_rs::LZO, output_dir: &str, archive_header: &ShortArchiveHeader, file_descriptor: &FileDescriptor) {
    let absolute_path: PathBuf = [output_dir, archive_header.output_root_path.as_str(), file_descriptor.name.as_str()].into_iter().collect();

    let mut source_file = tokio::fs::File::options()
        .read(true)
        .write(false)
        .open(archive_header.archive_path.as_path()).await.expect("Archive can be opened for reading");

    source_file.seek(SeekFrom::Start(file_descriptor.offset as u64)).await.expect("Expected to be able to seek to start of the source file");

    let mut dest_file = tokio::fs::File::options()
        .read(false)
        .write(true)
        .create(true)
        //.truncate(true)
        .open(absolute_path).await.expect("File can be opened for writing");

    if file_descriptor.real_size != file_descriptor.compressed_size {
        let mut buf = vec![0u8; file_descriptor.compressed_size as usize];
        source_file.read_exact(buf.as_mut_slice()).await.unwrap();

        let decompressed_buf = lzo.decompress_safe(buf.as_slice(), file_descriptor.real_size as usize).expect("Valid LZO data");

        let actual_crc = crc32fast::hash(decompressed_buf.as_slice());

        assert_eq!( file_descriptor.crc, actual_crc, "CRCs do not match");

        dest_file.write_all(decompressed_buf.as_slice()).await.expect("Unable to write to dest file");
    } else {
        let mut remaining_bytes = file_descriptor.real_size as usize;
        let mut buf = vec![0u8;  min(256 * 1024, remaining_bytes)];
        while remaining_bytes != 0 {
            let to_read = min(buf.len(), remaining_bytes);
            let read = source_file.read(&mut buf[..to_read]).await.unwrap();

            assert!(read <= remaining_bytes, "Must not read more bytes than remaining");
            assert_ne!(read, 0, "Unexpected End Of File");

            dest_file.write(&buf[..read]).await.expect("Unable to write to destination file");
            remaining_bytes -= read;
        }
    }

    dest_file.set_len(file_descriptor.real_size as u64).await.unwrap();

    // info!("{:?}", absolute_path);
}