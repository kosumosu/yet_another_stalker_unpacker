use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use clap::{arg, Parser, Subcommand};
use delharc::decode::{Decoder, Lh1Decoder};
use encoding_rs::Encoding;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use futures::future::join_all;
use archive_reader::ArchiveReader;

mod archive_header;
mod archive_reader;


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

    #[arg(short, long, default_value_t = false, help = "Don't use multithreading. Can help with peak memory usage.")]
    sequential: bool,
}


#[tokio::main]
async fn main() {
    let args = Args::parse();

    let encoding = Encoding::for_label(args.encoding.as_bytes()).expect("Specified encoding not found");

    let start_instant = Instant::now();

    let archive_reader = Arc::new(ArchiveReader::new(encoding));

    let files: Vec<_> = match args.mode {
        InputTypes::Archives { input } => {
            input
                .iter().map(|i| PathBuf::from(i))
                .collect()
        }
        InputTypes::Dirs { input } => {
            println!("Scanning directory");

            input.iter().flat_map(|dir| {
                let mut entries = std::fs::read_dir(dir)
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

                entries
            })
                .collect()
        }
    };

    println!("Reading archive headers");

    let archive_headers = match args.sequential {
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

    println!("Took {} sec", start_instant.elapsed().as_secs_f32());
}
