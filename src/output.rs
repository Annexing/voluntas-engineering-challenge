use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::{book::Record, util::AlignedSbeWriter};

pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Command {
    Records(Vec<Record>),
    Flush,
}

pub enum Destination {
    Stdout,
    File(PathBuf),
    Both(PathBuf),
}

impl Destination {
    pub fn open(self) -> Result<Output> {
        match self {
            Self::Stdout => Ok(Output::Stdout(io::stdout())),
            Self::File(path) => Ok(Output::File(create_file(path)?)),
            Self::Both(path) => Ok(Output::Both(create_file(path)?, io::stdout())),
        }
    }
}

fn create_file(path: PathBuf) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| {
            format!(
                "cannot create output {}; choose a new path if it exists",
                path.display()
            )
        })
}

pub enum Output {
    Stdout(io::Stdout),
    File(File),
    Both(File, io::Stdout),
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stdout(stdout) => stdout.write(bytes),
            Self::File(file) => file.write(bytes),
            Self::Both(file, stdout) => {
                file.write_all(bytes)?;
                stdout.write_all(bytes)?;
                Ok(bytes.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stdout(stdout) => stdout.flush(),
            Self::File(file) => file.flush(),
            Self::Both(file, stdout) => {
                file.flush()?;
                stdout.flush()
            }
        }
    }
}

/// Runs on a blocking thread. Closing the queue drains records and flushes the final chunk.
pub fn write_output<W: Write>(sink: W, mut commands: mpsc::Receiver<Command>) -> Result<()> {
    let mut writer = AlignedSbeWriter::from_writer(sink, CHUNK_SIZE)?;
    while let Some(command) = commands.blocking_recv() {
        match command {
            Command::Records(records) => {
                for record in records {
                    match record {
                        Record::Snapshot(snapshot) => writer.write_msg(&snapshot)?,
                        Record::Update(update) => writer.write_msg(&update)?,
                    }
                }
            }
            Command::Flush => writer.flush()?,
        }
    }
    writer.flush()
}
