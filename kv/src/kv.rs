use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crate::{KvError, Result};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStore {
    dir_path: PathBuf,
    current_id: u64,
    index: HashMap<String, CommandPos>,
    readers: HashMap<u64, BufReaderWithPos<File>>,
    curren_writer: BufWriterWithPos<File>,
    uncompacted: u64,
}
impl KvStore {
    /// Open a 'KvStore' with given path.
    ///
    /// This wiil create a new file if the given one is not exist.
    ///
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        fs::create_dir_all(&dir_path)?;

        let mut index = HashMap::new();
        let mut readers = HashMap::new();

        // generate id for every log file in given directory.
        let id_list = Self::generate_id(&dir_path)?;
        let mut uncompacted = 0;

        for &id in &id_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&dir_path, id))?)?;
            uncompacted += Self::recover(id, &mut reader, &mut index)?;
            readers.insert(id, reader);
        }
        let current_id = id_list.last().unwrap_or(&0) + 1;
        let writer = Self::new_log_file(&dir_path, current_id, &mut readers)?;

        Ok(KvStore {
            dir_path: dir_path,
            current_id: current_id,
            index: index,
            readers: readers,
            curren_writer: writer,
            uncompacted: uncompacted,
        })
    }
    fn recover(
        id: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &mut HashMap<String, CommandPos>,
    ) -> Result<u64> {
        // ready to read data
        let mut pos = reader.seek(SeekFrom::Current(0))?;
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
        let mut uncompacted = 0;

        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    if let Some(old_cmd) = index.insert(
                        key,
                        CommandPos {
                            file_id: id,
                            pos: pos,
                            len: new_pos - pos,
                        },
                    ) {
                        uncompacted += old_cmd.len;
                    }
                }
                Command::Remove { key } => {
                    if let Some(old_cmd) = index.remove(&key) {
                        uncompacted += old_cmd.len;
                    }
                    uncompacted += new_pos - pos;
                }
            };
            pos = new_pos;
        }
        Ok(uncompacted)
    }
    fn generate_id(path: &Path) -> Result<Vec<u64>> {
        // Get file key
        let mut id_list: Vec<u64> = fs::read_dir(&path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".log"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();

        id_list.sort_unstable();
        Ok(id_list)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key,
            value: value,
        };
        let pos = self.curren_writer.pos;
        serde_json::to_writer(&mut self.curren_writer, &cmd)?;
        self.curren_writer.flush()?;

        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.insert(
                key,
                CommandPos {
                    file_id: self.current_id,
                    pos: pos,
                    len: self.curren_writer.pos - pos,
                },
            ) {
                self.uncompacted += old_cmd.len;
            }
        };

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok({})
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.file_id)
                .expect("cann't find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key: key };
            serde_json::to_writer(&mut self.curren_writer, &cmd)?;
            self.curren_writer.flush()?;

            if let Command::Remove { key } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
    pub fn compact(&mut self) -> Result<()> {
        let compaction_id = self.current_id + 1;
        self.current_id += 2;
        self.curren_writer =
            Self::new_log_file(&self.dir_path, self.current_id, &mut self.readers)?;

        let mut compaction_writer =
            Self::new_log_file(&self.dir_path, compaction_id, &mut self.readers)?;
        let mut new_pos = 0;
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.file_id)
                .expect("Cann't find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = std::io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = CommandPos {
                file_id: compaction_id,
                pos: new_pos,
                len: len,
            };
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_files: Vec<_> = self
            .readers
            .keys()
            .filter(|&&id| id < compaction_id)
            .cloned()
            .collect();
        for stale_file in stale_files {
            self.readers.remove(&stale_file);
            std::fs::remove_file(log_path(&self.dir_path, stale_file))?;
        }
        self.uncompacted = 0;

        Ok(())
    }
    fn new_log_file(
        path: &Path,
        key: u64,
        readers: &mut HashMap<u64, BufReaderWithPos<File>>,
    ) -> Result<BufWriterWithPos<File>> {
        let path = log_path(path, key);
        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        readers.insert(key, BufReaderWithPos::new(File::open(&path)?)?);
        Ok(writer)
    }
}

// Generate log file by giving dirPath.
fn log_path(dir: &Path, key: u64) -> PathBuf {
    dir.join(format!("{}.log", key))
}
// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
// Record <key,value> pair position in diffrent files.
struct CommandPos {
    file_id: u64,
    pos: u64,
    len: u64,
}
// ReaderBufWithPos
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}
impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos: pos,
        })
    }
}
impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;

        Ok(len)
    }
}
impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
// WriterBufWithPos
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}
impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos: pos,
        })
    }
}
impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
