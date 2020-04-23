use super::KvsEngine;
use crate::errors::{KvsError, Result};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::io::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{ffi::OsStr, fs, io, path};
/// KvStore serves as the storage data structure for
/// our database.
#[derive(Clone)]
pub struct KvStore {
    mem_map: Arc<Mutex<BTreeMap<String, CommandPos>>>,
    path: Arc<path::PathBuf>,
    reader: KvReader,
    writer: Arc<Mutex<KvWriter>>,
}

struct KvReader {
    mem_map: Arc<Mutex<BTreeMap<String, CommandPos>>>,
    path: Arc<path::PathBuf>,
    readers: RefCell<HashMap<u64, io::BufReader<fs::File>>>,
    safe_point: Arc<AtomicU64>,
}

impl KvReader {
    fn read(&self, cmd_pos: &CommandPos) -> Result<Command> {
        self.remove_stale_files()?;
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.file_no) {
            readers.insert(
                cmd_pos.file_no,
                io::BufReader::new(fs::File::open(log_path(&self.path, cmd_pos.file_no))?),
            );
        }

        let reader = readers.get_mut(&cmd_pos.file_no).unwrap();
        reader.seek(io::SeekFrom::Start(cmd_pos.start))?;
        let cmd_reader = reader.take(cmd_pos.len);
        Ok(serde_json::from_reader(cmd_reader)?)
    }

    fn read_and_copy(
        &self,
        cmd_pos: &mut CommandPos,
        mut writer: &mut io::BufWriter<fs::File>,
    ) -> Result<u64> {
        self.remove_stale_files()?;
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.file_no) {
            readers.insert(
                cmd_pos.file_no,
                io::BufReader::new(fs::File::open(log_path(&self.path, cmd_pos.file_no))?),
            );
        }
        let reader = readers
            .get_mut(&cmd_pos.file_no)
            .expect("Couldn't find log reader");
        reader.seek(io::SeekFrom::Start(cmd_pos.start))?;

        let mut cmd_reader = reader.take(cmd_pos.len);
        let len = io::copy(&mut cmd_reader, &mut writer)?;
        Ok(len)
    }

    fn update_safe_point(&self, safe_point: u64) {
        self.safe_point.store(safe_point, Ordering::SeqCst);
    }

    fn remove_stale_files(&self) -> Result<()> {
        let stale_files: Vec<_> = self
            .readers
            .borrow()
            .keys()
            .filter(|&&file_no| file_no < self.safe_point.load(Ordering::SeqCst))
            .cloned()
            .collect();
        for stale_file in stale_files {
            self.readers.borrow_mut().remove(&stale_file);
            fs::remove_file(log_path(&self.path, stale_file))?;
        }
        Ok(())
    }
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader {
            mem_map: self.mem_map.clone(),
            path: self.path.clone(),
            readers: RefCell::new(HashMap::new()),
            safe_point: self.safe_point.clone(),
        }
    }
}

pub struct KvWriter {
    reader: KvReader,
    writer: io::BufWriter<fs::File>,
    current_file_no: u64,
    uncompacted_bytes: u64,
    path: Arc<path::PathBuf>,
    mem_map: Arc<Mutex<BTreeMap<String, CommandPos>>>,
}

impl KvWriter {
    fn write(&mut self, cmd: &Command) -> Result<()> {
        let pos = self.writer.seek(io::SeekFrom::Current(0))?;
        serde_json::to_writer(&mut self.writer, cmd)?;
        self.writer.flush()?;
        let new_pos = self.writer.seek(io::SeekFrom::Current(0))?;
        let cmd_pos = (self.current_file_no, pos, new_pos).into();
        match cmd {
            Command::Set(key, _) => {
                if let Some(old_cmd) = self.mem_map.lock().unwrap().insert(key.to_owned(), cmd_pos)
                {
                    self.uncompacted_bytes += old_cmd.len;
                }
            }
            Command::Rm(key) => {
                let old_cmd = self
                    .mem_map
                    .lock()
                    .unwrap()
                    .remove(&key.to_owned())
                    .expect("Key not found!");
                self.uncompacted_bytes += old_cmd.len;
            }
            _ => {}
        }
        // Initiate compaction ?
        if self.uncompacted_bytes > COMPACTION_THRESHOLD {
            self.compaction()?;
        }
        Ok(())
    }

    fn compaction(&mut self) -> Result<()> {
        let compaction_no = self.current_file_no + 1;
        self.current_file_no += 2;
        self.writer = new_db_file(&self.path, self.current_file_no, &self.reader)?;
        let mut compaction_writer = new_db_file(&self.path, compaction_no, &self.reader)?;

        let mut pos = 0;
        for cmd_pos in self.mem_map.lock().unwrap().values_mut() {
            let len = self.reader.read_and_copy(cmd_pos, &mut compaction_writer)?;
            *cmd_pos = (compaction_no, pos, pos + len).into();
            pos += len;
        }
        compaction_writer.flush()?;
        self.reader.update_safe_point(compaction_no);
        self.reader.remove_stale_files()?;
        self.uncompacted_bytes = 0;
        Ok(())
    }
}

const COMPACTION_THRESHOLD: u64 = 1024;

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key, value);
        self.writer.lock().unwrap().write(&cmd)?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.mem_map.lock().unwrap().get(&key) {
            if let Command::Set(_, value) = self.reader.read(cmd_pos)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandError)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let mem_map = self.mem_map.lock().unwrap();
        if mem_map.contains_key(&key) {
            let cmd = Command::Rm(key);
            self.writer.lock().unwrap().write(&cmd)?;
            Ok(())
        } else {
            Err(KvsError::NotFoundError(key))
        }
    }
}

impl KvStore {
    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<Self> {
        let path = path.to_path_buf();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::new();
        let mut mem_map = BTreeMap::new();
        let file_list = get_sorted_file_list(&path)?;
        let mut uncompacted_bytes = 0;

        for &file_no in &file_list {
            let mut reader = io::BufReader::new(fs::File::open(log_path(&path, file_no))?);
            uncompacted_bytes += intialise_mem_map(file_no, &mut reader, &mut mem_map)?;
            readers.insert(file_no, reader);
        }

        let current_file_no = file_list.last().unwrap_or(&0) + 1;
        let path = Arc::new(path);
        let mem_map = Arc::new(Mutex::new(mem_map));
        let readers = RefCell::new(readers);
        let mut kv_reader = KvReader {
            readers: readers,
            mem_map: mem_map.clone(),
            path: path.clone(),
            safe_point: Arc::new(AtomicU64::new(0)),
        };
        let buf_writer = new_db_file(&path, current_file_no, &mut kv_reader)?;
        let kv_writer = KvWriter {
            reader: kv_reader.clone(),
            writer: buf_writer,
            uncompacted_bytes,
            current_file_no,
            path: path.clone(),
            mem_map: mem_map.clone(),
        };
        let kv_store = KvStore {
            mem_map,
            path,
            reader: kv_reader,
            writer: Arc::new(Mutex::new(kv_writer)),
        };
        Ok(kv_store)
    }
}

fn intialise_mem_map(
    file_no: u64,
    reader: &mut io::BufReader<fs::File>,
    mem_map: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(io::SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted_bytes = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set(key, _) => {
                if let Some(old_cmd) = mem_map.insert(key, (file_no, pos, new_pos).into()) {
                    uncompacted_bytes += old_cmd.len;
                }
            }
            Command::Rm(key) => {
                if let Some(old_cmd) = mem_map.remove(&key) {
                    uncompacted_bytes += old_cmd.len;
                }

                uncompacted_bytes += new_pos - pos;
            }
            _ => {}
        }
        pos = new_pos;
    }
    Ok(uncompacted_bytes)
}

fn get_sorted_file_list(path: &path::Path) -> Result<Vec<u64>> {
    let mut file_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("db".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".db"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    file_list.sort();
    Ok(file_list)
}

fn log_path(path: &path::Path, file_no: u64) -> path::PathBuf {
    path.join(format!("{}.db", file_no))
}

fn new_db_file(
    path: &path::Path,
    file_no: u64,
    reader: &KvReader,
) -> Result<io::BufWriter<fs::File>> {
    let path = log_path(path, file_no);
    let writer = io::BufWriter::new(
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    );
    reader
        .readers
        .borrow_mut()
        .insert(file_no, io::BufReader::new(fs::File::open(&path)?));
    Ok(writer)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
    Get(String),
}

#[derive(Clone)]
struct CommandPos {
    file_no: u64,
    start: u64,
    len: u64,
}

impl From<(u64, u64, u64)> for CommandPos {
    fn from((file_no, pos, new_pos): (u64, u64, u64)) -> Self {
        CommandPos {
            file_no: file_no,
            start: pos,
            len: new_pos - pos,
        }
    }
}
