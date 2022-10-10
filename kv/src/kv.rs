use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{OpenOptions};
use std::io::{BufRead, BufReader, Write};

use std::path::PathBuf;

use crate::{KvError, Result};

pub struct KvStore {
    map: HashMap<String, String>,
    path: PathBuf,
}
impl KvStore {
    /// Open a 'KvStore' with given path.
    ///
    /// This wiil create a new file if the given one is not exist.
    ///
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path = path.into();
        path.push("log.txt");

        let mut store = KvStore {
            map: HashMap::new(),
            path: path,
        };
        // load data
        store.load_data()?;

        Ok(store)
    }
    pub fn load_data(&mut self) -> Result<()> {
        let path = self.path.as_os_str();
        let file = OpenOptions::new()
                            .create(true)
                            .read(true)
                            .append(true)
                            .open(path)?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let cmd: Command = serde_json::from_str(&line.unwrap()).unwrap();

            match cmd {
                Command::Set { key, value } => {
                    self.map.insert(key, value);
                }
                Command::Remove { key } => {
                    self.map.remove(&key);
                }
            }
        }
        Ok({})
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.clone(), value.clone());

        let cmd = Command::set(key, value);
        let path = self.path.as_os_str();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let serialized_str = serde_json::to_string(&cmd)?;
        let serialized_str = serialized_str.as_bytes();
        file.write(serialized_str)?;
        file.write("\n".as_bytes())?;
        Ok({})
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // Get value by giving key.
        if self.map.contains_key(&key) == false {
            Ok(None)
        } else {
            let value = self.map.get(&key).unwrap();
            Ok(Some(value.to_string()))
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            self.map.remove(&key);
            let cmd = Command::Remove { key };
            let path = self.path.as_os_str();
            let mut file = OpenOptions::new().create(true).append(true).open(path)?;
            let serialized_str = serde_json::to_string(&cmd)?;
            let serialized_str = serialized_str.as_bytes();
            file.write(serialized_str)?;
            file.write("\n".as_bytes())?;

            Ok({})
        } else {
            Err(KvError::KeyNotFound)
        }
    }
}
// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }
    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}
struct CommandPos {
    pos: u32,
    len: u32,
}
