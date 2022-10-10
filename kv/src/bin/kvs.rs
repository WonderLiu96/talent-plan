// extern crate clap;
// use clap::{App, Arg, SubCommand};
use kv::{KvStore, Result};
use std::{env::current_dir, process::exit};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name=env!("CARGO_PKG_NAME"),
            version=env!("CARGO_PKG_VERSION"),
            author=env!("CARGO_PKG_AUTHORS"),
            about=env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}
#[derive(Debug, StructOpt)]
enum Command {
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
    },
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", help = "The string value of the key")]
        value: String,
    },
    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
    },
}
fn main() -> Result<()> {
    let opt = Opt::from_args();
    match opt.command {
        Command::Get { key } => {
            let mut store = KvStore::open(current_dir()?)?;

            match store.get(key) {
                Ok(Some(value)) => {
                    println!("{}", value);
                }
                Ok(None) | Err(_) => {
                    println!("Key not found");
                }
            };
        }
        Command::Set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        Command::Remove { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(()) => {}
                Err(kv::KvError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            };
        }
    }
    Ok(())
}
