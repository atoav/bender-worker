//! It is a multi-plattform client for the bender-renderfarm. It receives it's \
//! tasks via amqp/rabbitmq, requests blendfiles from flaskbender \
//! via http GET, renders the Tasks and stores the rendered Frames on disk.
//!
//! ##
//! You can configure it via `bender-worker --configure`. If you want to see what \
//! else is possible (besides just running it) check `bender-worker -h`
//! 
//! ## Life of a task
//! 1. Task is received via `work`-queue from rabbitmq, the delivery-tags get stored because the Tasks will only be ACK'd once they are done
//! 2. The command stored in the Task gets constructed. This means the "abstract" paths stored insided the command get replaced with paths configured in the bender-worker (e.g. for reading blendfiles, or storing rendered frames)
//! 3. Once constructed bender-worker generate a unique set of parent (Job) IDs, because it is likely that multiple tasks belong to the same job. For each unique ID a asynchronous http request to flaskbender is made, and the blend will be downloaded
//! 4. Once the Task has a blendfile it gets dispatched asynchronously
//! 5. Once the Task is done its delivery-tag gets ACK'd, the Task finished and the next Task will be selected
//! 6. After a grace period the downloaded blendfile gets deleted if flaskbender says the job has actually been done
//! 7. Inbetween all these steps the Task gets transmitted to bender-bookkeeper for housekeeping
//! 

extern crate app_dirs;
extern crate serde;
extern crate fs2;
extern crate serde_derive;
extern crate uuid;
extern crate amqp;
extern crate chrono;
extern crate hyper;
extern crate itertools;
extern crate dialoguer;
extern crate shlex;
extern crate toml;
extern crate docopt;
extern crate colored;
extern crate console;
extern crate reqwest;

extern crate bender_job;
extern crate bender_mq;
extern crate bender_config;


use std::env;
use std::fs;
use colored::*;
use std::process;
use std::path::{PathBuf, Path};
use app_dirs::*;
use uuid::Uuid;
use docopt::Docopt;
use serde_derive::{Serialize, Deserialize};
use dialoguer::Confirmation;
use console::Term;

use bender_mq::Channel;


pub mod system;

pub mod config;

pub mod work;
use work::*;

const APP_INFO: AppInfo = AppInfo{name: "Bender-Worker", author: "David Huss"};

const USAGE: &str = "
bender-worker

The bender-worker is a multi-plattform client for the bender-renderfarm. It \
receives it's tasks via amqp/rabbitmq, requests blendfiles from flaskbender \
via http GET, renders the Tasks and stores the rendered Frames on disk.

Usage:
  bender-worker
  bender-worker --configure [--local]
  bender-worker --independent
  bender-worker clean [--force]
  bender-worker clean blendfiles [--force]
  bender-worker clean frames [--force]
  bender-worker get outpath
  bender-worker get blendpath
  bender-worker get id
  bender-worker get benderurl
  bender-worker (-h | --help)
  bender-worker --version

Options:
  --force, -f         Don't ask for confirmation, just do it
  --configure         Run configuration
  --independent, -i   Run local
  -h --help           Show this screen.
  --version           Show version.
";

#[derive(Debug, Deserialize)]
pub struct Args {
    flag_configure: bool,
    flag_independent: bool,
    cmd_get: bool,
    cmd_outpath: bool,
    cmd_blendpath: bool,
    cmd_benderurl: bool,
    cmd_id: bool,
    cmd_clean: bool,
    cmd_blendfiles: bool,
    cmd_frames: bool,
    flag_force: bool,
}


mod command;


fn main(){
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.deserialize())
                            .unwrap_or_else(|e| e.exit());

    let benderserver = env::var("BENDERSERVER").is_ok();

    // Read the config (if there is one) and get the path for frames
    if args.cmd_get && args.cmd_outpath{
        command::outpath(&args);
    // Read the config (if there is one) and get the path for blendfiles
    }else if args.cmd_get && args.cmd_blendpath{
        command::blendpath(&args);
    // Read the config (if there is one) and get the workers id
    }else if args.cmd_get && args.cmd_id{
        command::id(&args);
    // Read the config (if there is one) and get the bender url
    }else if args.cmd_get && args.cmd_benderurl{
        command::benderurl(&args);
    // Read the config (if there is one) and get the bender url
    }else if args.cmd_clean{
        command::clean(&args);
    }else{
        command::run(&args, benderserver);
    }
}





/// Return the width of the terminal
fn width() -> usize{
    let term = Term::stdout();
    term.size().1 as usize
}

/// A fancy error message
pub fn errmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " Error ".on_red().bold();
    eprintln!("    {} {}", label, s);
}

/// A fancy ok message
pub fn okmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = "  OK  ".on_green().bold();
    println!("    {} {}", label, s)
}

/// A fancy note message
pub fn notemsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = "  NOTE  ".on_yellow().bold();
    println!("    {} {}", label, s)
}

pub fn errrun<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " ✖ Error: ".red();
    eprintln!("{}{}", label, s);
}

pub fn okrun<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " ✔️ [WORKER] ".green();
    println!("{}{}", label, s);
}

pub fn scrnmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let subs = s.as_bytes()
                .chunks(width())
                .map(std::str::from_utf8)
                .filter(|l| l.is_ok())
                .map(|l| l.unwrap())
                .map(|line| format!("{}{}", line, " ".repeat(width()-line.len())))
                .collect::<Vec<String>>()
                .join("\n");
    println!("{}", subs.black().on_white());
}

pub fn redmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let subs = s.as_bytes()
                .chunks(width())
                .map(std::str::from_utf8)
                .filter(|l| l.is_ok())
                .map(|l| l.unwrap())
                .map(|line| format!("{}{}", line, " ".repeat(width()-line.len())))
                .collect::<Vec<String>>()
                .join("\n");
    println!("{}", subs.black().on_red());
}