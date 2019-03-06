#![allow(unused_imports)]
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
use work::*;

use bender_mq::Channel;

pub mod system;

pub mod config;

pub mod work;

pub mod main;

pub mod command;



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