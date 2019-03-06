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

use work::*;
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