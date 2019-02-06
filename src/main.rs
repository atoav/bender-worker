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

use bender_mq::{Channel, BenderMQ};


pub mod system;

pub mod config;
use config::WorkerConfig;

pub mod work;
use work::*;

const APP_INFO: AppInfo = AppInfo{name: "Bender-Worker", author: "David Huss"};

const USAGE: &'static str = "
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
  bender-worker get configpath
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
    cmd_configpath: bool,
    cmd_outpath: bool,
    cmd_blendpath: bool,
    cmd_benderurl: bool,
    cmd_id: bool,
    cmd_clean: bool,
    cmd_blendfiles: bool,
    cmd_frames: bool,
    flag_force: bool,
}




fn main(){
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.deserialize())
                            .unwrap_or_else(|e| e.exit());


    if args.cmd_get && args.cmd_configpath{
        // Print just the path of the application folder
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(err) => eprintln!("{}", format!(" ✖ Error: : Couldn't get application folder: {}", err).red()),
            Ok(app_savepath) => {
                let mut p = app_savepath.clone();
                p.push("config.toml");
                println!("{}", p.to_string_lossy());
            }
        }
    // Read the config (if there is one) and get the path for frames
    }else if args.cmd_get && args.cmd_outpath{
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red()),
            Ok(app_savepath) => {
                let mut configpath = app_savepath.clone();
                configpath.push("config.toml");
                match WorkerConfig::from_file(&configpath){
                    Ok(config) => println!("{}", config.outpath.to_string_lossy()),
                    Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
                }
            }
        }
    // Read the config (if there is one) and get the path for blendfiles
    }else if args.cmd_get && args.cmd_blendpath{
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(_err) => (), // Couldn't get app_savepath
            Ok(app_savepath) => {
                let mut configpath = app_savepath.clone();
                configpath.push("config.toml");
                match WorkerConfig::from_file(&configpath){
                    Ok(config) => println!("{}", config.blendpath.to_string_lossy()),
                    Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
                }
            }
        }
    // Read the config (if there is one) and get the workers id
    }else if args.cmd_get && args.cmd_id{
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(_err) => (), // Couldn't get app_savepath
            Ok(app_savepath) => {
                let mut configpath = app_savepath.clone();
                configpath.push("config.toml");
                match WorkerConfig::from_file(&configpath){
                    Ok(config) => println!("{}", config.id),
                    Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
                }
            }
        }
    // Read the config (if there is one) and get the bender url
    }else if args.cmd_get && args.cmd_benderurl{
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(_err) => (), // Couldn't get app_savepath
            Ok(app_savepath) => {
                let mut configpath = app_savepath.clone();
                configpath.push("config.toml");
                match WorkerConfig::from_file(&configpath){
                    Ok(config) => println!("{}", config.bender_url),
                    Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
                }
            }
        }
    // Read the config (if there is one) and get the bender url
    }else if args.cmd_clean{
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(_err) => (), // Couldn't get app_savepath
            Ok(app_savepath) => {
                let mut configpath = app_savepath.clone();
                configpath.push("config.toml");
                match WorkerConfig::from_file(&configpath){
                    Ok(config) => {
                        if args.flag_force{
                            if args.cmd_blendfiles || (!args.cmd_blendfiles && !args.cmd_frames) {
                                let p = config.blendpath.to_string_lossy().to_string();
                                match fs::remove_dir_all(&p){
                                    Ok(_) => {
                                        println!("{}", format!(" ✔ Deleted the contents of {}", p).green());
                                        match fs::create_dir_all(&p){
                                            Ok(_) => (),
                                            Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't recreate directory: {}", err).red())
                                        }
                                    },
                                    Err(err) => eprintln!("{}", format!(" ✖ Error while deleting in {}: {}", p, err).red())
                                }
                            }

                            if args.cmd_frames || (!args.cmd_blendfiles && !args.cmd_frames) {
                                let p = config.outpath.to_string_lossy().to_string();
                                match fs::remove_dir_all(&p){
                                    Ok(_) => {
                                        println!("{}", format!(" ✔ Deleted the contents of {}", p).green());
                                        match fs::create_dir_all(&p){
                                            Ok(_) => (),
                                            Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't recreate directory: {}", err).red())
                                        }
                                    },
                                    Err(err) => eprintln!("{}", format!(" ✖ Error while deleting in {}: {}", p, err).red())
                                }
                            }
                        }else{
                            if args.cmd_blendfiles || (!args.cmd_blendfiles && !args.cmd_frames) {
                                let p = config.blendpath.to_string_lossy().to_string();
                                let msg = format!("{}", format!("Delete all files at {} ?", p).on_bright_red());
                                if Confirmation::new().with_text(&msg).interact().unwrap(){
                                    match fs::remove_dir_all(&p){
                                        Ok(_) => {
                                            println!("{}", format!(" ✔ Deleted the contents of {}", p).green());
                                            match fs::create_dir_all(&p){
                                                Ok(_) => (),
                                                Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't recreate directory: {}", err).red())
                                            }
                                        },
                                        Err(err) => eprintln!("{}", format!(" ✖ Error while deleting in {}: {}", p, err).red())
                                    }
                                }
                            }

                            if args.cmd_frames || (!args.cmd_blendfiles && !args.cmd_frames) {
                                let p = config.outpath.to_string_lossy().to_string();
                                let msg = format!("{}", format!("Delete all files at {} ?", p).on_bright_red());
                                if Confirmation::new().with_text(&msg).interact().unwrap(){
                                    match fs::remove_dir_all(&p){
                                        Ok(_) => {
                                            println!("{}", format!(" ✔ Deleted the contents of {}", p).green());
                                            match fs::create_dir_all(&p){
                                                Ok(_) => (),
                                                Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't recreate directory: {}", err).red())
                                            }
                                        },
                                        Err(err) => eprintln!("{}", format!(" ✖ Error while deleting in {}: {}", p, err).red())
                                    }
                                }
                            }
                            
                        }
                    },
                    Err(err) => eprintln!("{}", format!("Error: there was no config.toml at {path}: {err}", 
                        path=&app_savepath.to_string_lossy(),
                        err=err).red())
                }
            }
        }
    }else{
        run(&args);
    }
}




/// Return the width of the terminal
fn width() -> usize{
    let term = Term::stdout();
    term.size().1 as usize
}




fn run(args: &Args) {
    // Get a valid application save path depending on the OS
    scrnmsg(format!("\n{x} BENDER-WORKER {x}", x="=".repeat((width()-15)/2)));
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(err) => errmsg(format!("Couldn't get application folder: {}", err)),
        Ok(app_savepath) => {
            // Load the configuration (or generate one if we are a first timer)
            match config::get_config(&app_savepath, &args){
                Err(err) => {
                    let e = format!("{}", err);
                    if !e.contains("missing field"){
                        errmsg(format!("Couldn't generate/read config file: {}", err));
                    }else{
                        errmsg(format!("The existing configuration misses a field: {}", err));
                        let msg = "Do you want to generate a new one? (this overrides the existing configuration)".on_red();
                        if Confirmation::new().with_text(&msg).interact().unwrap(){
                            let mut p = PathBuf::from(&app_savepath);
                            p.push("config.toml");
                            fs::remove_file(&p).expect(format!("Error: Couldn't remove the file at {}\nPlease try to remove it manually", p.to_string_lossy()).as_str());
                            println!("{}", "Deleted the configuration file. Run worker again for a fresh new start".on_green());
                        }
                    }

                },
                Ok(config) => {
                    if !system::blender_in_path(){
                        errmsg(format!("Found no 'blender' command in the PATH. Make sure it is installed and in PATH environment variable"));
                        process::exit(1);
                    }

                    if !config.outpath.exists(){
                        let mut configpath = app_savepath.clone();
                    configpath.push("config.toml");
                        errmsg(format!("the path specified as output path in {} does not exist or is not writeable!", configpath.to_string_lossy()));
                        println!("Please either create the path at {} or modify the config with bender-worker --configure", 
                            config.outpath.to_string_lossy() );
                        process::exit(1);
                    }
                    // We sucessfullt created a config file, let's go ahead
                    scrnmsg(format!("This Worker has the ID:             [{}]", config.id));

                    // For now. TODO: discover this on the bender.hfbk.net
                    let url = "amqp://localhost//".to_string();
                    scrnmsg(format!("Listening on for AMQP traffic at:   {}", url));
                    scrnmsg(format!("Storing jobs at:                    {}", config.blendpath.to_string_lossy()));
                    let mut channel = Channel::open_default_channel().expect(&format!("{}", format!(" ✖ [WORKER] Error: Couldn't aquire channel").red()));

                    // Declare a Work exchange
                    channel.create_work_queue().expect(&format!("{}", format!(" ✖ [WORKER] Error: Declaration of work queue failed").red()));

                    // Declare a topic exchange
                    channel.declare_topic_exchange().expect(&format!("{}", format!(" ✖ [WORKER] Error: Declaration of topic exchange failed").red()));
                    
                    // TODO APPSAVEPATH REINSPEICHERN

                    // Print the space left on the Worker Machine (at the path of the Application Data)
                    system::print_space_warning(&config.outpath, config.disklimit);

                    // Create a empty message buffer for debouncing
                    // let mut info_messages = MessageBuffer::new();

                    // Buffer for delivery tags
                    // let mut delivery_tags = Vec::<u64>::new();
                    let mut pmessage = "".to_string();

                    let mut work = Work::new(config.clone());

                    scrnmsg(format!("{}", "v".repeat(width())));
                        

                    loop{
                        work.update(&mut channel);

                        // Debounced Message handling
                        let message = "".to_string();

                        if message != pmessage{
                            println!("{}", message);
                            pmessage = message;
                        }
                    }
                }   
            }
        }
        
    }
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