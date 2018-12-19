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
extern crate bender_job;
extern crate bender_mq;
extern crate docopt;
extern crate colored;

use std::fs;
use colored::*;
use std::process;
use std::path::{PathBuf, Path};
use app_dirs::*;
use uuid::Uuid;
use docopt::Docopt;
use serde_derive::{Serialize, Deserialize};
use dialoguer::Confirmation;
use bender_mq::{Channel, BenderMQ};



pub mod config;
pub mod system;
pub mod work;
use work::*;
use config::Config;

const APP_INFO: AppInfo = AppInfo{name: "Bender-Worker", author: "David Huss"};

const USAGE: &'static str = "
bender-worker

Usage:
  bender-worker
  bender-worker --configure
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
  --force, -f   Don't ask for confirmation, just do it
  --configure   Run configuration
  -h --help     Show this screen.
  --version     Show version.
";

#[derive(Debug, Deserialize)]
pub struct Args {
    cmd_get: bool,
    cmd_configpath: bool,
    cmd_outpath: bool,
    cmd_blendpath: bool,
    cmd_benderurl: bool,
    cmd_id: bool,
    cmd_clean: bool,
    cmd_blendfiles: bool,
    cmd_frames: bool,
    flag_configure: bool,
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
                match Config::from_file(&configpath){
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
                match Config::from_file(&configpath){
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
                match Config::from_file(&configpath){
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
                match Config::from_file(&configpath){
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
                match Config::from_file(&configpath){
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


fn run(args: &Args) {
    // Get a valid application save path depending on the OS
    println!("\n{x} BENDER-WORKER {x}", x="=".repeat(24));
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't get application folder: {}", err).red()),
        Ok(app_savepath) => {
            println!("Storing Application Data in:        {}", app_savepath.to_string_lossy().replace("\"", "").bold());

            // Load the configuration (or generate one if we are a first timer)
            match config::get_config(&app_savepath, &args){
                Err(err) => {
                    let e = format!("{}", err);
                    if !e.contains("missing field"){
                        eprintln!("{}", format!(" ✖ Error: Couldn't generate/read config file: {}", err).red());
                    }else{
                        eprintln!("{}", format!(" ✖ Error: The existing configuration misses a field: {}", err).red());
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
                        eprintln!("{}", format!(" ✖ Error: Found no 'blender' command in the PATH. Make sure it is installed and in PATH environment variable").on_red());
                        process::exit(1);
                    }

                    if !config.outpath.exists(){
                        let mut configpath = app_savepath.clone();
                    configpath.push("config.toml");
                        eprintln!("{}", format!(" ✖ Error: the path specified as output path in {} does not exist or is not writeable!", configpath.to_string_lossy()).on_red());
                        println!("Please either create the path at {} or modify the config with bender-worker --configure", 
                            config.outpath.to_string_lossy() );
                        process::exit(1);
                    }
                    // We sucessfullt created a config file, let's go ahead
                    println!("This Worker has the ID:             [{}]", config.id);

                    // For now. TODO: discover this on the bender.hfbk.net
                    let url = "amqp://localhost//".to_string();
                    println!("Listening on for AMQP traffic at:   {}", url);
                    println!("Storing jobs at:                    {}", config.blendpath.to_string_lossy());
                    let mut channel = Channel::open_default_channel().expect(&format!("{}", format!(" ✖ [WORKER] Error: Couldn't aquire channel").red()));

                    // Declare a Work exchange
                    channel.create_work_queue().expect(&format!("{}", format!(" ✖ [WORKER] Error: Declaration of work queue failed").red()));

                    // Declare a topic exchange
                    channel.declare_topic_exchange().expect(&format!("{}", format!(" ✖ [WORKER] Error: Declaration of topic exchange failed").red()));
                    
                    // TODO APPSAVEPATH REINSPEICHERN

                    // Print the space left on the Worker Machine (at the path of the Application Data)
                    system::print_space_warning(Path::new(&app_savepath), config.disklimit);
                    println!();

                    // Create a empty message buffer for debouncing
                    // let mut info_messages = MessageBuffer::new();

                    // Buffer for delivery tags
                    // let mut delivery_tags = Vec::<u64>::new();
                    let mut pmessage = "".to_string();

                    let mut work = Work::new(config.clone());
                        

                    loop{
                        // -----------------------------------------------------
                        // 1. Clean up old stuff if necessary


                        // -----------------------------------------------------
                        // 2. Read Commands from the work queue and construct Work
                        //    with optimize_blend.py etc
                        work.update(&mut channel);


                        // -----------------------------------------------------
                        // 3. ACK all Commands that are done


                        // -----------------------------------------------------
                        // 4. Get files from Server or re-use stored ones


                        // -----------------------------------------------------
                        // 5. Execute Blender and Deal with the STDOUT


                        // -----------------------------------------------------
                        // 6. Upload stored Frames


                        // -----------------------------------------------------
                        // 7. Update Infos


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
