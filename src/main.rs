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

use std::fs;
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

const APP_INFO: AppInfo = AppInfo{name: "Bender-Worker", author: "David Huss"};

const USAGE: &'static str = "
bender-worker

Usage:
  bender-worker [--configure]
  bender-worker config path
  bender-worker (-h | --help)
  bender-worker --version

Options:
  --configure   Run configuration
  -h --help     Show this screen.
  --version     Show version.
";

#[derive(Debug, Deserialize)]
pub struct Args {
    cmd_config: bool,
    flag_configure: bool,
    cmd_path: bool,
}

fn main(){
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.deserialize())
                            .unwrap_or_else(|e| e.exit());

    if args.cmd_config && args.cmd_path{
        // Print just the path of the application folder
        match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(err) => eprintln!("ERROR: Couldn't get application folder: {}", err),
            Ok(app_savepath) => {
                let mut p = app_savepath.clone();
                p.push("config.toml");
                println!("{}", p.to_string_lossy());
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
        Err(err) => println!("ERROR: Couldn't get application folder: {}", err),
        Ok(app_savepath) => {
            println!("Storing Application Data in:        {}", app_savepath.to_string_lossy().replace("\"", ""));

            // Load the configuration (or generate one if we are a first timer)
            match config::get_config(&app_savepath, &args){
                Err(err) => {
                    let e = format!("{}", err);
                    if !e.contains("missing field"){
                        println!("ERROR: Couldn't generate/read config file: {}", err);
                    }else{
                        println!("ERROR: The existing configuration misses a field: {}", err);
                        let msg = "Do you want to generate a new one? (this overrides the existing configuration)";
                        if Confirmation::new().with_text(msg).interact().unwrap(){
                            let mut p = PathBuf::from(&app_savepath);
                            p.push("config.toml");
                            fs::remove_file(&p).expect(format!("Error: Couldn't remove the file at {}\nPlease try to remove it manually", p.to_string_lossy()).as_str());
                            println!("Deleted the configuration file. Run worker again for a fresh new start");
                        }
                    }

                },
                Ok(config) => {
                    if !system::blender_in_path(){
                        process::exit(1);
                    }
                    // We sucessfullt created a config file, let's go ahead
                    println!("This Worker has the ID:             [{}]", config.id);

                    // For now. TODO: discover this on the bender.hfbk.net
                    let url = "amqp://localhost//".to_string();
                    println!("Listening on for AMQP traffic at:   {}", url);
                    println!("Storing jobs at:                    {}", config.blendpath.to_string_lossy());
                    let mut channel = Channel::open_default_channel().expect(" ✖ [WORKER] Error: Couldn't aquire channel");

                    // Declare a Work exchange
                    channel.create_work_queue().expect(" ✖ [WORKER] Error: Declaration of work queue failed");

                    // Declare a topic exchange
                    channel.declare_topic_exchange().expect(" ✖ [WORKER] Error: Declaration of topic exchange failed");
                    
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
