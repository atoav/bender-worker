use crate::*;
use config::WorkerConfig;
use bender_mq::BenderMQ;
use std::fs::DirBuilder;

#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;




/// Return the path to the place where the rendered frames will be stored
pub fn outpath(args: &Args){
    match get_paths(){
        (Some(a), Some(b)) => {
            match config::get_config(a, b, &args){
                Ok(config) => println!("{}", config.outpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (None, None) => {
            match config::get_config(PathBuf::from(""), PathBuf::from(""), &args){
                Ok(config) => println!("{}", config.outpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
}



/// Return the path to the place where the blendfiles will be stored
pub fn blendpath(args: &Args){
    match get_paths(){
        (Some(a), Some(b)) => {
            match config::get_config(a, b, &args){
                Ok(config) => println!("{}", config.blendpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (None, None) => {
            match config::get_config(PathBuf::from(""), PathBuf::from(""), &args){
                Ok(config) => println!("{}", config.blendpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
}




/// Return the workers id
pub fn id(args: &Args){
    match get_paths(){
        (Some(a), Some(b)) => {
            match config::get_config(a, b, &args){
                Ok(config) => println!("{}", config.id),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (None, None) => {
            match config::get_config(PathBuf::from(""), PathBuf::from(""), &args){
                Ok(config) => println!("{}", config.id),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
}



/// Return the URL of bender
pub fn benderurl(args: &Args){
    match get_paths(){
        (Some(a), Some(b)) => {
            match config::get_config(a, b, &args){
                Ok(config) => println!("{}", config.bender_url),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (None, None) => {
            match config::get_config(PathBuf::from(""), PathBuf::from(""), &args){
                Ok(config) => println!("{}", config.bender_url),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        },
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
}




/// Delete the workers files (runs either clean_gentle() or clean_force())
pub fn clean(args: &Args){
    match get_paths(){
        (Some(a), Some(b)) => {
            match config::get_config(&a, &b, &args){
                Ok(config) => {
                    if args.flag_force{
                        clean_force(args, &config);
                    } else {
                        clean_gentle(args, &config);
                    }
                },
                Err(err) => {eprintln!("{}", format!("Error: there was no config.toml at {path}: {err}", 
                        path=&a.to_string_lossy(),
                        err=err).red())}
            }
        },
        (None, None) => {
            match config::get_config(PathBuf::from(""), PathBuf::from(""), &args){
                Ok(config) => {
                    if args.flag_force{
                        clean_force(args, &config);
                    } else {
                        clean_gentle(args, &config);
                    }
                },
                Err(err) => {
                    eprintln!("{}", format!("Error: : {err}", 
                        err=err).red())
                }
            }
        },
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
}



/// Subcommand to delete all of the workers file witrh confirmation if there is a config
pub fn clean_gentle(args: &Args, config: &WorkerConfig) {
    if (!args.cmd_frames || args.cmd_blendfiles) && (config.mode.is_independent() || args.flag_on_server) {
        let p = config.blendpath.to_string_lossy().to_string();
        let msg = format!("{}", format!("Delete all files at {} ?", p).on_bright_red());
        if Confirmation::new().with_text(&msg).interact().unwrap(){
            delete_blendfiles(&config);
        }
    }

    if (!args.cmd_blendfiles || args.cmd_frames) && (config.mode.is_independent() || args.flag_on_server) {
        let p = config.outpath.to_string_lossy().to_string();
        let msg = format!("{}", format!("Delete all files at {} ?", p).on_bright_red());
        if Confirmation::new().with_text(&msg).interact().unwrap(){
            delete_framesfolder(&config);
        }
    }
}



/// Subcommand to delete all of the workers files by force (if there is a config)
pub fn clean_force(args: &Args, config: &WorkerConfig){
    // Delete Jobs
    if (!args.cmd_frames || args.cmd_blendfiles) && (config.mode.is_independent() || args.flag_on_server) {
        delete_blendfiles(&config);
    }
    // Delete Frames
    if (!args.cmd_blendfiles || args.cmd_frames) && (config.mode.is_independent() || args.flag_on_server) {
        delete_framesfolder(&config);
    }
}

/// Delete the contents of the blendfiles directory specified in the config. 
/// This only deletes files whose extension starts with .blend
fn delete_blendfiles(config: &WorkerConfig){
    let p = config.blendpath.clone();
    match fs::read_dir(&p){
        Ok(entries) => {
            for entry in entries {
                match entry{
                    Ok(e) => {
                        let path = e.path();
                        if path.is_file() && path.extension().is_some() && path.extension().unwrap().to_string_lossy().to_lowercase().starts_with("blend"){
                            match fs::remove_file(&path) {
                                Ok(_) => println!("{}", format!(" ✔ Deleted blendfile at {}", path.to_string_lossy()).green()),
                                Err(err) => eprintln!("{}", format!(" ✖ Error while deleting {}: {}", path.to_string_lossy(), err).red())
                            }
                        }
                    },
                    Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't read: {}", err).red())
                }
            }
        },
        Err(err) => eprintln!("{}", format!(" ✖ Error: Couldn't read \"{}\": {}", &p.to_string_lossy(), err).red())
    }
}


/// Delete the contents of the Frames directory specified in the config. 
/// Recreate with the given permissions
fn delete_framesfolder(config: &WorkerConfig){
    let p = &config.outpath;
    if p.is_dir(){
        match fs::remove_dir_all(&p){
            Ok(_) => {
                println!("{}", format!(" ✔ Deleted the contents of {}", p.to_string_lossy()).green());

                // Create frames directory with 775 permissions on Unix
                let mut builder = DirBuilder::new();

                // Set the permissions to 775
                #[cfg(unix)]
                builder.mode(0o2775);
                
                match builder.recursive(true).create(&p){
                    Ok(_) => println!("Recreated directory {} with permission 2775", &*p.to_string_lossy()),
                    Err(err) => eprintln!(" ✖ [WORKER] Error: Couldn't recreate Directory {}", err)
                } 
            },
            Err(err) => eprintln!("{}", format!(" ✖ Error while deleting in {}: {}", p.to_string_lossy(), err).red())
        } 
    }else{
        eprintln!("{}", format!(" ✖ Error: Couldn't read directory at {} because it wasn't a directory or didn't exist", p.to_string_lossy()));
    }
}


/// Get the paths to the configuration file and the user cache
pub fn get_paths() -> (Option<PathBuf>, Option<PathBuf>){
    let on_server = env::var("BENDERSERVER").is_ok();

    // Get a configpath for the application
    let app_configpath = match (get_app_dir(AppDataType::UserConfig, &APP_INFO, "/"), on_server){
        (Err(err), false) => {
            errmsg(format!("Couldn't find a suitable configuration folder for bender-worker: {}", err));
            std::process::exit(1);
        },
        (Ok(app_configpath), false) => {
            // Run this branch if the BENDERSERVER environment variable isn't set
            Some(app_configpath)
        },
        (_, true) => {
            // Run this branch if the BENDERSERVER environment variable is set
            // which means we are running as a systemd service
            None
        }
    };

    // Get a cache path for the application
    let app_cachepath = match (get_app_dir(AppDataType::UserCache, &APP_INFO, "/"), on_server){
        (Err(err), false) => {
            errmsg(format!("Couldn't find a suitable cache folder for bender-worker: {}", err));
            std::process::exit(1);
        },
        (Ok(app_cachepath), false) => {
            // Run this branch if the BENDERSERVER environment variable isn't set
            Some(app_cachepath)
        },
        (_, true) => {
            // Run this branch if the BENDERSERVER environment variable is set
            // which means we are running as a systemd service
            None
        }
    };

    (app_configpath, app_cachepath)

}



/// Initialize the worker
pub fn run(args: &Args) {
    // Get a valid application save path depending on the OS
    scrnmsg(format!("\n{x} BENDER-WORKER {x}", x="=".repeat((width()-15)/2)));

    if !system::blender_in_path(){
        errmsg("Found no 'blender' command in the PATH. Make sure it is installed and in PATH environment variable".to_string());
        process::exit(1);
    }
    
    match get_paths(){
        (Some(a), Some(b)) => run_worker(args, a, b),
        (None, None) => run_worker(args, PathBuf::from(""), PathBuf::from("")),
        (_, _) => panic!("This shouldn't have happened. This was meant to be an unreachable arm!")
    }
    
}



/// The program logic with loop etc.
pub fn run_worker(args: &Args, app_configpath: PathBuf, app_cachepath: PathBuf){
    // Load the configuration (or generate one if we are a first timer)
    match config::get_config(&app_configpath, &app_cachepath, &args){
        Err(err) => {
            let e = format!("{}", err);
            if !e.contains("missing field"){
                errmsg(format!("Couldn't generate/read config file: {}", err));
            }else{
                errmsg(format!("The existing configuration misses a field: {}", err));
                let msg = "Do you want to generate a new one? (this overrides the existing configuration)".on_red();
                if Confirmation::new().with_text(&msg).interact().unwrap(){
                    let mut p = PathBuf::from(&app_configpath);
                    p.push("config.toml");
                    fs::remove_file(&p).unwrap_or_else(|_| panic!("Error: Couldn't remove the file at {}\nPlease try to remove it manually", p.to_string_lossy()));
                    println!("{}", "Deleted the configuration file. Run worker again for a fresh new start".on_green());
                }
            }
        },
        Ok(config) => {
            if !config.outpath.exists(){
                let mut configpath = app_configpath.clone();
                configpath.push("config.toml");
                errmsg(format!("the path specified as output path in {} does not exist or is not writeable!", configpath.to_string_lossy()));
                println!("Please either create the path at {} or modify the config with bender-worker --configure", 
                    config.outpath.to_string_lossy() );
                process::exit(1);
            }
            scrnmsg(format!("Running in Independent Mode. Using the config at {}", app_configpath.to_string_lossy()));
            // We sucessfully created a config file, let's go ahead
            scrnmsg(format!("This Worker has the ID:             [{}]", config.id));

            // For now. TODO: discover this on the bender.hfbk.net
            let url = "amqp://localhost//".to_string();
            scrnmsg(format!("Listening on for AMQP traffic at:   {}", url));
            scrnmsg(format!("Storing jobs at:                    {}", config.blendpath.to_string_lossy()));
            let mut channel = Channel::open_default_channel()
                                      .unwrap_or_else(|_| panic!("{}", " ✖ [WORKER] Error: Couldn't aquire channel".to_string().red()));

            // Declare a Work exchange
            channel.create_work_queue()
                   .unwrap_or_else(|_| panic!("{}", " ✖ [WORKER] Error: Declaration of work queue failed".to_string().red()));

            // Declare a topic exchange
            channel.declare_worker_exchange()
                   .unwrap_or_else(|_| panic!("{}", " ✖ [WORKER] Error: Declaration of worker-topic exchange failed".to_string().red()));
            

            // Print the space left on the Worker Machine (at the path of the Application Data)
            system::print_space_warning(&config.outpath, config.disklimit);

            let mut work = Work::new(config.clone());

            scrnmsg("v".repeat(width()).to_string());
                

            loop{
                work.update(&mut channel);
            }
        }   
    }
}