use crate::*;
use config::WorkerConfig;
use bender_mq::BenderMQ;




pub fn outpath(args: &Args){
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red()),
        Ok(app_savepath) => {
            match config::get_config(&app_savepath, &args){
                Ok(config) => println!("{}", config.outpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        }
    }
}

pub fn blendpath(args: &Args){
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(_err) => (), // Couldn't get app_savepath
        Ok(app_savepath) => {
            match config::get_config(&app_savepath, &args){
                Ok(config) => println!("{}", config.blendpath.to_string_lossy()),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        }
    }
}


pub fn id(args: &Args) {
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(_err) => (), // Couldn't get app_savepath
        Ok(app_savepath) => {
            match config::get_config(&app_savepath, &args){
                Ok(config) => println!("{}", config.id),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        }
    }
}

pub fn benderurl(args: &Args) {
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
        Err(_err) => (), // Couldn't get app_savepath
        Ok(app_savepath) => {
            match config::get_config(&app_savepath, &args){
                Ok(config) => println!("{}", config.bender_url),
                Err(err) => eprintln!("{}", format!(" ✖ Error: {}", err).red())
            }
        }
    }
}


pub fn clean(args: &Args) {
    match get_app_root(AppDataType::UserConfig, &APP_INFO){
            Err(_err) => (), // Couldn't get app_savepath
            Ok(app_savepath) => {
                match config::get_config(&app_savepath, &args){
                    Ok(config) => {
                        if args.flag_force{
                            force_clean(&args, &config);
                        }else{
                            if !args.cmd_frames || args.cmd_blendfiles {
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

                            if !args.cmd_blendfiles || args.cmd_frames {
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
}


pub fn force_clean(args: &Args, config: &WorkerConfig){
    if !args.cmd_frames || args.cmd_blendfiles {
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

    if !args.cmd_blendfiles || args.cmd_frames {
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
}



pub fn run(args: &Args) {
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
                            fs::remove_file(&p).unwrap_or_else(|_| panic!("Error: Couldn't remove the file at {}\nPlease try to remove it manually", p.to_string_lossy()));
                            println!("{}", "Deleted the configuration file. Run worker again for a fresh new start".on_green());
                        }
                    }
                },
                Ok(config) => {
                    if !system::blender_in_path(){
                        errmsg("Found no 'blender' command in the PATH. Make sure it is installed and in PATH environment variable".to_string());
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

                    // Create a empty message buffer for debouncing
                    // let mut info_messages = MessageBuffer::new();

                    // Buffer for delivery tags
                    // let mut delivery_tags = Vec::<u64>::new();
                    let mut pmessage = "".to_string();

                    let mut work = Work::new(config.clone());

                    scrnmsg("v".repeat(width()).to_string());
                        

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