//! The config module defines the Config Struct as well as initialization \
//! methods to interactively create a configuration file. The format of choice \
//! is toml. The config will be stored at the path returned by the app_dirs crate.
//! On Linux this should be `~/.config/Bender-Worker/config.toml`
use ::*;
use std::error::Error;
use std::fs;
use std::io::Read;
use dialoguer::Input;
use std::process::Command;

// Default parameters
const BENDER_URL: &'static str = "http://0.0.0.0:5000";
const DISKLIMIT: u64 =           200*1_000_000;
const WORKLOAD: usize =          1;
const GRACE_PERIOD: u64 =        60;


pub type GenError = Box<std::error::Error>;
pub type GenResult<T> = Result<T, GenError>;


/// Holds the bender-worker's Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfig{
    pub bender_url: String,
    pub id: Uuid,
    pub blendpath: PathBuf,
    pub outpath: PathBuf,
    pub disklimit: u64,
    pub workload: usize,
    pub grace_period: u64,
    pub mode: Mode
}




impl WorkerConfig{
    /// Create a new configuration with default values
    pub fn new() -> Self{
        // Default for now.
        Self{
            bender_url:     BENDER_URL.to_string(),  // URL of the bender frontend
            id:             Uuid::new_v4(),          // Random UUID on start, then from disk
            blendpath:      PathBuf::new(),          // Path to where the blendfiles should be stored
            outpath:        PathBuf::new(),          // Path to where the rendered frames should be stored
            disklimit:      DISKLIMIT,               // In MB
            workload:       WORKLOAD,                // How many frames to take at once
            grace_period:   GRACE_PERIOD,            // How many seconds to keep blendfiles around before deletion
            mode:           Mode::Independent        // use server config or not
        }
    }

    /// Serialize the Configuration to TOML
    pub fn serialize(&self) -> Result<String, Box<Error>>{
        let s = toml::to_string_pretty(&self)?;
        Ok(s)
    }

    /// Serialize the Configuration from TOML
    pub fn deserialize<S>(s: S) -> GenResult<Self> where S: Into<String> {
        let deserialized: WorkerConfig = toml::from_str(&s.into()[..])?;
        Ok(deserialized)
    }

    /// Write a given WorkerConfig to the file at path
    pub fn to_file<P>(&self, p: P) -> GenResult<()> where P: Into<String> {
        let p = p.into();
        // Step 1: Serialize
        let serialized = self.serialize()?;
        // Step 2: Write
        fs::write(&p, serialized)?;
        Ok(())
    }

    /// Create a new WorkerConfig from the given toml file
    pub fn from_file<S>(p: S) -> GenResult<Self> where S: Into<PathBuf>{
        let p = p.into();
        let mut file = fs::File::open(&p)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config = Self::deserialize(contents)?;
        Ok(config)
    }

    /// Extract the relevant parts of a `bender_config::Config` and return a \
    /// WorkerConfig. Fill all missing fields with the default values
    pub fn from_serverconfig(config: bender_config::Config) -> Self{
        Self{
            bender_url:    BENDER_URL.to_string(),
            id:            config.worker.id,
            disklimit:     config.worker.disklimit,
            grace_period:  config.worker.grace_period,
            workload:      config.worker.workload,
            blendpath:     PathBuf::from(config.paths.blend()),
            outpath:       PathBuf::from(config.paths.frames()),
            mode:          Mode::Server
        }
    }
}

/// Defines the mode the application is running in
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Mode{
    Server,
    Independent
}




/// Run the interactive setup dialog for the blendpath
pub fn setup_blendpath<P>(config: &mut WorkerConfig, p: P) -> GenResult<()> where P: Into<PathBuf>{
    // Create the default path
    let mut p = p.into();
            p.push("blendfiles");
    let p = p.to_string_lossy().to_string();

    // Display a dialog
    let msg = "Where should the blendfiles be saved? (Press Enter for Default)";
    let blendpath: String = Input::new().with_prompt(msg)
                                        .default(p)
                                        .interact()?;

    config.blendpath = PathBuf::from(blendpath);

    fs::create_dir_all(&config.blendpath)?;

    Ok(())
}



/// Run the interactive setup dialog for the outpath, where the Frames should be saved
pub fn setup_outpath<P>(config: &mut WorkerConfig, p: P) -> GenResult<()> where P: Into<PathBuf>{
    // Create the default path
    let mut p = p.into();
            p.push("frames");
    let p = p.to_string_lossy().to_string();

    // Display a dialog
    let msg = "Where should the rendered Frames be saved? (Press Enter for Default)";
    let outpath: String = Input::new().with_prompt(msg)
                                        .default(p)
                                        .interact()?;

    config.outpath = PathBuf::from(outpath);

    fs::create_dir_all(&config.outpath)?;

    Ok(())
}



/// Figure out if there is a config for the server via command `bender-config path`
/// and return a working config, either in server mode or in independent mode
pub fn get_config<P>(p: P, args: &Args) -> GenResult<WorkerConfig> where P: Into<PathBuf>{
    let p = p.into();

    // Check if we have a bender-config (this indicates we are on a server)
    let configpath: Option<String> = match Command::new("bender-config")
                                        .arg("path")
                                        .output(){
        Ok(out)     =>  Some(String::from_utf8_lossy(&out.stdout).to_string()),
        Err(_err)   =>  None
    };

    // Try to get a serverconfig if there is one, otherwise get the worker config \
    // or generate a new one
    match configpath {
        Some(path) => {
            match bender_config::Config::from_file(path.as_str()){
                Ok(config) => {
                    scrnmsg(format!("Running in Server Mode. Using the config at {}", path.trim()));
                    Ok(WorkerConfig::from_serverconfig(config))
                },
                Err(err)   => {
                    errmsg(format!("Failed to read bender's config.toml from {}: {}", path.trim().bold(), err));
                    notemsg(format!("Attempting to use Workers own config at {} as a fallback", p.to_string_lossy().bold()));
                    let c = get_worker_config(p, args)?;
                    Ok(c)
                }
            }
        },
        None       => {
            scrnmsg(format!("Running in Independent Mode. Using the config at {}", p.to_string_lossy()));
            let c = get_worker_config(p, args)?;
            Ok(c)
        }
    }
}

/// Try to read the WorkerConfig from the config folder or generate one if it doesn't\
/// exist and write it to disk
pub fn get_worker_config<P>(p: P, args: &Args) -> GenResult<WorkerConfig> where P: Into<PathBuf>{
    let mut p = p.into();
    let d = p.clone();
    p.push("config.toml");
    match Path::new(&p).exists() && !args.flag_configure{
        true => {
            okmsg(format!("Reading the Configuration from:     {}", p.to_string_lossy().bold()));
            // Deserialize it from file
            let config = WorkerConfig::from_file(&p)?;
            Ok(config)
        },
        false => {
            // No WorkerConfig on disk. Create a new one and attempt to write it there
            if !args.flag_configure{
                notemsg(format!("No Configuration found at \"{}\"", p.to_string_lossy()));
                notemsg(format!("Generating a new one"));
            }
            // Create directories on the way
            fs::create_dir_all(&d)?;
            // Get a new config
            let mut config = WorkerConfig::new();
            // Ask the user where to save blendfilesfiles
            while let Err(e) = setup_blendpath(&mut config, &d){
                errmsg(format!("This is not a valid directory: {}", e));
            }
            // Ask the user where to save the rendered Frames
            while let Err(e) = setup_outpath(&mut config, &d){
                errmsg(format!("This is not a valid directory: {}", e));
            }

            // Write it to file
            config.to_file(p.to_string_lossy())?;
            Ok(config)
        }
    }
}


