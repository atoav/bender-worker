//! The config module defines the Config Struct as well as initialization \
//! methods to interactively create a configuration file. The format of choice \
//! is toml. The config will be stored at the path returned by the app_dirs crate.
//! On Linux this should be `~/.config/Bender-Worker/config.toml`
use ::*;
use std::error::Error;
use std::fs;
use std::io::Read;
use dialoguer::Input;




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
    pub grace_period: u64
}




impl WorkerConfig{
    /// Create a new configuration with default values
    pub fn new() -> Self{
        // Default for now.
        let bender_url = "http://0.0.0.0:5000".to_string();
        Self{
            bender_url: bender_url,           // URL of the bender frontend
            id: Uuid::new_v4(),               // Random UUID on start, then from disk
            blendpath: PathBuf::new(),        // Path to where the blendfiles should be stored
            outpath: PathBuf::new(),          // Path to where the rendered frames should be stored
            disklimit: 200*1_000_000,         // In MB
            workload: 1,                      // How many frames to take at once
            grace_period: 60                  // How many seconds to keep blendfiles around before deletion
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

    /// Write a given WorkerConfig to the file at path P
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



/// Try to read the WorkerConfig from the config folder or generate one if it doesn't\
/// exist and write it to disk
pub fn get_config<P>(p: P, args: &Args) -> GenResult<WorkerConfig> where P: Into<PathBuf>{
    let mut p = p.into();
    let d = p.clone();
    p.push("config.toml");
    match Path::new(&p).exists() && !args.flag_configure{
        true => {
            println!("Reading the Configuration from:     {}", p.to_string_lossy());
            // Deserialize it from file
            let config = WorkerConfig::from_file(&p)?;
            Ok(config)
        },
        false => {
            // No WorkerConfig on disk. Create a new one and attempt to write it there
            if !args.flag_configure{
                println!("No Configuration found at \"{}\"", p.to_string_lossy());
                println!("Generating a new one");
            }
            // Create directories on the way
            fs::create_dir_all(&d)?;
            // Get a new config
            let mut config = WorkerConfig::new();
            // Ask the user where to save blendfilesfiles
            while let Err(e) = setup_blendpath(&mut config, &d){
                println!("ERROR: This is not a valid directory: {}", e);
            }
            // Ask the user where to save the rendered Frames
            while let Err(e) = setup_outpath(&mut config, &d){
                println!("ERROR: This is not a valid directory: {}", e);
            }

            // Write it to file
            config.to_file(p.to_string_lossy())?;
            Ok(config)
        }
    }
}
