use ::*;
use std::error::Error;
use std::fs;
use std::io::Read;

pub type GenError = Box<std::error::Error>;
pub type GenResult<T> = Result<T, GenError>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config{
    pub id: Uuid,
    pub disklimit: u64,
    pub workload: usize
}

impl Config{
    pub fn new() -> Self{
        Config{
            id: Uuid::new_v4(),         // Random UUID on start, then from disk
            disklimit: 200*1_000_000,   // In MB
            workload: 1                 // How many frames to take at once
        }
    }

    pub fn serialize(&self) -> Result<String, Box<Error>>{
        let s = serde_json::to_string_pretty(&self)?;
        Ok(s)
    }

    pub fn deserialize<S>(s: S) -> GenResult<Self> where S: Into<String> {
        let deserialized: Config = serde_json::from_str(&s.into()[..])?;
        Ok(deserialized)
    }

    /// Write a given Config to the file at path P
    pub fn to_file<P>(&self, p: P) -> GenResult<()> where P: Into<String> {
        let p = p.into();
        // Step 1: Serialize
        let serialized = self.serialize()?;
        // Step 2: Write
        fs::write(&p, serialized)?;
        Ok(())
    }

    /// Create a new Config from the given json file
    pub fn from_file<S>(p: S) -> GenResult<Self> where S: Into<PathBuf>{
        let p = p.into();
        let mut file = fs::File::open(p)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config = Self::deserialize(contents)?;
        Ok(config)
    }
}


/// Try to read the Config from the config folder or generate one if it doesn't\
/// exist and write it to disk
pub fn get_config<P>(p: P) -> GenResult<Config> where P: Into<PathBuf>{
    let mut p = p.into();
    let d = p.clone();
    p.push("config.json");
    match Path::new(&p).exists(){
        true => {
            println!("Reading the Configuration from:     {}", p.to_string_lossy());
            // Deserialize it from file
            let config = Config::from_file(&p)?;
            Ok(config)
        },
        false => {
            // No Config on disk. Create a new one and attempt to write it there
            println!("No Configuration found at \"{}\"", p.to_string_lossy());
            println!("Generating a new one");
            // Create directories on the way
            fs::create_dir_all(&d)?;
            // Get a new config
            let config = Config::new();
            // Write it to file
            config.to_file(p.to_string_lossy())?;
            Ok(config)
        }
    }
}