use ::*;



pub fn blender_in_path() -> bool{
    match process::Command::new("blender").arg("--version").status() {
    Ok(_) => true,
    Err(e) => {
        if let std::io::ErrorKind::NotFound = e.kind() {
            println!(" ✖ [WORKER] Blender is not installed or not in PATH environment variable: {}", e);
            false
        } else {
            println!(" ✖ [WORKER] Blender --version returned Error: {}", e);
            false
        }
    }, 
}
}



/// Check whether enough space is left on the disk
pub fn enough_space<P>(p: P, limit: u64) -> bool where P: Into<PathBuf>{
    let p = p.into();
    match fs2::available_space(&p){
        Ok(space) => {
            // println!("Space available: {}", space);
            space < limit
        },
        Err(err) => {
            println!(" ✖ [WORKER] Error: Couldn't get available space: {}", err);
            false
        }
    }
}



/// Print a warning if there is not enough space left on disk
pub fn print_space_warning<P>(p: P, limit: u64) where P: Into<PathBuf>{
    let p = p.into();
    match fs2::available_space(&p){
        Ok(space) => {
            let gigabytes = space as f64/1_000_000_000.0;
            if  space < limit{
                println!("❗ Warning: Space left on disk:        {:.*} GB", 4, gigabytes.to_string());
            }else{
                println!("Space left on disk:                 {:.*} GB", 4, gigabytes.to_string());
            }
        },
        Err(err) => {
            println!(" ✖ [WORKER] Error: Couldn't get available space: {}", err);
        }
    }
}
