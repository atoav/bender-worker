use ::*;



pub fn blender_in_path() -> bool{
    match process::Command::new("blender").arg("--version").output() {
        Ok(s) => {
            let blender_version = String::from_utf8_lossy(&s.stdout).to_string();
            scrnmsg(format!("Using Blender version: {}", blender_version.trim()));
            scrnmsg(" ".to_string());
            true
        },
        Err(e) => {
            if let std::io::ErrorKind::NotFound = e.kind() {
                eprintln!(" ✖ [WORKER] Blender is not installed or not in PATH environment variable: {}", e);
                false
            } else {
                eprintln!(" ✖ [WORKER] Blender --version returned Error: {}", e);
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
            space < (limit as f64 * 1e9) as u64
        },
        Err(err) => {
            eprintln!(" ✖ [WORKER] Error: Couldn't get available space for path \"{}\": {}", 
                p.to_string_lossy(),
                err);
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
            if  space < (limit as f64 * 1e9) as u64{
                redmsg(format!("❗ Warning: Space left on disk:        {:.*} GB (Limit: {:.*} GB)", 4, gigabytes.to_string(), 4, (limit as f64 * 1e9).to_string()));
            }else{
                scrnmsg(format!("Space left on disk:                 {:.*} GB (Limit: {:.*} GB)", 4, gigabytes.to_string(), 4, (limit as f64 * 1e9).to_string()));
            }
        },
        Err(err) => {
            eprintln!(" ✖ [WORKER] Error: Couldn't get available space: {}", err);
        }
    }
}
