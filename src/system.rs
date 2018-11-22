use ::*;



pub fn enough_space<P>(p: P, limit: u64) -> bool where P: Into<PathBuf>{
    let p = p.into();
    match fs2::available_space(&p){
        Ok(space) => {
            // println!("Space available: {}", space);
            space < limit
        },
        Err(err) => {
            println!("ERROR: Couldn't get available space: {}", err);
            false
        }
    }
}

pub fn print_space_warning<P>(p: P, limit: u64) where P: Into<PathBuf>{
    let p = p.into();
    match fs2::available_space(&p){
        Ok(space) => {
            let gigabytes = space as f64/1_000_000_000.0;
            if  space < limit{
                println!("Warning: Space left on disk:        {:.*} GB", 4, gigabytes.to_string());
            }else{
                println!("Space left on disk:                 {:.*} GB", 4, gigabytes.to_string());
            }
        },
        Err(err) => {
            println!("ERROR: Couldn't get available space: {}", err);
        }
    }
}
