use ::*;
use std::collections::HashMap;
use bender_config::GenResult;
use bender_job::Task;
use blend::Blend;
use std::io;
use std::process::Command;
use bender_job::common::tempfile::NamedTempFile;

#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;





static OPTIMIZE_PY: &'static str = include_str!("optimize.py");


impl Work{

    /// Returns true if the Tasks Blend Variant is optimized
    pub fn blendfile_is_optimized(&self, t: &Task) -> bool{
        match self.blendfiles.get(&t.parent_id) {
            Some(blend) => blend.is_optimized(),
            None => false
        }
    }

    /// Returns true if the Tasks Blend Variant is downloaded
    pub fn blendfile_is_downloaded(&self, t: &Task) -> bool{
        match self.blendfiles.get(&t.parent_id) {
            Some(blend) => blend.is_downloaded(),
            None => false
        }
    }

    /// Returns true if the Tasks Blend Variant is none
    pub fn blendfile_is_none(&self, t: &Task) -> bool{
        match self.blendfiles.get(&t.parent_id) {
            Some(blend) => blend.is_none(),
            None => true
        }
    }

    /// Filter all Blendfiles that are downloaded
    pub fn optimize_blendfiles(&mut self){
        if self.has_task() && !self.all_jobs_finished(){

            // Generate a hashmap with optimized blends
            let h: HashMap<String, Blend> = 
            self.blendfiles.iter_mut()
                           .filter(|(_, blend)|{
                                match blend{
                                    Blend::Downloaded(_) => true,
                                    _ => false
                                }
                           })
                           .map(|(id, blend)|{
                                let path = blend.clone().unwrap().path;
                                match optimize(path){
                                    Ok(_) => Some((id.clone(), Blend::Optimized(blend.clone().unwrap()))),
                                    Err(err) => {
                                        errrun(format!("Couldn't optimize blendfile for job [{}]: {}", &id[..6], err));
                                        None
                                    }
                                }
                           })
                           .filter(|a| a.is_some())
                           .map(|a| a.unwrap())
                           .collect();

            // Extend
            self.blendfiles.extend(h.into_iter());
        }
    }

}


/// Execute the jobs blendfile with optimize_blend.py, gather data and optimize settings.
fn optimize(blendpath: PathBuf) -> GenResult<()>{
    if Path::new(&blendpath).exists(){
        // Run Blend with Python
        match NamedTempFile::new(){
            Ok(mut tempfile) => {
                match io::copy(&mut OPTIMIZE_PY.as_bytes(), &mut tempfile){
                    Ok(_) => {
                        let path = tempfile.path();
                        match run_with_python(&*blendpath.to_string_lossy(), &path.to_string_lossy()){
                            Ok(_)    => Ok(()),
                            Err(err) => Err(From::from(format!("Error while running with optimize.py: {}",  err)))
                        }

                    },
                    Err(err) => Err(From::from(format!("Error: Failed to copy optimize.py to tempfile: {}", err)))
                }
            },
            Err(err) => Err(From::from(format!("Error: Couldn't create tempfile: {}", err)))
        }
    }else{
        Err(From::from(format!("Didn't find blendfile at {}", blendpath.to_string_lossy())))
    }
}


/// Execute the checked blend-file at blend_path with the python file at python_path
/// The final command will look something like this:
/// ```text
/// blender -b myfile.blend --disable-autoexec --python path/to/optimize_blend.py
/// ```
fn run_with_python<S>(path: S, pythonpath: S) -> GenResult<String>where S: Into<String>{
    let path = path.into();
    let pythonpath = pythonpath.into();

    // Pass variables as environment variables, let blender run optimize_blend.py
    // to set some things straight and save a new file
    // blender -b / --disable-autoexec --python /usr/local/lib/optimize_blend.py

    println!("blender -b {}", path);
    let command = Command::new("blender")
            .arg("-b")
            .arg(&path)
            .arg("--disable-autoexec")
            .arg("--python")
            .arg(pythonpath)
            .spawn()?
            .wait_with_output()?;

    // Collect all lines starting with "{" for JSON
    let output: String = String::from_utf8(command.stdout.clone())?
        .lines()
        .filter(|line|line.trim().starts_with('{'))
        .collect();

    // Error on empty string
    if output == "" { 
        Err(From::from(String::from_utf8(command.stdout).unwrap())) 
    } else {
        // Set permissions
        match fs::metadata(&path){
            Ok(meta) => {
                // Set the permissions to 775
                let mut permissions = meta.permissions();
                if !cfg!(windows){
                    permissions.set_mode(0o775);
                }
                match fs::set_permissions(&path, permissions){
                    Ok(_) => (),
                    Err(err) => eprintln!("Error: failed to set permissions to 775: {}", err)
                }
            },
            Err(err) => eprintln!("Error: Failed to get file metadata: {}", err)
        }
        Ok(output)
    }
}