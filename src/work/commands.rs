//! The work::command module implements methods related to the construction, \
//! spawning and processing of actual Commands (as defind in `bender_job::Command`)

use ::*;
use std::thread::sleep;
use std::process::{Stdio};
use std::io::{BufRead, BufReader};
use std::process::Command;
use std::time::Duration;
// use console::{Term, measure_text_width};







impl Work{
    // Construct the commands
    pub fn construct_commands(&mut self){
        // copy the data of tasks
        let mut data = std::mem::replace(&mut self.tasks, vec![]);
        // mutate over it
        data.iter_mut()
            .filter(|task| task.is_queued())
            .filter(|task| !task.command.is_constructed())
            .filter(|task| task.data.contains_key("blendfile"))
               .for_each(|task|{
                // we can unwrap this because, the key "blendfile" only exists
                   // if there is a value
                let p = task.data.get("blendfile").unwrap().clone();
                let mut out = self.config.outpath.clone();
                out.push(task.parent_id.as_str());
                if !out.exists(){
                    match fs::create_dir(&out){
                        Ok(_) => (),
                        Err(err) => eprintln!(" ✖ [WORKER] Error: Couldn't create Directory {}", err)
                    }  
                }
                if out.exists(){
                    let outstr = out.clone().to_string_lossy().to_string();
                    task.construct(p, outstr);
                    self.display_divider = true;
                    match task.command{
                        bender_job::Command::Blender(ref c) => println!(" ✚ [WORKER] Constructed task for frame [{}]", c.frame),
                        _ => println!(" ✚ [WORKER] Constructed generic task [{}]", task.id)
                    }
                }
            });
        // put it pack
        std::mem::replace(&mut self.tasks, data);
    }


    /// Spawn constructed commands and either error or finish the current Task. \
    /// This method is meant to run in the update loop, that means it behaves \
    /// differently depending on wheter there is a current task or command or not.
    //
    pub fn run_command(&mut self, channel: &mut Channel){
        let exitstatus = match self{
            // When there is no command but a current task, create a command and spawn it
            Work{command: None, current: Some(task), ..} => {
                // If there is no command create one
                if task.command.is_blender(){
                    // Replace only first "blender " in command string
                    let s = task.command.to_string().unwrap().replacen("blender ", "", 1);
                    match shlex::split(&s){
                        Some(args) => {
                            match Command::new("blender")
                                                   .args(args.clone())
                                                   .stdout(Stdio::piped())
                                                   .stderr(Stdio::piped())
                                                   .spawn(){
                                Ok(c) => {
                                    println!(" ⚟ [WORKER] Dispatched Command: \"blender {}\"", args.join(" "));
                                    self.command = Some(c);
                                    
                                    ExitStatus::Running
                                },
                                Err(err) => ExitStatus::Errored(
                                    format!(" ✖ [WORKER] Error: Couldn't spawn Command with args: {:?}. Error was: {}", args, err))
                            }
                        },
                        None => ExitStatus::Errored(format!(" ✖ [WORKER] Error: Couldn't split arguments for command: {:?}", task.command))
                    }
                }else{
                    ExitStatus::None
                }
            },
            // when there is a command and a current task wait for the command to finish
            Work{command: Some(ref mut child), current:Some(_task), ..} => {
                let timeout = Duration::from_secs(1);
                sleep(timeout);
                match child.try_wait() {
                    Ok(Some(status)) if status.success() => {
                        ExitStatus::Finished
                    },
                    Ok(Some(status))  => ExitStatus::Errored(format!(" ✖ [WORKER] Error: Command returned with status: {:?}", status)),
                    Ok(None) => {
                        process_stdout(child);
                        ExitStatus::Running
                    },
                    Err(err) => 
                        ExitStatus::Errored(format!(" ✖ [WORKER] Error: waiting for spawned Command: {}", err)),
                }
            },
            // Everything else
            _ => ExitStatus::None
        };

        // println!("Debug: We have a exitstatus of: {:?}", exitstatus);

        match exitstatus{
            ExitStatus::None => (),
            ExitStatus::Running => {
                match self.current{
                    Some(ref mut c) if !c.is_running() => {
                        c.start();
                        // println!("Debug: Started Task because it wasn't running");
                    },
                    _ => ()
                }
            },
            ExitStatus::Errored(err) => self.error_current(err, channel),
            ExitStatus::Finished => self.finish_current(channel)
        }
    }
}



/// Holds the Exit Status of sapwned commands
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExitStatus{
    Finished,
    Errored(String),
    Running,
    None
}



/// Process the stdout of spawned commands
pub fn process_stdout(child:&mut std::process::Child){
    match child.stdout{
        Some(ref mut stdout) => {
            let reader = BufReader::new(stdout);
            reader.lines()
                  .filter_map(|line| line.ok())
                  .filter(|line| line.trim() != "")
                  .for_each(|line| {
                    let _message = format!("   [WORKER][COMMAND] {}", line).dimmed();
                    
                    // let term = Term::stdout();
                    // let w = term.size().1 as usize;
                    // let lines = measure_text_width(&message.to_string()) / w;
                    // let _ = term.clear_last_lines(lines+1);
                    
                    // println!("{}", message);
                  });
        },
        None => eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't get a stdout").red())
    }
}