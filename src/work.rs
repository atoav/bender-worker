//! The work module defines the Work Struct, which holds most of the current \
//! state of the program.

use ::*;
use config::Config;
use bender_job::Task;
use bender_job::History;
use std::collections::HashMap;
use chrono::Utc;
use work::blendfiles::Blendfile;


// Import work submodules
pub mod commands;
pub mod blendfiles;
pub mod requests;
pub mod taskmanagment;




/// The Work struct holds the current configuration, tasks structs, as well as a\
/// history and a Hashmap with blendfiles.
#[derive(Debug)]
pub struct Work{
    pub config: Config,
    pub tasks: Vec<Task>,
    pub current: Option<Task>,
    pub history: History,
    pub blendfiles: HashMap<String, Option<Blendfile>>,
    command: Option<std::process::Child>,
    display_divider: bool
}




impl Work{
    /// Create a new task with a given config
    pub fn new(config: Config) -> Self{
        Work{
            config: config,
            tasks: Vec::<Task>::new(),
            current: None,
            history: History::new(),
            blendfiles: HashMap::<String, Option<Blendfile>>::new(),
            command: None,
            display_divider: true
        }
    }


    /// Add to the Work-History
    pub fn add_history<S>(&mut self, value: S) where S: Into<String> {
        self.history.insert(Utc::now(), value.into());
    }


    /// Runs every loop and updates everything. This is the meat of the business\
    /// logic for the worker.
    pub fn update(&mut self, channel: &mut Channel){
        // Add new tasks only if we don't exceed the number of tasks definied in \
        // the workload setting
        if self.should_add(){
            self.get_tasks(channel);
        }

        // Get the blendfile from the server only if there are 
        // tasks that actually need one
        if self.has_task(){
            self.get_blendfiles();
            self.add_paths_to_tasks();
        }

        // Construct Commands for Tasks that have a matching blendfile on disk \
        // and whoose commands are not constructed yet
        self.construct_commands();

        // Update who the current Task is ("self.current")
        self.queue_next_task(channel);

        // Dispatch a Command for the current Task ("self.current")
        self.run_command(channel);

        // Figure out if a blendfile's tasks are all finished. If so request the\
        // job status from flaskbender. If the job has finished and a certain grace\
        // period has passed, delete the blendfile in question

        // Print a divider
        if self.has_task(){
            self.print_divider();
        }
    }  


    /// Print a horizontal divider if the flag is set and reset the flag afterwards
    fn print_divider(&mut self) {
        if self.display_divider {
            println!("{}", "-".repeat(width()));
            self.display_divider = false;
        }
    }
}





