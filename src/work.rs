//! The work module defines the Work Struct, which holds most of the current \
//! state of the program.

use ::*;
use config::WorkerConfig;
use bender_job::{Task, History};
use bender_mq::BenderMQ;
use std::collections::HashMap;
use chrono::Utc;
use chrono::DateTime;
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
    pub config: WorkerConfig,
    pub tasks: Vec<Task>,
    pub current: Option<Task>,
    pub history: History,
    pub blendfiles: HashMap<String, Option<Blendfile>>,
    pub parent_jobs: HashMap<String, String>,
    command: Option<std::process::Child>,
    display_divider: bool,
    last_heartbeat: Option<DateTime<Utc>>
}




impl Work{




    /// Create a new task with a given config
    pub fn new(config: WorkerConfig) -> Self{
        Work{
            config,
            tasks: Vec::<Task>::new(),
            current: None,
            history: History::new(),
            blendfiles: HashMap::<String, Option<Blendfile>>::new(),
            parent_jobs: HashMap::<String, String>::new(),
            command: None,
            display_divider: true,
            last_heartbeat: None
        }
    }


    /// Add to the Work-History
    pub fn add_history<S>(&mut self, value: S) where S: Into<String> {
        self.history.insert(Utc::now(), value.into());
    }




    /// Runs every loop and updates everything. This is the meat of the \
    /// business logic for the worker.
    pub fn update(&mut self, channel: &mut Channel){
        // Add new tasks only if we don't exceed the number of tasks definied \
        // in the workload setting
        if self.should_add(){
            self.get_tasks(channel);
        }

        // Update the parent jobs statuses
        if self.has_task() && self.config.mode.is_independent() {
            self.fetch_parent_jobs_stati();
        }else if self.config.mode.is_server(){
            self.read_parent_jobs_stati();
        }

        // Get the blendfile from the server only if there are 
        // tasks that actually need one
        if self.has_task() || !self.tasks.iter().all(|t| t.is_ended()){
            if self.config.mode.is_independent() {
                self.fetch_blendfiles();
            }else{
                self.read_blendfiles();
            }
            self.add_paths_to_tasks();
        }

        if self.has_task() && !self.all_jobs_finished() {
            // Construct Commands for Tasks that have a matching blendfile on \
            // disk and whose commands are not constructed yet
            self.construct_commands();

            // Update who the current Task is ("self.current")
            self.select_next_task(channel);
        }

        // Dispatch a Command for the current Task ("self.current")
        self.run_command(channel);

        // Figure out if a blendfile's tasks are all finished. If so request \
        // a job status from flaskbender. If the job has finished and a \
        // certain grace period has passed, delete the blendfile in question.
        // Don't do this when running in server mode, because bender-janitor \
        // will manage this
        if self.has_task() && !self.all_jobs_finished()
        && self.any_job_finished() && !self.config.mode.is_server(){
            self.cleanup_blendfiles();
        }

        // Print a divider in debug mode
        if cfg!(debug_assertions) && self.has_task() && !self.all_jobs_finished() {
            self.print_divider();
        }

        self.beat_heart(channel);
    }  




    /// Print a horizontal divider if the flag is set \
    /// and reset the flag afterwards
    fn print_divider(&mut self) {
        if self.display_divider {
            println!("{}", "-".repeat(width()));
            self.display_divider = false;
            let a = self.tasks.iter().count();
            let f = self.tasks.iter().filter(|t| t.is_finished()).count();
            let q = self.tasks.iter().filter(|t| t.is_queued()).count();
            let w = self.tasks.iter().filter(|t| t.is_waiting()).count();
            let cur = self.current.is_some();
            if cfg!(debug_assertions) {
                eprintln!("All: {}    Finished: {}    Queued: {}     Waiting: {}    Current: {}", a,  f, q, w, cur);
            }
        }
    }




    /// Send a heartbeat message to bender-worker via rabbitmq as a life sign
    /// The heartbeat is rate limited and will only beat if the specified has \
    /// passed
    fn beat_heart(&mut self, channel: &mut Channel) {
        // Determine whether the heart should beat
        let should_beat = match self.last_heartbeat{
            Some(time) => {
                let delta = Utc::now() - time;
                delta > chrono::Duration::seconds(self.config.heart_rate_seconds as i64)
            },
            None => true
        };

        // Update the heartbeat only if there was none in the last n seconds
        // or it was the first one.
        if should_beat{
            let routing_key = format!("heart.{}", self.config.id);
            channel.worker_post(routing_key, Vec::new());
            self.last_heartbeat = Some(Utc::now());
        }
    }



}
