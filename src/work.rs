//! The work module defines the Work Struct, which holds most of the current \
//! state of the program.

use ::*;
use config::WorkerConfig;
use bender_job::{Task, History};
use bender_mq::BenderMQ;
use std::collections::HashMap;
use chrono::{Utc, DateTime};
use work::blendfiles::Blendfile;


// Import work submodules
pub mod commands;
pub mod blendfiles;
pub mod requests;
pub mod taskmanagment;
pub mod ratelimit;

use ratelimit::RateLimiter;




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
    last_heartbeat: Option<DateTime<Utc>>,
    last_status: RateLimiter,
    last_upload: RateLimiter
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
            last_heartbeat: None,
            last_status: RateLimiter::new(),
            last_upload: RateLimiter::new()
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
        self.get_tasks(channel);
        // dbg!(&self);

        // Update each unique parent job status for all the Tasks
        self.update_parent_job_status();

        // Get the blendfile from the server only if there are 
        // tasks that actually need one
        self.get_blendfiles();
        // dbg!(&self);
        // std::process::exit(1);
        
        // Construct Commands for Tasks that have a matching blendfile on \
        // disk and whose commands are not constructed yet
        self.construct_commands();

        // Update who the current Task is ("self.current")
        self.select_next_task(channel);

        // Dispatch a Command for the current Task ("self.current")
        self.run_command(channel);

        // Get the filesize and hash for the rendered frames of a Task
        self.stat_finished(channel);

        // Upload the finished files
        self.upload_finished(channel);

        // Cleanup finished and uploaded blendfiles
        self.cleanup_blendfiles();

        // Send a heart beat to the qu to signal you are alive
        self.beat_heart(channel);
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
