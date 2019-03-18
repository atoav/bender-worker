//! The work module defines the Work Struct, which holds most of the current \
//! state of the program.

use ::*;
use blend::Blend;
use config::WorkerConfig;
use bender_job::{Task, History};
use bender_mq::BenderMQ;
use std::collections::HashMap;
use chrono::{Utc, DateTime};


// Import work submodules
pub mod commands;
pub mod blendfiles;
pub mod requests;
pub mod taskmanagment;
pub mod optimize;
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
    pub blendfiles: HashMap<String, Blend>,
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
            blendfiles: HashMap::<String, Blend>::new(),
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
        // self.print_self("After get_tasks()");

        // Update each unique parent job status for all the Tasks
        self.update_parent_job_status();
        // self.print_self("After update_parent_job_status()");

        // Get the blendfile from the server only if there are 
        // tasks that actually need one
        self.get_blendfiles();
        // self.print_self("After get_blendfiles()");
        
        // Construct Commands for Tasks that have a matching blendfile on \
        // disk and whose commands are not constructed yet
        self.construct_commands();
        // self.print_self("After construct_commands()");

        // Optimize Blendfiles for local consumtion
        self.optimize_blendfiles();

        // Update who the current Task is ("self.current")
        self.select_next_task(channel);
        // self.print_self("After select_next_task()");

        // Dispatch a Command for the current Task ("self.current")
        self.run_command(channel);
        // self.print_self("After run_command()");

        // Get the filesize and hash for the rendered frames of a Task
        self.stat_finished(channel);
        // self.print_self("After stat_finished()");

        // Upload the finished files
        self.upload_finished(channel);
        // self.print_self("After upload_finished()");

        // Cleanup finished blendfiles
        self.cleanup_blendfiles();
        // self.print_self("After cleanup_blendfiles()");

        // Cleanup rendered and uploaded frames
        self.cleanup_frames();
        // self.print_self("After cleanup_frames()");

        // Send a heart beat to the qu to signal you are alive
        self.beat_heart(channel);

        // Don't spin out of control if there are no Tasks
        self.sleep();
    }  



    #[allow(dead_code)]
    fn print_self<S>(&self, note: S) where S:Into<String>{
        if self.has_task(){
            let note = note.into();
            let current = match self.current{
                Some(ref current) => format_task(&current),
                None => "None".to_string()
            };

            let p: Vec<String> = self.parent_jobs.iter()
                                    .map(|(id, status)| format!("[{}]: {}", &id[..6], &status[..]))
                                    .collect();
        
            println!("\n");
            for pjob in p.iter(){
                println!("        {}", pjob.yellow());
            }
            println!("\n");
            println!("        {}", note.yellow());
            println!("        {}", "─────────────────────────────────┬────────────────────────────".yellow());
            println!("        {}", format!(" Tasks ({})                       │ Current", self.tasks.len()).yellow());
            println!("        {}", "─────────────────────────────────┼────────────────────────────".yellow());
            if self.tasks.is_empty(){
                println!("        {}", format!("                                │ {current}", 
                            current=current
                            ).yellow());
            }else{
                for (i, task) in self.tasks.iter().enumerate(){
                    if i == 0{
                        println!("        {}", format!(" {task}     │ {current}", 
                            task=format_task(&task),
                            current=current
                            ).yellow());
                    }else{
                        println!("        {}", format!(" {task}     │ ", 
                            task=format_task(&task)
                            ).yellow());
                    }
                }
            }
            println!("        {}", "─────────────────────────────────┴────────────────────────────".yellow());
            println!("\n");

            if p.is_empty() && self.tasks.len() > 2{
                std::process::exit(1);
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


fn format_task(task: &Task) -> String{
    // [######][Frame 1]
    format!("[{task_id}][{short}] {status}", 
                    task_id=&task.id[..6],
                    short=task.command.short(),
                    status=format!("{:<9}", format!("{:?}",task.status).replace("\"", "")))
}