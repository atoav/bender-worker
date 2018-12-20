//! The work::taskmanagment module implements all task managment related methods\
//! for the Work Struct. This includes getting tasks, checking wheter it makes \
//! any sense to get tasks, selecting the next tasks and methods that alter primarily
//! the Task's states (e.g. finishing etc.)

use ::*;
use std::thread::sleep;
use std::time::Duration;
use amqp::Basic;
use bender_job::Task;
use work::blendfiles::format_duration;

impl Work{
    /// Returns true if a new task should be added. This depends on two factors:
    /// 1. the workload that self has set in the config
    /// 2. whether there is enough space left
    pub fn should_add(&self) -> bool{
        let okay_space = system::enough_space(&self.config.outpath, self.config.disklimit);
        if okay_space{
            eprintln!("{}", format!(" ❗ [WORKER] Warning: Taking no new jobs").black().on_yellow());
            system::print_space_warning(&self.config.outpath, self.config.disklimit);
            let timeout = Duration::from_secs(5);
            sleep(timeout);
        }
        self.tasks.iter()
                  .filter(|t| t.is_waiting() || t.is_queued())
                  .count() < self.config.workload
                      &&
                  !okay_space
                  
    }

    /// Listen in to work queue and get n messages (defined by the workload setting)
    /// Store these in a Option-wrapped Work struct (along with...)
    /// Reject these messages if our system is not fit to do work
    /// Acknowledge messages that are wonky
    pub fn get_tasks(&mut self, channel: &mut Channel){
        let mut remaining_delivery_tags = Vec::<u64>::new();

        // Get the next task from the work queue
        channel.basic_get("work", true)
               .take(1)
               .for_each(|message|{
                    match Task::deserialize_from_u8(&message.body){
                        Ok(mut t) => {
                            println!(" ✚ [WORKER] Received Task [{id}]", id=t.id);
                            
                            // Add Delivery tag to task data for later acknowledgement
                            t.add_data("task-delivery-tag", message.reply.delivery_tag.to_string().as_str());
                            
                            // Add this as a event to the tasks history
                            let h = format!("Task arrived at Worker [{}] with delivery tag {}", self.config.id, t.data.get("task-delivery-tag").unwrap());
                            self.add_history(h.as_str());
                            
                            // Add the newly modified Task to the queue
                            self.tasks.push(t);
                        },
                        Err(err) => {
                            eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't deserialize Task from message.body: {}", err).red());
                            // Always try to acknowledge received messages that couldn't be decoded
                            remaining_delivery_tags.push(message.reply.delivery_tag);
                        }
                    }
                });

        // Acknowledge all remaining wonky messages, that had their deserialization failed
        // to avoid the accumulation of garbage in the queue
        for tag in remaining_delivery_tags.iter(){
            if let Err(err) = channel.basic_ack(*tag, false){
                eprintln!("{}", format!(" ✖ [WORKER] Error: acknowledgment failed for received message: {}", err).red());
            }
        }
    }


    /// Returns true if there is at least one task
    pub fn has_task(&self) -> bool{
        self.tasks.len() > 0
    }

    /// Return all tasks that have a given parent id
    pub fn get_tasks_for_parent_id<S>(&self, id: S) -> Vec<&Task> where S: Into<String>{
        let id = id.into();
        self.tasks.iter()
                  .filter(|&task| task.parent_id == id)
                  .collect()
    }


    /// Returns a Reference to the next Task only if there is no Task running
    /// Only works on tasks with a constructed Command
    /// Doesn't affect the Tasks Status (use it too peek for next Task)
    pub fn next_task(&self) -> Option<&Task>{
        if !self.tasks.iter().any(|t| t.is_running()) {
            self.tasks.iter()
                      .filter(|t| t.command.is_constructed())
                      .find(|t| t.is_queued())
        }else{
            None
        }
    }

    /// Moves the next Task to self.current only if there is no Task running
    /// Only works on tasks with a constructed Command
    /// Sets the Tasks Status to Running
    pub fn queue_next_task(&mut self, channel: &mut Channel){
        // Only do this if there is no task running
        if !self.tasks.iter().any(|t| t.is_running()) && self.current.is_none() {
            let mut i = 0;
            let mut next = None;
            // Find the first task that:
            // - has a blendfile
            // - has a constructed command
            // - is queued
            // then remove this Task from the list and tore it in next
            while i != self.tasks.len() {
                if self.has_blendfile(&self.tasks[i]) &&
                   self.tasks[i].command.is_constructed() &&
                   self.tasks[i].is_queued() &&
                   next.is_none() {
                       next = Some(self.tasks.remove(i));
                } else {
                       i += 1;
                }
            }

            // Match the result of above find operation and assign it to
            // self.current only if there is an actual Task
            match next{
                Some(mut t) => {
                    t.start();
                    println!(" ✚ [WORKER] Queued task [{}] for job [{}]", t.id, t.parent_id);
                    self.display_divider = true;
                    let routing_key = format!("worker.{}", self.config.id);
                    match channel.post_task_info(&t, routing_key){
                        Ok(_) => (),
                        Err(err) => eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err).red())
                    }
                    self.current = Some(t);
                },
                None => () //println!("Debug: Queued none, there was no next...")
            }
        }else{
             //println!("Debug: didn't get a new task because the old is running");
        }
    }


    /// finish the current task and push it back to tasks
    pub fn finish_current(&mut self, channel: &mut Channel){
        let mut moved = false;
        // let c =  self.clone();
        if let Some(ref mut t) = self.current{
            t.finish();
            self.tasks.push(t.clone());

            // Ack the finished Task!
            // let deliver_tag = t.data.get("task-delivery-tag")
            //                         .clone()
            //                         .unwrap()
            //                         .parse::<u64>()
            //                         .unwrap();
            // if let Err(err) = channel.basic_ack(deliver_tag, false){
            //     eprintln!(" ✖ [WORKER] Error: Couldn't acknowledge task {} for job [{}]: {}", 
            //         t.command.short(), 
            //         t.parent_id,
            //         err);
            // }

            // Post the updated Task Info
            let routing_key = format!("worker.{}", self.config.id);
            if let Err(err) = channel.post_task_info(&t, routing_key){
                eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err).red())
            }

            moved = true;
            match self.blendfiles.get_mut(&t.parent_id){
                Some(mut opt_bf) => {
                    match opt_bf{
                        Some(ref mut bf) => {
                            bf.increment_frame();
                            let duration = bf.last_frame_duration().unwrap();
                            let average = bf.average_duration();
                            println!("{}", format!(" ✔️ [WORKER] Finished task [{task_id}] for job [{job_id}] (Duration: {duration}, Average Duration: {average})", 
                                task_id=t.id, 
                                job_id=t.parent_id,
                                duration=format_duration(duration),
                                average=format_duration(average)).green(),
                            );
                            self.display_divider = true;
                        },
                        None => eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't find Job with ID {} in self.blendfiles... This must be a bug!", t.parent_id).red())
                    }
                },
                None => eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't find Job with ID {} in self.blendfiles... This must be a bug!", t.parent_id).red())
            }
        }

        if moved{
            self.current = None;
            self.command = None;
        }
    }
    

    /// Errors the current task and push it back to tasks
    pub fn error_current<S>(&mut self, err: S, channel: &mut Channel) where S: Into<String>{
        let err = err.into();
        let mut moved = false;
        // let c =  self.clone();
        if let Some(ref mut t) = self.current{
            t.error();
            self.tasks.push(t.clone());
            moved = true;
            eprintln!("{}", format!(" ✖ [WORKER] Errored task [{}] for job [{}]: {}", t.id, t.parent_id, err).red());
            let routing_key = format!("worker.{}", self.config.id);
            match channel.post_task_info(&t, routing_key){
                Ok(_) => (),
                Err(err) => eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err).red())
            }
        }

        if moved{
            self.current = None;
            self.command = None;
        }
    }


    /// Adds the blendfiles paths to the task's data Hashmap for easier referencing
    pub fn add_paths_to_tasks(&mut self){
        let blendfiles = self.blendfiles.clone();
        self.tasks.iter_mut()
                  .filter(|task| !task.data.contains_key("blendfile"))
                  .for_each(|task|{
                      match blendfiles.get(&task.parent_id){
                          Some(blendfile) => {
                            match blendfile{
                                Some(bf) => task.add_data("blendfile", &bf.path.to_string_lossy()),
                                None => ()
                            }
                          },
                          None => ()
                      }
                  })
    }
}