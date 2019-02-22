//! The work::taskmanagment module implements all task managment related methods\
//! for the Work Struct. This includes getting tasks, checking wheter it makes \
//! any sense to get tasks, selecting the next tasks and methods that alter primarily
//! the Task's states (e.g. finishing etc.)

use ::*;
use std::thread::sleep;
use std::time::Duration;
use amqp::Basic;
use bender_job::Task;
use bender_mq::BenderMQ;
use work::blendfiles::format_duration;




impl Work{
    /// Returns true if a new task should be added. This depends on two factors:
    /// 1. the workload that self has set in the config
    /// 2. whether there is enough space left
    pub fn should_add(&self) -> bool{
        // Return early if ther isn't enough space
        if !system::enough_space(&self.config.outpath, self.config.disklimit){
            eprintln!("{}", " ❗ [WORKER] Warning: Taking no new jobs".to_string().black().on_yellow());
            system::print_space_warning(&self.config.outpath, self.config.disklimit);
            let timeout = Duration::from_secs(5);
            sleep(timeout);
            false
        }else{
            // Do not add new tasks if we have reached the workload defined in Tasks
            let active_task_count = self.tasks.iter()
                                         .filter(|t| !t.is_ended())
                                         .count();
            active_task_count < self.config.workload
        }

        // let self.unique_parent_ids()
        // && self.job_is_finished(t.parent_id)
    }

    /// Listen in to work queue and get n messages (defined by the workload setting)
    /// Store these in a Option-wrapped Work struct (along with...)
    /// Reject these messages if our system is not fit to do work
    /// Acknowledge messages that are wonky
    pub fn get_tasks(&mut self, channel: &mut Channel){
        let mut remaining_delivery_tags = Vec::<u64>::new();

        // Get the next task from the work queue
        channel.basic_get("work", false)
               .take(1)
               .for_each(|message|{
                    match Task::deserialize_from_u8(&message.body){
                        Ok(mut t) => {
                            
                            // Add Delivery tag to task data for later acknowledgement
                            t.add_data("task-delivery-tag", message.reply.delivery_tag.to_string().as_str());
                            
                            // Add this as a event to the tasks history
                            let h = format!("[WORKER] Task arrived at Worker [{}] with delivery tag {}", self.config.id, &t.data["task-delivery-tag"]);
                            self.add_history(h.as_str());
                            
                            // Set the status of the task to queued
                            t.queue();

                            println!(" ✚ [WORKER][{id}] Received Task for {short}", 
                                id=&t.id[..6],
                                short=t.command.short());
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
        !self.tasks.is_empty()
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
        if self.current.is_none() {
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
    pub fn select_next_task(&mut self, channel: &mut Channel){
        // Only do this if there is no current task running
        if self.current.is_none() && self.has_task() {
            let mut i = 0;
            let mut next = None;
            // Find the first task that:
            // - has a blendfile
            // - has a constructed command
            // - is queued
            // then remove this Task from the list and tore it in next
            while i < self.tasks.len() {
                if self.has_blendfile(&self.tasks[i]) &&
                    self.tasks[i].command.is_constructed() &&
                    (self.tasks[i].is_queued() || self.tasks[i].is_running()) &&
                    next.is_none() {
                        // println!("SELECTED TASK [{}]", &self.tasks[i].id);
                        next = Some(self.tasks.remove(i));
                } else {
                        i += 1;
                        if i < self.tasks.len() {
                            // println!("SELECTION for {}", &self.tasks[i].id);
                            // println!("              has_blendfile:  {}", self.has_blendfile(&self.tasks[i]));
                            // println!("              is_constructed: {}", self.tasks[i].command.is_constructed());
                            // println!("              is_queued:      {}", self.tasks[i].is_queued());
                            // println!("              next.is_none:   {}\n", next.is_none());
                        }
                }
            }

            // Match the result of above find operation and assign it to
            // self.current only if there is an actual Task
            if let Some(mut t) = next {
                t.start();
                println!(" ✚ [WORKER][{}] Queued task for job [{}]", &t.id[..6], t.parent_id);
                self.display_divider = true;
                let routing_key = format!("start.{}", self.config.id);
                match t.serialize_to_u8(){
                    Ok(task_json) => channel.worker_post(routing_key, task_json),
                    Err(err) => eprintln!(" ✖ [WORKER] Error: Failed ot deserialize Task {}: {}", t.id, err)
                }
                
                self.current = Some(t);
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
            let deliver_tag = &t.data["task-delivery-tag"]
                                .parse::<u64>()
                                .unwrap();
            if let Err(err) = channel.basic_ack(*deliver_tag, false){
                eprintln!(" ✖ [WORKER] Error: Couldn't acknowledge task {} for job [{}]: {}", 
                    t.command.short(), 
                    t.parent_id,
                    err);
            }

            // Post the updated Task Info
            let routing_key = format!("finish.{}", self.config.id);
            match t.serialize_to_u8(){
                Ok(task_json) => channel.worker_post(routing_key, task_json),
                Err(err) => eprintln!(" ✖ [WORKER] Error: Failed ot deserialize Task {}: {}", t.id, err)
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
            eprintln!("{}", format!(" ✖ [WORKER] Errored task [{}] for job [{}]: {}", &t.id[..6], t.parent_id, err).red());
            let routing_key = format!("error.{}", self.config.id);
            match t.serialize_to_u8(){
                Ok(task_json) => channel.worker_post(routing_key, task_json),
                Err(err) => eprintln!(" ✖ [WORKER] Error: Failed to deserialize Task {}: {}", &t.id[..6], err)
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
                    if let Some(blendfile) = blendfiles.get(&task.parent_id) {
                        match blendfile{
                            Some(bf) => task.add_data("blendfile", &bf.path.to_string_lossy()),
                            None => ()
                        }
                    }
                  })
    }
}