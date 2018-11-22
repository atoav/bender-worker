use ::*;
use config::Config;
use amqp::Basic;
use bender_job::Task;
use std::collections::HashMap;
use bender_job::History;
use chrono::Utc;
use std::io::{self, Write};
use hyper::Client;
use hyper::rt::{self, Future, Stream};




pub struct Work{
    pub tasks: Vec<Task>,
    pub history: History,
    pub frames: Vec<PathBuf>,
    pub blendfiles: HashMap<String, PathBuf>,
    pub workload: usize
}




impl Work{
    pub fn new(workload: usize) -> Self{
        Work{
            tasks: Vec::<Task>::new(),
            history: History::new(),
            frames: Vec::<PathBuf>::new(),
            blendfiles: HashMap::<String, PathBuf>::new(),
            workload: workload
        }
    }

    /// Add to the Work-History
    pub fn add_history<S>(&mut self, value: S) where S: Into<String> {
        self.history.insert(Utc::now(), value.into());
    }

    /// Add a ad ID:Blendfilepath pair to the Hashmap
    pub fn add_blendfile<S, P>(&mut self, id: S, path: P) where S: Into<String>, P: Into<PathBuf> {
        self.blendfiles.insert(id.into(), path.into());
    }

    /// Returns true if a new task should be added
    pub fn should_add(&self) -> bool{
        self.tasks.len() < self.workload
    }

    /// Returns true if there is at least one task
    pub fn has_task(&self) -> bool{
        self.tasks.len() > 0
    }

    /// Runs every loop and updates everything work related things.
    pub fn update(&mut self, channel: &mut Channel, config: &Config){
        // Add new tasks only if we don't exceed the number of tasks definied in \
        // the workload setting
        if self.should_add(){
            self.add(channel, config);
        }

        // Get the blendfile from the server
        if self.has_task(){
            self.get_blendfiles();
        }

    }

    /// Listen in to work queue and get n messages (defined by the workload setting)
    /// Store these in a Option-wrapped Work struct (along with...)
    /// Reject these messages if our system is not fit to do work
    /// Acknowledge messages that are wonky
    pub fn add(&mut self, channel: &mut Channel, config: &Config){
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
                            let h = format!("Task arrived at Worker [{}] with delivery tag {}", config.id, t.data.get("task-delivery-tag").unwrap());
                            self.add_history(h.as_str());
                            
                            // Add the newly modified Task to the queue
                            self.tasks.push(t);
                        },
                        Err(err) => {
                            println!(" ✖ [WORKER] Error: Couldn't deserialize Task from message.body: {}", err);
                            // Always try to acknowledge received messages that couldn't be decoded
                            remaining_delivery_tags.push(message.reply.delivery_tag);
                        }
                    }
                });

        // Acknowledge all remaining wonky messages, that had their deserialization failed
        // to avoid the accumulation of garbage in the queue
        for tag in remaining_delivery_tags.iter(){
            if let Err(err) = channel.basic_ack(*tag, false){
                println!(" ✖ [WORKER] Error: acknowledgment failed for received message: {}", err);
            }
        }
    }

    // d
    pub fn construct_commands(&mut self){
        self.tasks.iter_mut()
                  .filter(|task| task.is_queued())
                  .for_each(|task|{
                      // task.construct()
                      println!("{}", task);
                  })
    }

    // a
    pub fn run_task(&mut self){

    }


    pub fn get_blendfiles(&mut self){
        // figure out if we actually NEED a new blendfile

        // then
        
            self.tasks.iter()
                      .map(|task| task.parent_id.clone())
                      .for_each(|u|{
                        rt::run(rt::lazy(move || {
                        let client = Client::new();
                        let url = format!("http://0.0.0.0:5000/job/{}", u);
                        let uri = url.as_str().parse().unwrap();

                        client
                            .get(uri)
                            .map(|res| {
                                println!("Response: {}", res.status());
                            })
                            .map_err(|err| {
                                println!("Error: {}", err);
                            })
                        }));
                      });
            
        
    }
}