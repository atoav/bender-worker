use ::*;
use config::Config;
use amqp::Basic;
use bender_job::Task;
use bender_job::History;
use std::collections::HashMap;
use chrono::Utc;
use itertools::Itertools;
use std::io::{Write};
use std::fs::File;
use hyper::{Client, Body};
use hyper::http::Request;
use hyper::rt::{self, Future, Stream};
// use config::GenResult;




pub struct Work{
    pub config: Config,
    pub tasks: Vec<Task>,
    pub current: Option<Task>,
    pub history: History,
    pub frames: Vec<PathBuf>,
    pub blendfiles: HashMap<String, Option<PathBuf>>
}




impl Work{
    pub fn new(config: Config) -> Self{
        Work{
            config: config,
            tasks: Vec::<Task>::new(),
            current: None,
            history: History::new(),
            frames: Vec::<PathBuf>::new(),
            blendfiles: HashMap::<String, Option<PathBuf>>::new()
        }
    }

    /// Add to the Work-History
    pub fn add_history<S>(&mut self, value: S) where S: Into<String> {
        self.history.insert(Utc::now(), value.into());
    }

    /// Add a ad ID:Blendfilepath pair to the Hashmap
    pub fn add_blendfile<S, P>(&mut self, id: S, path: P) where S: Into<String>, P: Into<PathBuf> {
        self.blendfiles.insert(id.into(), Some(path.into()));
    }

    /// Returns true if a new task should be added
    pub fn should_add(&self) -> bool{
        self.tasks.len() < self.config.workload
    }

    /// Returns true if there is at least one task
    pub fn has_task(&self) -> bool{
        self.tasks.len() > 0
    }

    /// Returns true if the Tasks blendfile is 
    pub fn has_blendfile(&self, t: &Task) -> bool{
        match self.blendfiles.get(&t.parent_id) {
            Some(entry) => {
                match entry{
                    Some(_) => true,
                    None => false
                }
            },
            none => false
        }
    }

    /// Returns the path to the blendfile if it has one
    pub fn get_blendfile_for_task(&self, t: &Task) -> Option<PathBuf>{
        match self.blendfiles.get(&t.parent_id){
            Some(ref entry) => {
                match entry{
                    Some(p) => Some(p.clone()),
                    None => None
                }
            },
            None => None
        }
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

    /// Moves the next Task out only if there is no Task running
    /// Only works on tasks with a constructed Command
    /// Sets the Tasks Status to Running
    pub fn update_next_task(&mut self){
        if !self.tasks.iter().any(|t| t.is_running()) {
            let x: Option<Task> = 
                self.tasks.into_iter()
                          .filter(|t| self.has_blendfile(t))
                          .filter(|t| t.command.is_constructed())
                          .find(|t| t.is_queued())
                          .take();
            // Match the result of above find operation and assign it to
            // self.current only if there is an actual Task
            match x{
                Some(mut t) => {
                    t.start();
                    self.current = Some(t)
                },
                None => ()
            }
        }
    }

    /// finish the current task and push it back to tasks
    pub fn finish_current(&mut self){
        let moved = false;
        if let Some(ref mut t) = self.current{
            t.finish();
            self.tasks.push(t.clone());
        }

        if moved{
            self.current = None;
        }
    }

    /// Runs every loop and updates everything work related things.
    pub fn update(&mut self, channel: &mut Channel){
        // Add new tasks only if we don't exceed the number of tasks definied in \
        // the workload setting
        if self.should_add(){
            self.add(channel);
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
        self.update_next_task();

        // Dispatch a Command for the current Task ("self.current")
        self.do_work();

    }

    /// Listen in to work queue and get n messages (defined by the workload setting)
    /// Store these in a Option-wrapped Work struct (along with...)
    /// Reject these messages if our system is not fit to do work
    /// Acknowledge messages that are wonky
    pub fn add(&mut self, channel: &mut Channel){
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

    /// Add the blendfiles paths to data
    pub fn add_paths_to_tasks(&mut self){
        let blendfiles = self.blendfiles.clone();
        self.tasks.iter_mut()
                  .filter(|task| !task.data.contains_key("blendfile"))
                  .for_each(|task|{
                      match blendfiles.get(&task.parent_id){
                          Some(entry) => {
                            match entry{
                                Some(p) => task.add_data("blendfile", &p.to_string_lossy()),
                                None => ()
                            }
                          },
                          None => ()
                      }
                  })

    }

    // Construct the commands
    pub fn construct_commands(&mut self){
        self.tasks.iter_mut()
                  .filter(|task| task.is_queued())
                  .filter(|task| !task.command.is_constructed())
                  .filter(|task| task.data.contains_key("blendfile"))
                  .for_each(|task|{
                      // we can unwrap this because, the key "blendfile" only exists
                      // if there is a value
                      let p = task.data.get("blendfile").unwrap().clone();
                      task.construct(p, std::borrow::Cow::Borrowed("some/out/folder/####.png").to_string());
                      println!(" ✚ [WORKER] Constructed task [{}]: {:?}", task.id, task.command.to_string());
                  })
    }

    /// If there is a current Task, dispatch it. If a Task finished, finish it and
    /// push it back
    pub fn do_work(&mut self){
        // Do nothing if there is no current Task
        match self.current{
            Some(ref task) => {
                let _c = task.command.to_string();
                println!("Simulating Work...");
            },
            None => ()
        }

    }

    /// Check if the ID for a Job is already stored in the blendfiles
    pub fn holds_parent_id<S>(&self, id: S) -> bool where S: Into<String>{
        let id = id.into();
        self.blendfiles.keys().any(|key| key == &id)
    }

    // Get a iterator over references to unique parent IDs found in the tasks
    pub fn unique_parent_ids<'a>(&'a self) -> impl Iterator<Item = &str> + 'a{
        self.tasks
            .iter()
            .map(|task| task.parent_id.as_str())
            .unique()
    }

    /// Deals with getting new blendfiles
    pub fn get_blendfiles(&mut self){
        // Get a unique list from the tasks job ids, ignoring job IDs that are 
        // present as keys for the HashMap self.blendfiles already
        let ids: Vec<String> = self.unique_parent_ids()
                                   .filter(|&id| !self.holds_parent_id(id))
                                   .map(|id| id.to_owned())
                                   .collect();

        if ids.len() != 0{ 
        println!("Debug: Found {} unique job IDs", ids.len());
    }

        // For each remaining ID start a request and insert the resulting path
        // into the hashmap
        ids.iter()
            .for_each(|id|{
                let p = self.request_blendfile(id.to_owned());
                self.blendfiles.insert(id.to_string(), p);
             });
            
        
    }

    /// Request a single blendfile for a given Job-ID from flaskbender via http
    /// get request. Uses the User-Agent http header to request the actual file
    pub fn request_blendfile<S>(&mut self, id: S) -> Option<PathBuf> where S: Into<String>{
        let id = id.into();
        let url = self.config.bender_url.clone();
        let mut savepath = self.config.blendpath.clone();
        savepath.push(format!("{id}.blend", id=id));

        let savepath2 = savepath.clone();
        let mut ok = false;

        // Run in own thread with future
        rt::run(rt::lazy(move || {
            let client = Client::new();
            // Make a request to the URL
            let url = format!("{url}/job/{id}", url=url, id=id);
            let mut request = Request::builder();
            request.uri(url)
                   .header("User-Agent", "bender-worker");
            let request = request.body(Body::empty()).unwrap();

            // The actual request
            client.request(request)
                  .and_then(|response| {
                        // The body is a stream, and for_each returns a new Future
                        // when the stream is finished, and calls the closure on
                        // each chunk of the body and writes the file to the 
                        let mut file = File::create(&savepath).unwrap();
                        let status = response.status().clone();
                        response.into_body().for_each(move |chunk| {
                            if status.is_success() {
                                println!(" ✚ [WORKER] Requesting blendfile for job [{id}]", id=id);
                                println!(" ✚ [WORKER] Saving blendfile to {path}", path=savepath.to_string_lossy());
                                file.write_all(&chunk)
                                    .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to file: {}", e))
                            }else{
                                println!(" ✖ [WORKER] Warning: The Server responded with: {}", status);
                                std::io::sink().write_all(&chunk)
                                    .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to sink: {}", e))
                            }
                        })
                  })
                  .map(move |_| {
                        println!(" ✚ [WORKER] Sucessfully saved blendfile for job");
                        ok = true;
                  })
                    // If there was an error, let the user know...
                  .map_err(|err| {
                        eprintln!(" ✖ [WORKER] Error {}", err);
                  })
        }));

        // If everything worked out, insert the id with the path to the file into the values
        match ok { 
            true => Some(savepath2),
            false => None
        }
    }
}