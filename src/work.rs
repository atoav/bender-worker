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



#[derive(Debug, Clone)]
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
        self.tasks.iter()
                  .filter(|t| t.is_waiting())
                  .count() < self.config.workload
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
            None => false
        }
    }

    /// Returns true if the Tasks blendfile is 
    pub fn has_blendfile_by_id<S>(&self, id: S) -> bool where S: Into<String>{
        let id = id.into();
        match self.blendfiles.get(&id) {
            Some(entry) => {
                match entry{
                    Some(_) => true,
                    None => false
                }
            },
            None => false
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

    /// Moves the next Task to self.current only if there is no Task running
    /// Only works on tasks with a constructed Command
    /// Sets the Tasks Status to Running
    pub fn queue_next_task(&mut self, channel: &mut Channel){
        // Only do this if there is no task running
        if !self.tasks.iter().any(|t| t.is_running()) {
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
                    let routing_key = format!("worker.{}", self.config.id);
                    match channel.post_task_info(&t, routing_key){
                        Ok(_) => (),
                        Err(err) => eprintln!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err)
                    }
                    self.current = Some(t);
                },
                None => ()
            }
        }else{
            println!(" ✚ [WORKER] didn't get a new task because the old is running");
        }
    }

    /// finish the current task and push it back to tasks
    pub fn finish_current(&mut self){
        let mut moved = false;
        // let c =  self.clone();
        if let Some(ref mut t) = self.current{
            t.finish();
            println!(" ✚ [WORKER] Finished Task");
            self.tasks.push(t.clone());
            moved = true;
            // println!("\n{:#?}", c);
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
        self.do_work();

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
                task.construct(p, self.config.outpath.clone().to_string_lossy().to_string());
                println!(" ✚ [WORKER] Constructed task [{}]", task.id);
            });
        // put it pack
        std::mem::replace(&mut self.tasks, data);
    }

    /// If there is a current Task, dispatch it. If a Task finished, finish it and
    /// push it back
    pub fn do_work(&mut self){
        // Do nothing if there is no current Task
        let finish = match self.current{
            Some(ref mut task) => {
                let c = task.command.to_string();
                println!(" ✚ [WORKER] Simulating Work...");
                println!(" ✚ [WORKER] {:?}", c);
                // finish?
                true
            },
            None => false
        };

        if finish{
            self.finish_current();
        }

    }

    /// Check if the ID for a Job is stored in the blendfiles
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
                                   .filter(|&id| !self.has_blendfile_by_id(id))
                                   .map(|id| id.to_owned())
                                   .collect();

        if ids.len() != 0{ 


            // For each remaining ID start a request and insert the resulting path
            // into the hashmap
            ids.iter()
                .for_each(|id|{
                    let p = self.request_blendfile(id.to_owned());
                    // println!("{:?}", p);
                    let p =match p.as_path().exists(){
                        true => Some(p),
                        false => None
                    };
                    self.blendfiles.insert(id.to_string(), p);
                    
                 });

            // println!("{:?}", self.blendfiles);
        }

        if ids.len() == self.blendfiles.iter().map(|(_,x)| x).filter(|e|e.is_some()).count(){
            println!(" ✚ [WORKER] Downloaded all blendfiles");
        }
    }

    /// Request a single blendfile for a given Job-ID from flaskbender via http
    /// get request. Uses the User-Agent http header to request the actual file
    pub fn request_blendfile<S>(&mut self, id: S) -> PathBuf where S: Into<String>{
        let id = id.into();
        let url = self.config.bender_url.clone();
        let url2 = self.config.bender_url.clone();
        let mut savepath = self.config.blendpath.clone();
        savepath.push(format!("{id}.blend", id=id));

        let savepath2 = savepath.clone();

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
                  .and_then(move |response| {
                        // The body is a stream, and for_each returns a new Future
                        // when the stream is finished, and calls the closure on
                        // each chunk of the body and writes the file to the 
                        println!(" ✚ [WORKER] Downloading blendfile to {path}", path=savepath.to_string_lossy());
                        // Create File
                        let mut file = File::create(&savepath).unwrap();
                        let status = response.status().clone();
                        // Write all chunks
                        response.into_body().for_each(move |chunk| {
                            if status.is_success() {
                                file.write_all(&chunk)
                                    .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to file: {}", e))
                            }else{
                                println!(" ❗ [WORKER] Warning: The Server responded with: {}", status);
                                std::io::sink().write_all(&chunk)
                                    .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to sink: {}", e))
                            }
                        })
                  })
                  .map(move |_| {
                        println!(" ✚ [WORKER] Sucessfully saved blendfile for job [{}]", id);
                  })
                  .map_err(move |err| {
                        if format!("{}", err).contains("(os error 111)") {
                            eprintln!(" ✖ [WORKER] There is no server running at {}: {}", url2, err);
                        }else{
                            eprintln!(" ✖ [WORKER] Error {}", err);
                        }
                  })
        }));
            // If everything worked out, insert the id with the path to the file into the values
            savepath2
    }
}