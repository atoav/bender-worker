use ::*;
use std::thread::sleep;
use std::time::Duration;
use std::process::Command;
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
use std::process::{Stdio};
use std::io::{BufRead, BufReader};
use std::{thread, time};
// use config::GenResult;



#[derive(Debug)]
pub struct Work{
    pub config: Config,
    pub tasks: Vec<Task>,
    pub current: Option<Task>,
    pub history: History,
    pub frames: Vec<PathBuf>,
    pub blendfiles: HashMap<String, Option<PathBuf>>,
    command: Option<std::process::Child>
}




impl Work{
    pub fn new(config: Config) -> Self{
        Work{
            config: config,
            tasks: Vec::<Task>::new(),
            current: None,
            history: History::new(),
            frames: Vec::<PathBuf>::new(),
            blendfiles: HashMap::<String, Option<PathBuf>>::new(),
            command: None
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

    /// Returns true if a new task should be added. This depends on two factors:
    /// 1. the workload that self has set in the config
    /// 2. whether there is enough space left
    pub fn should_add(&self) -> bool{
        let okay_space = system::enough_space(&self.config.outpath, self.config.disklimit);
        if okay_space{
            println!(" ❗ [WORKER] Warning: Taking no new jobs");
            system::print_space_warning(&self.config.outpath, self.config.disklimit);
        }
        self.tasks.iter()
                  .filter(|t| t.is_waiting() || t.is_queued())
                  .count() < self.config.workload
                      &&
                  !okay_space
                  
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
                    let routing_key = format!("worker.{}", self.config.id);
                    match channel.post_task_info(&t, routing_key){
                        Ok(_) => (),
                        Err(err) => eprintln!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err)
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
                eprintln!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err)
            }

            moved = true;
            println!(" ✔️ [WORKER] Finished task [{}] for job [{}]", t.id, t.parent_id);
        }

        if moved{
            self.current = None;
            self.command = None;
        }
    }

    /// error the current task and push it back to tasks
    pub fn error_current<S>(&mut self, err: S, channel: &mut Channel) where S: Into<String>{
        let err = err.into();
        let mut moved = false;
        // let c =  self.clone();
        if let Some(ref mut t) = self.current{
            t.error();
            println!(" ✚ [WORKER] Task Errored: {}", err);
            self.tasks.push(t.clone());
            moved = true;
            println!(" ✖ [WORKER] Errored task [{}] for job [{}]: {}", t.id, t.parent_id, err);
            let routing_key = format!("worker.{}", self.config.id);
            match channel.post_task_info(&t, routing_key){
                Ok(_) => (),
                Err(err) => eprintln!(" ✖ [WORKER] Error: Couldn't post current task to info queue: {}", err)
            }
        }

        if moved{
            self.current = None;
            self.command = None;
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
        self.run_command(channel);

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
                    match task.command{
                        bender_job::Command::Blender(ref c) => println!(" ✚ [WORKER] Constructed task for frame [{}]", c.frame),
                        _ => println!(" ✚ [WORKER] Constructed generic task [{}]", task.id)
                    }
                }
            });
        // put it pack
        std::mem::replace(&mut self.tasks, data);
    }


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
                                    println!(" ◯ [WORKER] Dispatched Command: \"blender {}\"", args.join(" "));
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
                   .header("content-type", "application/json")
                   .header("User-Agent", "bender-worker");
                   let json = r#"{"request":"blendfile"}"#;
            let request = request.body(Body::from(json)).expect("Creating request failed! ");

            // The actual request
            client.request(request)
                  .and_then(move |response| {
                        // The body is a stream, and for_each returns a new Future
                        // when the stream is finished, and calls the closure on
                        // each chunk of the body and writes the file to the 
                        println!(" ✚ [WORKER] Downloading blendfile to {path}", path=savepath.to_string_lossy());
                        // Create File
                        let status = response.status().clone();
                        // Write all chunks
                        response.into_body().for_each(move |chunk| {
                            if status.is_success() {
                                let file =  File::create(&savepath);
                                match file{
                                    Ok(mut f) => f.write_all(&chunk)
                                                 .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to file: {}", e)),
                                    Err(err) => {
                                        eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't write requested blendfile to path {}: {}",
                                            savepath.to_string_lossy(),
                                            err).red());
                                        std::io::sink().write_all(&chunk)
                                        .map_err(|e| panic!(" ✖ [WORKER] Error: Couldn't write Chunks to sink: {}", e))
                                    }
                                }
                            }else{
                                println!("{}", format!(" ❗ [WORKER] Warning: The Server responded with: {}", status).yellow());
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
                            eprintln!("{}", format!(" ✖ [WORKER] There is no server running at {}: {}", url2, err).red());
                            let duration = time::Duration::from_millis(2000);
                            thread::sleep(duration);
                        }else{
                            eprintln!("{}", format!(" ✖ [WORKER] Error {}", err).red());
                        }
                  })
        }));
            // If everything worked out, insert the id with the path to the file into the values
            savepath2
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExitStatus{
    Finished,
    Errored(String),
    Running,
    None
}



pub fn process_stdout(child:&mut std::process::Child){
    match child.stdout{
        Some(ref mut stdout) => {
            let reader = BufReader::new(stdout);
            reader.lines()
                  .filter_map(|line| line.ok())
                  .filter(|line| line.trim() != "")
                  .for_each(|_line| {
                    // println!("   [WORKER] {}", line);
                  });
        },
        None => eprintln!(" ✖ [WORKER] Error: Couldn't get stdout")
    }
}