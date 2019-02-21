//! The work::blendfile module implements methods related to wrangling the \
//! Job's blendfile. A single Task is referenced to a Blendfile via the Tasks \
//! parent_id. 
//!
//! ## The steps in the life of a blendfile
//! 1. When `Work` receives new Tasks via amqp, for each unique `task.parent_id`\
//!    a blendfile will be requested from flaskbender (unless it is already there)
//! 2. The requested file will be stored in the location defined in the config
//! 3. Tasks that call the Blendfile their parent increment it and the last access\
//!    time is stored
//! 4. Once there is no unfinished Task left, `Work` runs another request to \
//!    flaskbender asking about the Status of the Job. If the job is finished \
//!    and a grace period has passed, the blendfile can be deleted.


use ::*;
use chrono::prelude::*;
use chrono::Duration;
use itertools::Itertools;
use bender_job::{Status, Task, Job};




impl Work{

    /// Update the parent Jobs status via request
    pub fn fetch_parent_jobs_stati(&mut self) {
        // Clear the hashmap
        self.parent_jobs.clear();
        // Collect all unique parent ids into a Vec
        let u: Vec<String> = self.unique_parent_ids()
                                 .map(|id| id.to_string())
                                 .collect();
        // For each unique parent id request the current job status from flaskbender
        u.iter()
         .for_each(|id|{
            match self.request_jobstatus(id.to_string()) {
                Ok(status) => {
                    // Status is a string that looks like this: {'Job': 'Queued'}
                    let status = status.split('\'').collect::<Vec<&str>>();
                    match status.get(3){
                        Some(s) => {
                            self.parent_jobs.insert(id.to_string(), s.to_string());
                        },
                        None => errrun(format!("While requesting job status for [{}], malformed response", id.to_string()))
                    }
                },
                Err(err) => errrun(format!("While requesting job status for [{}]: {}", id.to_string(), err))
            }
         })
    }

    /// Update the parent Jobs status via read
    pub fn read_parent_jobs_stati(&mut self) {
        // Clear the hashmap
        self.parent_jobs.clear();
        // Collect all unique parent ids into a Vec
        let u: Vec<String> = self.unique_parent_ids()
                                 .map(|id| id.to_string())
                                 .collect();
        // For each unique parent id request the current job status from flaskbender
        u.iter()
         .for_each(|id|{
            let mut path = self.config.blendpath.clone();
            path.push(id);
            path.push("data.json");
            let mut read = false;
            while !read {
                read = match Job::from_datajson(&path){
                    Ok(job)  => {
                        let s = format!("{}", job.status);
                        self.parent_jobs.insert(id.to_string(), s);
                        true
                    },
                    Err(ref e) if format!("{}", &e).contains("EOF") => {
                        errrun(format!("EOF while reading job status for [{}]: {}\nTrying again", id.to_string(), e));
                        false
                    }
                    Err(err) => {
                        errrun(format!("While reading job status for [{}]: {}", id.to_string(), err));
                        true
                    }
                }
            }
        });
    }

    /// Check whether the given job id is queued
    pub fn job_is_finished<S>(&self, id: S) -> bool where S: Into<String> {
        let id = id.into();
        match self.parent_jobs.get(&id){
            Some(status) => {
                status.clone().contains("Finished")
            },
            None => false
        }
    }

    pub fn any_job_finished(&self) -> bool {
        self.parent_jobs.iter().any(|(_id, status)|status.contains("Finished"))
    }

    pub fn all_jobs_finished(&self) -> bool {
        self.parent_jobs.iter().all(|(_id, status)|status.contains("Finished"))
    }


    /// Delete finished blendfiles that are done and overdue
    pub fn cleanup_blendfiles(&mut self) {
        // Collect all Blendfile IDs that have all their tasks finished.
        let potentially_finished: Vec<String> = 
        self.blendfiles.iter()
                        .filter(|(_, b)|b.is_some())
                        .filter(|(job_id, _)|{
                            // Filter out jobs with unfinished jobs
                            self.get_tasks_for_parent_id(job_id.as_str()).iter()
                                                                 .all(|t|{
                                                                    t.is_ended()
                                                                 })
                        })
                        .filter(|(_, entry)|{
                            // Filter out jobs that are still within the grace period
                            match entry{
                                Some(bf) => bf.is_over_grace_period(std::time::Duration::from_secs(self.config.grace_period)),
                                None => false
                            }
                            
                        })
                        .map(|(id, _)| id.clone())
                        .collect();

        // Check if 
        let shall_finish: Vec<String> = potentially_finished.iter()
                                               .cloned()
                                               .filter(|id|{
                                                    self.job_is_finished(id.as_str())
                                               })
                                               .collect();

        // Remove tasks that are contained in finished blendfiles
        self.tasks.retain(|ref task| shall_finish.contains(&task.parent_id));

        // Transform the ids into  a tuple with ids and paths
        let shall_finish: Vec<(String, PathBuf)> =
        shall_finish.iter()
                    .map(|id| {
                        let id = id.clone();
                        let p = self.blendfiles.get_mut(id.as_str()).cloned();
                        let p = p.unwrap().unwrap().path;
                        (id, p)
                    })
                    .collect();

        // Actually go and delete the blendfiles and erase them from self.blendfiles
        shall_finish.iter()
                    .map(|(id, path)|{
                        let erase: bool = 
                        if path.exists() {
                            match fs::remove_file(&path){
                                Ok(_) => {
                                    okrun(format!("Deleted blendfile for finished job [{}]", id));
                                    true
                                },
                                Err(err) => {
                                    errrun(format!("Couldn't delete blendfile for finished job ({}): {}", path.to_string_lossy(), err));
                                    false
                                }
                            }
                        } else {
                             okrun(format!("ಠ_ಠ Tried to delete blendfile for finished job at {}, but it was already gone... that is okay I guess..", path.to_string_lossy()));
                             true
                        };
                        (id, erase)
                    })
                    .filter(|&(_, erase)| erase)
                    .for_each(|(id, _)| {
                        let _ = self.blendfiles.remove(id.as_str());
                        okrun(format!("Forgot blendfile for [{}]", id));
                    } );
    }

    /// Deals with reqeusting new blendfiles from flaskbender, inserts the paths
    /// into self.blendfiles
    pub fn fetch_blendfiles(&mut self){
        // Get a unique list from the tasks job ids, ignoring job IDs that are 
        // present as keys for the HashMap self.blendfiles already
        let ids: Vec<String> = self.unique_parent_ids()
                                   .filter(|&id| !self.has_blendfile_by_id(id))
                                   .map(|id| id.to_owned())
                                   .collect();

        // Only dispatch a request if we have something to reqeust
        if !ids.is_empty(){ 
            // For each remaining ID start a request and insert the resulting path
            // into the hashmap
            ids.iter()
                .for_each(|id|{
                    let p = self.request_blendfile(id.to_owned());
                    // println!("{:?}", p);
                    let opt_bf = if p.as_path().exists() { Some(Blendfile::new(p)) } else { None };
                    self.blendfiles.insert(id.to_string(), opt_bf);
                    
                 });
        }

        // If the length of unique ids equals the length of entries containing \
        // Some<Blendfile> in Work::blendfiles, we assume that all files have \
        // been downloaded 
        if ids.len() == self.blendfiles.iter().map(|(_,x)| x).filter(|e|e.is_some()).count(){
            okrun("Downloaded all blendfiles");
            self.display_divider = true;
        }
    }

    /// Deals with reading new blendfiles from disk, inserts the results into
    /// self.blendfiles
    pub fn read_blendfiles(&mut self){
        // Get a unique list from the tasks job ids, ignoring job IDs that are 
        // present as keys for the HashMap self.blendfiles already
        let ids: Vec<String> = self.unique_parent_ids()
                                   .filter(|&id| !self.has_blendfile_by_id(id))
                                   .map(|id| id.to_owned())
                                   .collect();

        // Only read if there are jobs to be read
        if !ids.is_empty(){ 
            // For each remaining ID find a blendfile and insert the resulting path
            // into the hashmap
            ids.iter()
                .for_each(|id|{
                    let mut p = self.config.blendpath.clone();
                    p.push(id.to_owned());
                    let blendfile = match fs::read_dir(p.clone()){
                        Ok(paths) => {
                            paths.filter(|direntry| direntry.is_ok())
                                 .map(|direntry| direntry.unwrap().path())
                                 .find(|path| {
                                    match path.extension(){
                                        Some(ext) => ext == std::ffi::OsStr::new("blend"),
                                        None      => false
                                    }
                                 })
                        },
                        Err(err) => {
                            errrun(format!("Directory for blendfile at {} doesn't exist: {}", p.to_string_lossy(), err));
                            None
                        }
                    };
                    // Create a Blendfile from the Option<PathBuf>
                    let opt_bf = match blendfile{
                        Some(b) => Some(Blendfile::new(b)),
                        None    => None
                    };
                    self.blendfiles.insert(id.to_string(), opt_bf);
                 });
        }

        // If the length of unique ids equals the length of entries containing \
        // Some<Blendfile> in Work::blendfiles, we assume that all files have \
        // been read 
        if ids.len() == self.blendfiles.iter().map(|(_,x)| x).filter(|e|e.is_some()).count(){
            okrun("Read all blendfiles");
            self.display_divider = true;
        }
    }

    /// Add a ID:Blendfile pair to the Hashmap
    pub fn add_blendfile<S, P>(&mut self, id: S, path: P) where S: Into<String>, P: Into<PathBuf> {
        let id = id.into();
        let path = path.into();
        self.blendfiles.insert(id.clone(), Some(Blendfile::new(&path)));
        let h = format!("Worker [{}] stored blendfile [{}] at {}", 
            self.config.id, id.as_str(), path.to_string_lossy());
        self.add_history(h.as_str());
    }

    /// Returns true if the Tasks blendfile is 
    pub fn has_blendfile(&self, t: &Task) -> bool{
        match self.blendfiles.get(&t.parent_id) {
            Some(entry) => entry.is_some(),
            None => false
        }
    }

    /// Returns true if the Tasks blendfile is 
    pub fn has_blendfile_by_id<S>(&self, id: S) -> bool where S: Into<String>{
        let id = id.into();
        match self.blendfiles.get(&id) {
            Some(entry) => entry.is_some(),
            None => false
        }
    }

    /// Returns the path to the blendfile if it has one
    pub fn get_blendfile_for_task(&self, t: &Task) -> Option<PathBuf>{
        match self.blendfiles.get(&t.parent_id){
            Some(ref blendfile) => {
                match blendfile{
                    Some(bf) => Some(bf.path.clone()),
                    None => None
                }
            },
            None => None
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
            .filter(|t| !t.is_ended())
            .map(|task| task.parent_id.as_str())
            .unique()
    }
}



/// The Blendfile Struct holds information about a blendfile, and their creation-\
/// and mod dates as well as the number of frames rendered, their individual \
/// durations and the file Paths.
///
/// When a Frame is rendered run the `increment_frame()` mmethod
#[derive(Debug, Clone)]
pub struct Blendfile{
    pub path: PathBuf,
    pub creation: DateTime<Utc>,
    pub lastaccess: DateTime<Utc>,
    pub frames_rendered: usize,
    pub remote_job_status: Option<Status>,
    pub frame_durations: Vec<Duration>
}



impl Blendfile{
    /// Create a new blendfile
    pub fn new<P>(p: P) -> Blendfile where P: Into<PathBuf>{
        let p = p.into();
        let now = Utc::now();
        Blendfile{
            path: p,
            creation: now,
            lastaccess: now,
            frames_rendered: 0,
            remote_job_status: None,
            frame_durations: Vec::<Duration>::new()
        }
    }

    /// Run this function, once a frame has been rendered. This calculates the \
    /// duration between this call and the last access and pushes it to the Vec.
    /// Then the access time is updated and the frame count incremented
    pub fn increment_frame(&mut self) {
        let now = Utc::now();
        let duration = now - self.lastaccess;
        self.frame_durations.push(duration);
        self.lastaccess = now;
        self.frames_rendered += 1;
    }

    /// Returns the duration since the Creation of the Blendfile
    pub fn age(&self) -> Duration {
        Utc::now() - self.creation
    }

    /// Returns the duration since the Last Access of the Blendfile
    pub fn since_last_access(&self) -> Duration {
        Utc::now() - self.lastaccess
    }

    /// Return the duration of the last Frame
    pub fn last_frame_duration(&self) -> Option<Duration>{
        self.frame_durations.last().cloned()
    }

    /// Returns the average duration of a rendered frame
    pub fn average_duration(&self) -> Duration {
        let millis = self.frame_durations.iter()
                                          .map(|&duration| duration.num_milliseconds())
                                          .sum::<i64>() / self.frame_durations.len() as i64;
        Duration::milliseconds(millis)
    }

    /// Returns the mean duration of a rendered frame
    pub fn mean_duration(&self) -> Duration {
        let mut d = self.frame_durations.clone();
        d.sort();
        let middle = d.len()/2;
        d[middle]
    }

    /// Returns true if the last access has happened a longer time ago than the \
    /// supplied grace period. Note that the grace_duration is a std::time::Duration
    pub fn is_over_grace_period(&self, grace_duration: std::time::Duration) -> bool{
        // println!("Duration since last access: {}", format_duration(self.since_last_access()));
        self.since_last_access().to_std().unwrap() > grace_duration
    }
}


/// Helper function to format a `chrono::Duration` to something more human readable
pub fn format_duration(duration: Duration) -> String {
    let w = duration.num_weeks();
    let d = duration.num_days() - w*7;
    let h = duration.num_hours() - d*24;
    let min = duration.num_minutes() - h*60;
    let s = duration.num_seconds() - min*60;
    let ms = duration.num_milliseconds() - s*1000;

    let w_label = match w{
        1 => "week",
        _ => "weeks"
    }.to_string();

    let d_label = match w{
        1 => "day",
        _ => "days"
    }.to_string();

    let h_label = match w{
        1 => "hour",
        _ => "hours"
    }.to_string();

    if w == 0 && d == 0 && h == 0 && min == 0 {
        // We have a duration with only seconds
        format!("{s}.{ms} s", s=s, ms=ms)
    }else if w == 0 && d == 0 && h == 0{
        // We have a duration with minutes and seconds
        format!("{min:02}:{s:02}.{ms}", min=min, s=s, ms=ms)
    }else if w == 0 && d == 0{
        // We have a duration with hours
        format!("{h}:{min:02}:{s:02}", h=h, min=min, s=s)
    }else if w == 0 {
        // We have a duration with days
        format!("{d} {d_label} {h} {h_label} {min} min", 
            d=d,
            h=h, 
            min=min, 
            d_label=d_label, 
            h_label=h_label)
    }else{
        // We have a duration with days
        format!("{w} {w_label} {d} {d_label} {h} {h_label}", 
            w=w, 
            d=d, 
            h=h, 
            w_label=w_label, 
            d_label=d_label, 
            h_label=h_label)
    }
}