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
use bender_job::{Status, Task};




impl Work{

    /// Deals with reqeusting new blendfiles
    pub fn get_blendfiles(&mut self){
        // Get a unique list from the tasks job ids, ignoring job IDs that are 
        // present as keys for the HashMap self.blendfiles already
        let ids: Vec<String> = self.unique_parent_ids()
                                   .filter(|&id| !self.has_blendfile_by_id(id))
                                   .map(|id| id.to_owned())
                                   .collect();

        // Only dispatch a request if we have something to reqeust
        if ids.len() != 0{ 
            // For each remaining ID start a request and insert the resulting path
            // into the hashmap
            ids.iter()
                .for_each(|id|{
                    let p = self.request_blendfile(id.to_owned());
                    // println!("{:?}", p);
                    let opt_bf = match p.as_path().exists(){
                        true => Some(Blendfile::new(p)),
                        false => None
                    };
                    self.blendfiles.insert(id.to_string(), opt_bf);
                    
                 });
        }

        // If the length of unique ids equals the length of entries containing \
        // Some<Blendfile> in Work::blendfiles, we assume that all files have \
        // been downloaded 
        if ids.len() == self.blendfiles.iter().map(|(_,x)| x).filter(|e|e.is_some()).count(){
            println!("{}", format!(" ✔️ [WORKER] Downloaded all blendfiles").green());
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
        format!("{min}:{s}.{ms}", min=min, s=s, ms=ms)
    }else if w == 0 && d == 0{
        // We have a duration with hours
        format!("{h}:{min}:{s}", h=h, min=min, s=s)
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