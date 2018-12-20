use ::*;
use chrono::prelude::*;
use chrono::Duration;
use bender_job::Status;


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