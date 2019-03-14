use ::*;
use chrono::prelude::DateTime;
use chrono::Utc;


/// The RateLimiter allows to exponentially backoff failing tasks
#[derive(Debug, Default, Clone, Copy)]
pub struct RateLimiter{
    last:        Option<DateTime<Utc>>,
    last_failed: Option<DateTime<Utc>>,
    n_failed:    usize,
    n_max:       usize,
    min_rate_s:  usize,
    max_rate_s:  usize
}


impl RateLimiter{

    /// Create a new RateLimiter using the default configuration
    pub fn new() -> Self{
        Self::default()
    }

    /// Implements the Default trait for RateLimiter
    pub fn default() -> Self{
        RateLimiter{
            last:        None,
            last_failed: None,
            n_failed:    0,
            n_max:       10,
            min_rate_s:  1,
            max_rate_s:  120
        }
    }

    /// Set the last successful run and thus resetting the count in `n_failed`
    pub fn set_last(&mut self){
        self.last        = Some(Utc::now());
        self.last_failed = None;
        self.n_failed    = 0
    }

    /// Set the last errored run, increasing the backoff count and thus making \
    /// the next backoff period longer
    pub fn set_last_failed(&mut self){
        self.last_failed = Some(Utc::now());
        self.last        = None;
        self.n_failed += 1;
    }

    /// Return true if the last duration is over a certain threshold. For a \
    /// sucessful last run we always use the `min_rate_s` as a basis for this \
    /// threshold. For a failed last run, this  threshold is dynamically \
    /// calculated for each increment in the `n_failed` counter by the \
    /// method in `Self::calculate_backoff()`
    pub fn should_run(&self) -> bool{
        match (self.last, self.last_failed){
            (None, None) => true,
            (Some(last), None) => {
                let threshold = self.calculate_minimum_rate();
                last > threshold
            },
            (None, Some(last_failed)) => {
                let backoff_threshold_duration = self.calculate_backoff();
                last_failed > backoff_threshold_duration
            },
            _ => false
        }
    }

    /// Return a exponential backoff period duration depending on the number of\
    /// times `set_last_failed()` has been called, exponentially blending \
    /// between `min_rate_s` and `max_rate_s`, with the `max_rate_s` beeing \
    /// reached after `n_max` tries.
    fn calculate_backoff(&self) -> chrono::DateTime<Utc>{
        let factor = self.n_failed as f64 / self.n_max as f64;
        // Exponential Backoff
        let factor = factor * factor;
        // Blend between the min_rate_s and the max_rate_s exponentially
        let d = (self.min_rate_s + (self.max_rate_s-self.min_rate_s)) as f64 * factor;
        // Return a chrono::Duration
        let delta = chrono::Duration::seconds(d.round() as i64);
        Utc::now() + delta
    }

    fn calculate_minimum_rate(&self) -> chrono::DateTime<Utc>{
        let delta = chrono::Duration::seconds(self.min_rate_s as i64);
        Utc::now() + delta
    }


}