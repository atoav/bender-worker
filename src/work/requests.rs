//! The work::requests module implements all requests a work struct makes to the \
//! `flaskbender` webinterface.


use ::*;
use std::fs::File;
use std::collections::HashMap;
use reqwest::header::{USER_AGENT, CONTENT_TYPE};


pub type GenError = Box<std::error::Error>;
pub type GenResult<T> = Result<T, GenError>;




impl Work {




    /// Request a single blendfile for a given Job-ID from flaskbender via http \
    /// get request. Uses the User-Agent http header in combination with a json \
    /// body to get the actual blendfile
    pub fn request_blendfile<S>(&mut self, id: S) -> GenResult<PathBuf> where S: Into<String>{
        let id = id.into();
        // Create the URL
        let url = self.config.bender_url.clone();
        let url = format!("{url}/job/worker/blend/{id}", url=url, id=id);
        // Construct a file path
        let mut savepath = self.config.blendpath.clone();
        savepath.push(format!("{id}.blend", id=id));
        // Create the output file
        let mut output_file = File::create(&*savepath)?;
        // Build the Client
        let client = reqwest::Client::new();
        // Construct the json message body
        let mut map = HashMap::new();
        map.insert("request", "blendfile");
        // Make the Request
        client.get(url.as_str())
              .header(CONTENT_TYPE, "application/json")
              .header(USER_AGENT, "bender-worker")
              .json(&map)
              .send()?
              .copy_to(&mut output_file)?;
        Ok(savepath)
    }






    pub fn request_jobstatus<S>(&self, id: S) -> GenResult<String> where S: Into<String>{
        let id = id.into();
        // Make a request to the URL
        let url = format!("{url}/job/worker/status/{id}", 
            url=self.config.bender_url.clone(), 
            id=id.clone());

        let client = reqwest::Client::new();
        let res = client.get(url.as_str())
            .header(USER_AGENT, "bender-worker")
            .send()?
            .text()?;
            
        Ok(res)
    }


    

}