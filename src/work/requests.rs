//! The work::requests module implements all requests a work struct makes to the \
//! `flaskbender` webinterface.

use ::*;
use std::thread::sleep;
use std::time::Duration;
use std::io::{Write};
use std::fs::File;
use hyper::{Client, Body};
use hyper::http::Request;
use hyper::rt::{self, Future, Stream};
use reqwest::header::USER_AGENT;


pub type GenError = Box<std::error::Error>;
pub type GenResult<T> = Result<T, GenError>;




impl Work {

    /// Request a single blendfile for a given Job-ID from flaskbender via http \
    /// get request. Uses the User-Agent http header in combination with a json \
    /// body to get the actual blendfile
    pub fn request_blendfile<S>(&mut self, id: S) -> PathBuf where S: Into<String>{
        let id = id.into();
        let url = self.config.bender_url.clone();
        let url2 = self.config.bender_url.clone();
        let mut savepath = self.config.blendpath.clone();
        savepath.push(format!("{id}.blend", id=id));

        let savepath2 = savepath.clone();
        let savepath3 = savepath.clone();

        // Run in own thread with future
        rt::run(rt::lazy(move || {
            let client = Client::new();
            // Make a request to the URL
            let url = format!("{url}/job/worker/blend/{id}", url=url, id=id);
            let mut request = Request::builder();
            request.uri(url)
                   .header("content-type", "application/json")
                   .header("User-Agent", "bender-worker");
                   let json = r#"{"request":"blendfile"}"#;
            let request = request.body(Body::from(json)).expect("Creating request failed! ");

            // The actual request
            client.request(request)
                  .and_then(move |response| {
                        println!(" ⛁ [WORKER] Downloading blendfile to {path}", path=savepath.to_string_lossy());
                        // The body is a stream, and for_each returns a new Future
                        // when the stream is finished, and calls the closure on
                        // each chunk of the body and writes the file to the file
                        let status = response.status();
                        // Run the closure on each of the chunks
                        response.into_body().for_each(move |chunk| {
                            if status.is_success() {
                                // Create File only if it doesn't exist yet,
                                // if it _exists_ open it instead!
                                let file =  if savepath.exists() { File::open(&savepath) } else { File::create(&savepath) };

                                // Discard Chunks when the file is not ok
                                match file{
                                    Ok(mut f) => f.write_all(&chunk)
                                                 .map_err(|e| panic!(format!(" ✖ [WORKER] Error: Couldn't write Chunks to file: {}", e).red())),
                                    Err(err) => {
                                        eprintln!("{}", format!(" ✖ [WORKER] Error: Couldn't write requested blendfile to path {}: {}",
                                            savepath.to_string_lossy(),
                                            err).red());
                                        std::io::sink().write_all(&chunk)
                                        .map_err(|e| panic!("{}", format!(" ✖ [WORKER] Error: Couldn't write Chunks to sink: {}", e).red()))
                                    }
                                }
                            }else{
                                println!("{}", format!(" ❗ [WORKER] Warning: The Server responded with: {}", status).yellow());
                                std::io::sink().write_all(&chunk)
                                    .map_err(|e| panic!(format!(" ✖ [WORKER] Error: Couldn't write Chunks to sink: {}", e).on_red()))
                            }
                        })
                  })
                  .map(move |_| {
                        if savepath2.is_file(){
                            println!(" ⛁ [WORKER] Sucessfully saved blendfile for job [{}]", id);
                        }   
                  })
                  .map_err(move |err| {
                        if format!("{}", err).contains("(os error 111)") {
                            eprintln!("{}", format!(" ✖ [WORKER] There is no server running at {}: {}", url2, err).red());
                            let timeout = Duration::from_secs(2);
                            sleep(timeout);
                        }else{
                            eprintln!("{}", format!(" ✖ [WORKER] Error {}", err).red());
                        }
                  })
        }));
        
        // If everything worked out, insert the id with the path to the file into the values
        savepath3
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