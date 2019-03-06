#![allow(unused_imports)]
pub mod system;

pub mod config;

pub mod work;

pub mod main;

pub mod command;



/// Return the width of the terminal
fn width() -> usize{
    let term = Term::stdout();
    term.size().1 as usize
}

/// A fancy error message
pub fn errmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " Error ".on_red().bold();
    eprintln!("    {} {}", label, s);
}

/// A fancy ok message
pub fn okmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = "  OK  ".on_green().bold();
    println!("    {} {}", label, s)
}

/// A fancy note message
pub fn notemsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = "  NOTE  ".on_yellow().bold();
    println!("    {} {}", label, s)
}

pub fn errrun<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " ✖ Error: ".red();
    eprintln!("{}{}", label, s);
}

pub fn okrun<S>(s: S) where S: Into<String>{
    let s = s.into();
    let label = " ✔️ [WORKER] ".green();
    println!("{}{}", label, s);
}

pub fn scrnmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let subs = s.as_bytes()
                .chunks(width())
                .map(std::str::from_utf8)
                .filter(|l| l.is_ok())
                .map(|l| l.unwrap())
                .map(|line| format!("{}{}", line, " ".repeat(width()-line.len())))
                .collect::<Vec<String>>()
                .join("\n");
    println!("{}", subs.black().on_white());
}

pub fn redmsg<S>(s: S) where S: Into<String>{
    let s = s.into();
    let subs = s.as_bytes()
                .chunks(width())
                .map(std::str::from_utf8)
                .filter(|l| l.is_ok())
                .map(|l| l.unwrap())
                .map(|line| format!("{}{}", line, " ".repeat(width()-line.len())))
                .collect::<Vec<String>>()
                .join("\n");
    println!("{}", subs.black().on_red());
}