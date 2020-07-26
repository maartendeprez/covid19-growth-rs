use std::{io,time,num,fmt};
use std::convert::From;


pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    CSV(csv::Error),
    JSON(serde_json::Error),
    Reqwest(reqwest::Error),
    HttpError(reqwest::StatusCode),
    SystemTime(time::SystemTimeError),
    ParseInt(num::ParseIntError),
    ParseDate(chrono::format::ParseError),
    MissingRegion(&'static str),
    MissingData,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
	Self::IO(err)
    }
}

impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Self {
	Self::CSV(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
	Self::JSON(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
	Self::Reqwest(err)
    }
}

impl From<time::SystemTimeError> for Error {
    fn from(err: time::SystemTimeError) -> Self {
	Self::SystemTime(err)
    }
}

impl From<num::ParseIntError> for Error {
    fn from(err: num::ParseIntError) -> Self {
	Self::ParseInt(err)
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(err: chrono::format::ParseError) -> Self {
	Self::ParseDate(err)
    }
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
	match self {
	    Self::IO(err) => write!(f, "I/O error: {}", err),
	    Self::CSV(err) => write!(f, "CSV error: {}", err),
	    Self::JSON(err) => write!(f, "JSON error: {}", err),
            Self::Reqwest(err) => write!(f, "Request error: {}", err),
	    Self::HttpError(err) => write!(f, "HTTP error: {}", err),
	    Self::SystemTime(err) => write!(f, "System Time error: {}", err),
	    Self::ParseInt(err) => write!(f, "Integer parse error: {}", err),
	    Self::ParseDate(err) => write!(f, "Date parse error: {}", err),
	    Self::MissingRegion(name) => write!(f, "Missing region: {}", name),
	    Self::MissingData => write!(f, "No data!"),
	}
    }
}
