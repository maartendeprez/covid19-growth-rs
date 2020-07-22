use std::{fs,io};
use std::fs::File;
use std::path::Path;
use std::time::Duration;
use std::collections::HashMap;

use chrono::naive::NaiveDate;

use super::error::Result;
use super::graph::Series;
use super::NaiveDateRange;


pub fn confirmed(cache_path: &Path) -> Result<HashMap<String,Series>> {

    let cache_path = cache_path.join("csse");
    let cache_file = cache_path.join("confirmed.json");

    if cache_file.exists() && fs::metadata(&cache_file)?.modified()?.elapsed()? < Duration::new(1800,0) {
	let contents = serde_json::from_reader::<_,HashMap<String,Vec<f64>>>(
	    io::BufReader::new(File::open(&cache_file)?));
	if let Ok(cached) = contents {
	    return Ok(cached.into_iter().map(
		|(n,s)| (n, NaiveDateRange(NaiveDate::from_ymd(2020, 1, 22), None)
			 .zip(s).collect())).collect());
	}
    }

    let data = download_confirmed()?;
    fs::create_dir_all(&cache_path)?;
    serde_json::to_writer(io::BufWriter::new(File::create(cache_file)?), &data)?;
    Ok(data.into_iter().map(
	|(n,s)| (n, NaiveDateRange(NaiveDate::from_ymd(2020, 1, 22), None)
		 .zip(s).collect())).collect())

}


fn download_confirmed() -> Result<HashMap<String,Vec<f64>>> {
    println!("Downloading time_series_covid19_confirmed_global.csv...");
    let res = reqwest::blocking::get("https://raw.githubusercontent.com/CSSEGISandData/COVID-19\
				      /master/csse_covid_19_data/csse_covid_19_time_series\
				      /time_series_covid19_confirmed_global.csv")?;
    csv::Reader::from_reader(res.text()?.as_bytes()).into_records().skip(1).map(|c| {
	let c = c?;
	Ok((match (c.get(0).unwrap_or(""), c.get(1).unwrap_or("")) {
	    ("",country) => country.to_string(),
	    (state,country) => format!("{},{}", state, country),
	}, c.iter().skip(4).map(|v| v.parse().map(|v:u64| v as f64))
	    .collect::<std::result::Result<_,_>>()?))
    }).collect()

}
