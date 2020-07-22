use std::{io,fs};
use std::fs::File;
use std::path::Path;

use serde::{Serialize,Deserialize};
use chrono::{DateTime,Local,Duration};
use chrono::naive::NaiveDate;
use encoding_rs::mem::decode_latin1;

use super::error::{Result,Error};
use super::NaiveDateRange;


#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Cases {
    tx_descr_nl: Option<String>,
    tx_adm_dstr_descr_nl: Option<String>,
    tx_prov_descr_nl: Option<String>,
    tx_rgn_descr_nl: Option<String>,
    cases: String
}

pub enum Level {
    Municipality,
    District,
    Province,
    Region,
    Country
}

impl Level {

    pub fn name(&self) -> &'static str {
	match self {
	    Self::Municipality => "municipality",
	    Self::District => "district",
	    Self::Province => "province",
	    Self::Region => "region",
	    Self::Country => "country"
	}
    }

    pub fn filter(&self, val: &str, cases: &Cases) -> bool {
	match self {
	    Self::Municipality => cases.tx_descr_nl.as_ref()
		.map_or(false, |v| val == v.as_str()),
	    Self::District => cases.tx_adm_dstr_descr_nl.as_ref()
		.map_or(false, |v| val == v.as_str()),
	    Self::Province => cases.tx_prov_descr_nl.as_ref()
		.map_or(false, |v| format!("Provincie {}", val).as_str() == v.as_str()),
	    Self::Region => cases.tx_rgn_descr_nl.as_ref()
		.map_or(false, |v| val == v.as_str()),
	    Self::Country => true
	}
    }

}


pub fn case_series<F>(data: &Vec<Vec<Cases>>,filter: F) -> Vec<Option<u64>>
where F: for<'r> Fn(&'r Cases) -> bool {
    data.iter().map(
	|cs| cs.iter().filter(|cs| filter(*cs))
	    .fold(None, |a,b| match (a,b.cases.parse().ok()) {
		(Some(a),Some(b)) => Some(a+b),
		(Some(a),None) => Some(a),
		(_,b) => b
	    })
    ).collect()
}


pub fn cases(cache_path: &Path) -> Result<Vec<Vec<Cases>>> {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31),
		   Some(Local::today().naive_local()))
	.map(|date| Ok(cases_per_day(cache_path, date)?
		       .unwrap_or(vec![])))
	.collect()
}


pub fn case_dates() -> NaiveDateRange {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31), None)
}


fn cases_per_day(cache_path: &Path, date: NaiveDate) -> Result<Option<Vec<Cases>>> {

    let cache_path = cache_path.join("sciensano/cases");
    let cache_file = cache_path.join(format!(
	"COVID19BE_CASES_MUNI_CUM_{}.json",
	date.format("%Y%m%d")));

    if cache_file.exists() {
	let modified : DateTime<Local> = fs::metadata(&cache_file)?.modified()?.into();
	let maturity = modified.date().naive_local() - date.succ();
	let age = Local::now() - modified;
	if age < Duration::minutes(30) || maturity > Duration::days(4) {
	    return Ok(serde_json::from_reader(io::BufReader::new(File::open(&cache_file)?))?);
	}
    }

    let data = download_cases_per_day(date)?;
    fs::create_dir_all(&cache_path)?;
    serde_json::to_writer(io::BufWriter::new(File::create(cache_file)?), &data)?;
    Ok(data)

}


fn download_cases_per_day(date: NaiveDate) -> Result<Option<Vec<Cases>>> {

    println!("Downloading COVID19BE_CASES_MUNI_CUM_{}.json...",
	   date.format("%Y%m%d"));

    let res = reqwest::blocking::get(&format!(
	"https://epistat.sciensano.be/Data/{}/COVID19BE_CASES_MUNI_CUM_{}.json",
	date.format("%Y%m%d"), date.format("%Y%m%d")))?;

    match res.status().as_u16() {
	404 => Ok(None),
	200 => Ok(Some(serde_json::from_str(decode_latin1(&res.bytes()?).as_ref())?)),
	_ => Err(Error::HttpError(res.status())),
    }

}
