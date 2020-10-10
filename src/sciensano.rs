use std::{io,fs};
use std::fs::File;
use std::path::Path;

use serde::{Serialize,Deserialize,de::DeserializeOwned};
use chrono::{DateTime,Local,Duration};
use chrono::naive::NaiveDate;
use encoding_rs::mem::decode_latin1;

use super::error::{Result,Error};
use super::NaiveDateRange;


#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct CasesMuni {
    tx_descr_nl: Option<String>,
    tx_adm_dstr_descr_nl: Option<String>,
    tx_prov_descr_nl: Option<String>,
    tx_rgn_descr_nl: Option<String>,
    cases: String
}

#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct CasesAgeSex {
    pub date: Option<String>,
    pub province: Option<String>,
    pub region: Option<String>,
    pub agegroup: Option<String>,
    pub sex: Option<String>,
    pub cases: u64
}

#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Hospitalizations {
    pub date: Option<String>,
    pub province: Option<String>,
    pub region: Option<String>,
    pub nr_reporting: u64,
    pub total_in: u64,
    pub total_in_icu: u64,
    pub total_in_resp: u64,
    pub total_in_ecmo: u64,
    pub new_in: u64,
    pub new_out: u64,
}

#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
pub struct Tests {
    pub date: Option<String>,
    pub province: Option<String>,
    pub region: Option<String>,
    pub tests_all: u64,
    pub tests_all_pos: u64,
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

    pub fn filter_muni(&self, val: &str, cases: &CasesMuni) -> bool {
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


pub fn cases_muni_series<F>(data: &Vec<Vec<CasesMuni>>,filter: F) -> Vec<Option<u64>>
where F: for<'r> Fn(&'r CasesMuni) -> bool {
    data.iter().map(
	|cs| cs.iter().filter(|cs| filter(*cs))
	    .fold(None, |a,b| match b.cases.as_str() {
		"<5" => a,
		n => Some(a.unwrap_or(0) + n.parse::<u64>()
			  .expect(&format!("failed to parse number of cases {:?}!", n)))
	    })
    ).collect()
}


pub fn cases_muni(cache_path: &Path) -> Result<Vec<Vec<CasesMuni>>> {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31),
		   Some(Local::today().naive_local()))
	.map(|date| Ok(cases_muni_per_day(cache_path, date)?
		       .unwrap_or(vec![])))
	.collect()
}


pub fn cases_muni_dates() -> NaiveDateRange {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31), None)
}


fn cases_muni_per_day(cache_path: &Path, date: NaiveDate)
		      -> Result<Option<Vec<CasesMuni>>> {

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

    let data = download_cases_muni_per_day(date)?;
    fs::create_dir_all(&cache_path)?;
    serde_json::to_writer(io::BufWriter::new(File::create(cache_file)?), &data)?;
    Ok(data)

}


pub fn cases_agesex(cache_path: &Path) -> Result<Vec<CasesAgeSex>> {
    cached("https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.json",
	   cache_path, "COVID19BE_CASES_AGESEX.json", Duration::minutes(30))
}


pub fn tests(cache_path: &Path) -> Result<Vec<Tests>> {
    cached("https://epistat.sciensano.be/Data/COVID19BE_tests.json",
	   cache_path, "COVID19BE_tests.json", Duration::minutes(30))
}


pub fn hospitalizations(cache_path: &Path) -> Result<Vec<Hospitalizations>> {
    cached("https://epistat.sciensano.be/Data/COVID19BE_HOSP.json",
	   cache_path, "COVID19BE_HOSP.json", Duration::minutes(30))
}


fn cached<T>(url: &str, cache_path: &Path, filename: &str,
	     max_age: Duration) -> Result<Vec<T>>
where T: Serialize + DeserializeOwned {

    let cache_path = cache_path.join("sciensano");
    let cache_file = cache_path.join(filename);

    if cache_file.exists() {
	let modified : DateTime<Local> = fs::metadata(&cache_file)?.modified()?.into();
	if Local::now() - modified < max_age {
	    return Ok(serde_json::from_reader(io::BufReader::new(File::open(&cache_file)?))?);
	}
    }

    println!("Downloading {}...", filename);
    let data = reqwest::blocking::get(url)?.json()?;

    fs::create_dir_all(&cache_path)?;
    serde_json::to_writer(io::BufWriter::new(File::create(cache_file)?), &data)?;
    Ok(data)

}


fn download_cases_muni_per_day(date: NaiveDate) -> Result<Option<Vec<CasesMuni>>> {

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
