use std::convert::From;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;
use std::{fmt,fs,io};

use serde_json::{Value,json};
use serde::{Serialize,Deserialize};
use chrono::{DateTime,Local,Duration};
use chrono::naive::NaiveDate;
use encoding_rs::mem::decode_latin1;


fn main() -> Result<()> {

    let regions = vec![
	(Level::Municipality, vec![
	    "Scherpenheuvel-Zichem", "Holsbeek", "Aarschot", "Kortrijk", "Herselt",
	    "Wervik", "Leuven", "Brussel", "Mechelen", "Antwerpen", "Gent", "Tienen",
	    "Hasselt", "Sint-Truiden", "Westerlo", "Heist-op-den-Berg"
	]),
	(Level::Province, vec![
	    "Vlaams-Brabant", "Antwerpen", "Limburg", "West-Vlaanderen", "Oost-Vlaanderen",
	    "Waals-Brabant", "Namen", "Luik", "Henegouwen", "Luxemburg"
	]),
	(Level::Country, vec!["Belgium"])
    ];


    let data = cases()?;

    let graph_path = PathBuf::from("graphs");
    fs::create_dir_all(&graph_path)?;

    for (level,mut regions) in regions {

	regions.sort();

	let graph_data = regions.iter().map(|region| {
	    let series = case_series(&data, |cs| level.filter(region, cs));
	    (*region, case_dates().zip(series).filter_map(
		|(d,n)| n.map(|n| (d, n as f64))
	    ).collect())
	}).collect();

	write_graph(io::BufWriter::new(File::create(graph_path.join(
	    format!("belgium-{}-absolute.html", level.name())))?),
		    format!("Number of confirmed COVID-19 cases \
			     by {}", level.name()).as_str(),
		    "Count", json!({"type":"log"}), vec![],
		    &graph_data)?;


	for smoothing in vec![7,14] {
	
	    let graph_data = regions.iter().map(|region| {
		let series = interpolate(case_series(&data, |cs| level.filter(region, cs)));
		let series = growths(&average(&(daily(&series)[1..]), smoothing), smoothing);
		(*region, case_dates().skip(1).zip(series).collect())
	    }).collect();

	    write_graph(io::BufWriter::new(File::create(graph_path.join(
		format!("belgium-{}-growth-{}days.html", level.name(), smoothing)))?),
			format!("Average daily growth of {}-day average confirmed \
				 COVID-19 cases by {}", smoothing, level.name()).as_str(),
			"Factor", json!({"domain":[0.5, 1.5]}), vec![1.0], &graph_data)?;

	}

    }
    
    Ok(())

}


fn write_graph<W:Write>(mut out: W, title: &str, ytitle: &str, scale: Value, refs: Vec<f64>,
			data: &Vec<(&str,Vec<(NaiveDate,f64)>)>) -> Result<()> {
    write!(out, "<!DOCTYPE html><html><head>")?;
    write!(out, "<meta charset=\"UTF-8\">")?;
    write!(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")?;
    write!(out, "<title>COVID-19 Growth of CASES</title>")?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>")?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega-lite@4\"></script>")?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega-embed\"></script>")?;
    write!(out, "</head>")?;
    write!(out, "<body>")?;
    write!(out, "<div id=\"vis\" style=\"overflow: hidden; position: absolute;top: 0; left: 0; right: 0; bottom: 0;\"></div>")?;
    write!(out, "<script type=\"text/javascript\">")?;
    write!(out, "var spec = ")?;
    serde_json::to_writer_pretty(out.by_ref(), &json!({
	"$schema": "https://vega.github.io/schema/vega-lite/v4.json",
	"height": "container",
	"width": "container",
	"title": title,
	"data": {
	    "values": data.iter().flat_map(
		|(region,vals)| vals.iter().filter_map(
		    move |(date,val)| match val.is_normal() {
			false => None,
			true => Some(json!({
			    "Date": format!("{}", date.format("%Y-%m-%d")),
			    "Region": region.to_string(),
			    "Value": val
			}))
		    })
	    ).collect::<Vec<_>>()
	},
	"layer": [
	    {
		"encoding": {
		    "color": {
			"field": "Region",
			"type":"nominal"
		    },
		    "x": {
			"field":"Date",
			//"scale": {"domain": ["2020-03-31","2020-07-18"]},
			"timeUnit": "utcyearmonthdate",
			"title":"Date",
			"type":"temporal"
		    },
		    "y": {
			"field":"Value",
			"title": ytitle,
			"scale": scale,
			"type":"quantitative"
		    }
		},
		"layer": [
		    {
			"mark":"line",
			"selection": {
			    "Highlight": {"bind":"legend","type":"multi","fields":["Region"]},
			    "Grid": {"bind":"scales","type":"interval"}
			},
			"encoding":{
			    "opacity":{"value":0.1,"condition":{"value":1,"selection":"Highlight"}}
			}
		    },
		    {
			"mark":"point",
			"encoding": {
			    "opacity": {
				"value":0,
				"condition": [
				    {"value":1,"test":{"and":[{"selection":"Highlight"},{"selection":"Hover"}]}},
				    {"value":0.2,"selection":"Hover"}
				]
			    }
			}
		    }
		]	
	    },
	    {
		"transform": [
		    {
			"groupby": ["Date"],
			"value": "Value",
			"pivot": "Region"
		    }
		],
		"mark": {
		    "color": "gray",
		    "tooltip": {"content":"data"},
		    "type": "rule"
		},
		"selection": {
		    "Hover": {
			"nearest":true,
			"empty":"none",
			"clear":"mouseout",
			"type":"single",
			"on":"mouseover",
			"fields":["Date"]
		    }
		},
		"encoding": {
		    "opacity": {
			"value": 0,
			"condition": {
			    "value": 1,
			    "selection": "Hover"
			}
		    },
		    "x": {
			"field":"Date",
			"type":"temporal"
		    },
		    "tooltip": vec![
			json!({"field":"Date","type":"temporal"})
		    ].into_iter().chain(data.iter().map(
			|(region,_)| json!({"field":region,"format":".3f","type":"quantitative"})
		    )).collect::<Vec<_>>()
		}
	    },
	    {
		"mark": {
		    "color": "red",
		    "opacity": 0.5,
		    "size": 1,
		    "type":"rule"
		},
		"data": {
		    "values": refs.iter().map(|y| json!({"Value": y})).collect::<Vec<_>>()
		},
		"encoding": {
		    "y": {
			"field":"Value",
			"type":"quantitative"
		    }
		}
	    }
	]
    }))?;
    write!(out, ";vegaEmbed('#vis', spec,{{}}).then(function(result) {{")?;
    write!(out, "}}).catch(console.error);")?;
    write!(out, "</script>")?;
    write!(out, "</body></html>")?;
    Ok(())
}


fn case_series<F>(data: &Vec<Vec<Cases>>,filter: F) -> Vec<Option<u64>>
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


fn cases() -> Result<Vec<Vec<Cases>>> {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31),
		   Some(Local::today().naive_local()))
	.map(|date| Ok(cases_per_day(date)?
		       .unwrap_or(vec![])))
	.collect()
}

fn case_dates() -> NaiveDateRange {
    NaiveDateRange(NaiveDate::from_ymd(2020, 3, 31), None)
}

fn cases_per_day(date: NaiveDate) -> Result<Option<Vec<Cases>>> {

    let cache_path = PathBuf::from("data/sciensano/cases");
    let cache_file = cache_path.join(format!(
	"COVID19BE_CASES_MUNI_CUM_{}.json",
	date.format("%Y%m%d")));

    if cache_file.exists() {
	match serde_json::from_reader(io::BufReader::new(File::open(&cache_file)?))? {
	    Some(data) => return Ok(Some(data)),
	    None => {
		let modified : DateTime<Local> = fs::metadata(&cache_file)?.modified()?.into();
		if modified.date().naive_local() > date.succ()
		    || Local::now() - modified < Duration::minutes(30)
		{
		    return Ok(None)
		}
	    }
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


fn interpolate(series: Vec<Option<u64>>) -> Vec<f64> {

    let mut result = Vec::new();
    let mut s = 0;
    let mut n = 1;

    for i in series.into_iter() {
	match i {
	    Some(i) => {
		for j in 1..n {
		    result.push(s as f64 + (i - s) as f64 * j as f64 / n as f64);
		}
		result.push(i as f64);
		s = i; n = 1;
	    }
	    None => {
		n += 1;
	    }
	}
    }

    result

}


fn daily(data: &[f64]) -> Vec<f64> {
    (0..data.len()).map(
	|i| data[i] - if i > 0 {data[i-1]} else {0.0}
    ).collect()
}   


fn growths(data: &[f64], avg: usize) -> Vec<f64> {
    (0..data.len()).map(
	|i| match data[i] == 0.0 || data[i - avg.min(i)] == 0.0 {
	    true => 1.0,
	    false => (data[i] / data[i - avg.min(i)]).powf(1.0 / avg as f64)
	}
    ).collect()
}


fn average(data: &[f64], avg: usize) -> Vec<f64> {
    let mut sum = 0.0;
    (0..data.len()).map(|i| {
	sum += data[i] - if i >= avg {data[i-avg]} else {0.0};
	sum / avg.min(i+1) as f64
    }).collect()
}


#[derive(Serialize,Deserialize,Debug)]
#[serde(rename_all = "UPPERCASE")]
struct Cases {
    tx_descr_nl: Option<String>,
    tx_adm_dstr_descr_nl: Option<String>,
    tx_prov_descr_nl: Option<String>,
    tx_rgn_descr_nl: Option<String>,
    cases: String
}

enum Level {
    Municipality,
    District,
    Province,
    Region,
    Country
}

impl Level {

    fn name(&self) -> &'static str {
	match self {
	    Self::Municipality => "municipality",
	    Self::District => "district",
	    Self::Province => "province",
	    Self::Region => "region",
	    Self::Country => "country"
	}
    }

    fn filter(&self, val: &str, cases: &Cases) -> bool {
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


struct NaiveDateRange(NaiveDate,Option<NaiveDate>);

impl Iterator for NaiveDateRange {
    type Item = NaiveDate;
    fn next(&mut self) -> Option<NaiveDate> {
	match self.1.map_or(true, |end| self.0 < end) {
	    false => None,
	    true => {
		let current = self.0;
		self.0 = self.0.succ();
		Some(current)
	    }
	}
    }
}


type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
enum Error {
    IO(io::Error),
    JSON(serde_json
	 ::Error),
    Reqwest(reqwest::Error),
    HttpError(reqwest::StatusCode),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
	Self::IO(err)
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


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
	match self {
	    Self::IO(err) => write!(f, "I/O error: {}", err),
	    Self::JSON(err) => write!(f, "JSON error: {}", err),
            Self::Reqwest(err) => write!(f, "Request error: {}", err),
	    Self::HttpError(err) => write!(f, "HTTP error: {}", err),
	}
    }
}
