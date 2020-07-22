mod graph;
mod error;
mod csse;
mod sciensano;

use std::fs;
use std::path::{PathBuf,Path};
use std::collections::BTreeMap;

use chrono::naive::NaiveDate;

use graph::{Series,GraphData};
use error::{Result,Error};


fn main() -> Result<()> {

    let graph_path = PathBuf::from("graphs");
    let cache_path = PathBuf::from("cache");
    let smoothings = vec![7,14];

    fs::create_dir_all(&graph_path)?;


    /* Graphs from CSSE Data.*/

    let groups = vec![("europe", vec![("Italy", vec!["Italy"]),
				      ("Spain", vec!["Spain"]),
				      ("Belgium", vec!["Belgium"]),
				      ("Netherlands", vec!["Netherlands"]),
				      ("Romania", vec!["Romania"]),
				      ("Switzerland", vec!["Switzerland"]),
				      ("Austria", vec!["Austria"]),
				      ("France", vec!["France"]),
				      ("Germany", vec!["Germany"]),
				      ("Sweden", vec!["Sweden"]),
				      ("Norway", vec!["Norway"]),
				      ("Finland", vec!["Finland"]),
				      ("United Kingdom", vec!["United Kingdom"])]),
		      ("america", vec![("Brazil", vec!["Brazil"]),
				       ("Chile", vec!["Chile"]),
				       ("Peru", vec!["Peru"]),
				       ("Argentina", vec!["Argentina"]),
				       ("Ecuador", vec!["Ecuador"]),
				       ("Bolivia", vec!["Bolivia"]),
				       ("Colombia", vec!["Colombia"]),
				       ("Mexico", vec!["Mexico"]),
				       ("US", vec!["US"]),
				       ("Canada", vec!["Northwest Territories,Canada", "Saskatchewan,Canada",
						       "Prince Edward Island,Canada", "Alberta,Canada",
						       "Nova Scotia,Canada", "Yukon,Canada", "British Columbia,Canada",
						       "Newfoundland and Labrador,Canada", "New Brunswick,Canada",
						       "Ontario,Canada", "Quebec,Canada", "Manitoba,Canada"])]),
		      ("africa", vec![("South Africa", vec!["South Africa"]),
				      ("Congo (Kinshasa)", vec!["Congo (Kinshasa)"]),
				      ("Ghana", vec!["Ghana"]),
				      ("Egypt", vec!["Egypt"]),
				      ("Israel", vec!["Israel"])]),
		      ("rest", vec![("South Korea", vec!["Korea, South"]),
				    ("Japan", vec!["Japan"]),
				    ("Russia", vec!["Russia"]),
				    ("India", vec!["India"]),
				    ("China", vec!["Anhui,China", "Xinjiang,China", "Henan,China",
						   "Shaanxi,China", "Hunan,China", "Jiangxi,China",
						   "Zhejiang,China", "Shanxi,China", "Tibet,China",
						   "Shanghai,China", "Macau,China", "Beijing,China",
						   "Jilin,China", "Tianjin,China", "Fujian,China",
						   "Guizhou,China", "Heilongjiang,China", "Gansu,China",
						   "Hainan,China", "Guangdong,China", "Hubei,China",
						   "Qinghai,China", "Sichuan,China", "Ningxia,China",
						   "Shandong,China", "Hebei,China", "Inner Mongolia,China",
						   "Chongqing,China", "Guangxi,China", "Liaoning,China",
						   "Yunnan,China", "Jiangsu,China", "Hong Kong,China"]),
				    ("Australia", vec!["South Australia,Australia",
						       "Australian Capital Territory,Australia",
						       "New South Wales,Australia",
						       "Victoria,Australia",
						       "Western Australia,Australia",
						       "Queensland,Australia",
						       "Northern Territory,Australia",
						       "Tasmania,Australia"]),
				    ("Iran", vec!["Iran"]),
				    ("Iraq", vec!["Iraq"]),
				    ("Turkey", vec!["Turkey"])])];

    let data = csse::confirmed(&cache_path)?;

    for (group,mut regions) in groups {

	regions.sort();

	graphs(&graph_path, &smoothings,
	       &format!("csse-{}", group), "country",
	       &regions.iter().map(
		   |(region,keys)| Ok((*region, sum_series(&keys.iter().map(
		       |key| data.get(*key).ok_or(Error::MissingRegion(*key))
		   ).collect::<Result<_>>()?)))
	       ).collect::<Result<_>>()?)?;

    }

    
    /* Graphs from Sciensano data. */

    let belgium = vec![
	(sciensano::Level::Municipality, vec![
	    "Scherpenheuvel-Zichem", "Holsbeek", "Aarschot", "Kortrijk", "Herselt",
	    "Wervik", "Leuven", "Brussel", "Mechelen", "Antwerpen", "Gent", "Tienen",
	    "Hasselt", "Sint-Truiden", "Westerlo", "Heist-op-den-Berg"
	]),
	(sciensano::Level::Province, vec![
	    "Vlaams-Brabant", "Antwerpen", "Limburg", "West-Vlaanderen", "Oost-Vlaanderen",
	    "Waals-Brabant", "Namen", "Luik", "Henegouwen", "Luxemburg"
	]),
	(sciensano::Level::Country, vec!["Belgium"])
    ];

    let data = sciensano::cases(&cache_path)?;

    for (level,mut regions) in belgium {

	regions.sort();

	graphs(&graph_path, &smoothings,
	       &format!("belgium-{}", level.name()), level.name(),
	       &regions.iter().map(|region| {
		   let series = sciensano::case_series(&data, |cs| level.filter(region, cs));
		   (*region, sciensano::case_dates().zip(interpolate(series)).collect())
	       }).collect())?;
	
    }
    
    Ok(())

}

fn graphs(graph_path: &Path, smoothings: &Vec<usize>,
	  group: &str, level: &str, data: &GraphData) -> Result<()> {
    graph::cases_graph(graph_path, group, level, &data)?;
    for smoothing in smoothings {
	graph::growth_graph(graph_path, group, level, *smoothing, &data.iter().map(
	    |(region,series)| (*region, growths(&average(&daily(series), *smoothing), *smoothing))
	).collect())?;
    }
    Ok(())
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


fn daily(data: &Series) -> Series {
    (1..data.len()).map(
	|i| (data[i].0, data[i].1 - data[i-1].1)
    ).collect()
}


fn growths(data: &Series, avg: usize) -> Series {
    (0..data.len()).map(
	|i| (data[i].0, match data[i].1 == 0.0 || data[i - avg.min(i)].1 == 0.0 {
	    true => 1.0,
	    false => (data[i].1 / data[i - avg.min(i)].1).powf(1.0 / avg as f64)
	})
    ).collect()
}


fn average(data: &Series, avg: usize) -> Series {
    let mut sum = 0.0;
    (0..data.len()).map(|i| {
	sum += data[i].1 - if i >= avg {data[i-avg].1} else {0.0};
	(data[i].0, sum / avg.min(i+1) as f64)
    }).collect()
}


fn sum_series(data: &Vec<&Series>) -> Series {
    let mut result = BTreeMap::new();
    for series in data {
	for (date,val) in series.iter() {
	    *result.entry(*date).or_insert(0.0) += *val;
	}
    }
    result.into_iter().collect()
}


pub struct NaiveDateRange(NaiveDate,Option<NaiveDate>);

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
