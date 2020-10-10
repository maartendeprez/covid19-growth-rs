mod graph;
mod error;
mod sus;
mod csse;
mod sciensano;

use std::fs;
use std::path::{PathBuf,Path};
use std::collections::BTreeMap;

use chrono::naive::NaiveDate;
use serde_json::json;
use unidecode::unidecode;

use graph::{Series,CasesData,TestsData};
use error::{Result,Error};


fn main() -> Result<()> {

    let graph_path = PathBuf::from("graphs");
    let cache_path = PathBuf::from("cache");
    let smoothings = vec![1,7,14];

    fs::create_dir_all(&graph_path)?;

    if let Err(err) = csse_graphs(&graph_path, &cache_path, &smoothings) {
	eprintln!("Error: csse graphs: {}", err);
    }

    if let Err(err) = sciensano_muni_graphs(&graph_path, &cache_path, &smoothings) {
	eprintln!("Error: sciensano municipality graphs: {}", err);
    }

    if let Err(err) = sciensano_agesex_graphs(&graph_path, &cache_path, &smoothings) {
	eprintln!("Error: sciensano agesex graphs: {}", err);
    }

    if let Err(err) = sciensano_hospitalization_graphs(&graph_path, &cache_path, &smoothings) {
	eprintln!("Error: sciensano hospitalization graphs: {}", err);
    }
    
    if let Err(err) = sciensano_test_graphs(&graph_path, &cache_path, &smoothings) {
	eprintln!("Error: sciensano test graphs: {}", err);
    }

    if let Err(err) = sus_test_graphs(&graph_path, &smoothings) {
	eprintln!("Error: sus test graphs: {}", err);
    }

    Ok(())
    
}


fn csse_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let groups = vec![
	("europe", vec![("Italy", vec!["Italy"]),
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

	case_graphs(graph_path, smoothings,
		    &format!("csse/{}", group), "country", "confirmed COVID-19 cases",
		    &regions.iter().map(
			|(region,keys)| Ok((region.to_string(), sum_series(&keys.iter().map(
			    |key| data.get(*key).ok_or(Error::MissingRegion(*key))
			).collect::<Result<_>>()?)))
		    ).collect::<Result<_>>()?)?;

    }

    Ok(())

}


fn sciensano_muni_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let belgium = vec![
	(sciensano::Level::Municipality, vec![
	    "Scherpenheuvel-Zichem", "Holsbeek", "Aarschot", "Kortrijk", "Herselt",
	    "Wervik", "Leuven", "Brussel", "Mechelen", "Antwerpen", "Gent", "Tienen",
	    "Hasselt", "Sint-Truiden", "Westerlo", "Heist-op-den-Berg"
	])
    ];

    let data = sciensano::cases_muni(&cache_path)?;

    for (level,mut regions) in belgium {

	regions.sort();

	case_graphs(&graph_path, &smoothings,
		    &format!("belgium/cases/{}", level.name()), level.name(),
		    "confirmed COVID-19 cases",
		    &regions.iter().map(|region| {
			let series = sciensano::cases_muni_series(&data, |cs| level.filter_muni(region, cs));
			(region.to_string(), sciensano::cases_muni_dates().zip(interpolate(series)).collect())
		    }).collect())?;

    }

    Ok(())

}


fn sciensano_agesex_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let data = sciensano::cases_agesex(&cache_path)?;

    let mut by_province = BTreeMap::new();
    let mut by_region = BTreeMap::new();
    let mut by_country = BTreeMap::new();
    let mut by_agegroup = BTreeMap::new();

    for row in &data {
	let date = NaiveDate::parse_from_str(row.date.as_ref().map(|d| d.as_str())
					     .unwrap_or("2020-02-29"), "%Y-%m-%d")?;
	if let Some(province) = row.province.clone() {
	    *by_province.entry(province).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.cases as f64;
	}
	if let Some(region) = row.region.clone() {
	    *by_region.entry(region).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.cases as f64;
	}
	if let Some(agegroup) = row.agegroup.clone() {
	    *by_agegroup.entry(agegroup).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.cases as f64;
	}
	*by_country.entry(date).or_insert(0.0) += row.cases as f64;
    }

    let date_range = NaiveDateRange(*by_country.keys().min().ok_or(Error::MissingData)?,
				    Some(*by_country.keys().max().ok_or(Error::MissingData)?));
    let groups = vec![
	("country", vec![("Belgium".to_string(), date_range.clone().scan(
	    0.0, |sum,date| { *sum += by_country.remove(&date).unwrap_or(0.0);
			       Some((date, *sum)) }).collect())]),
	("province", by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().scan(
		0.0, |sum,date| { *sum += series.remove(&date).unwrap_or(0.0);
				   Some((date, *sum)) }).collect())
	).collect()),
	("region", by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().scan(
		0.0, |sum,date| { *sum += series.remove(&date).unwrap_or(0.0);
				   Some((date, *sum)) }).collect())
	).collect()),
	("age", by_agegroup.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().scan(
		0.0, |sum,date| { *sum += series.remove(&date).unwrap_or(0.0);
				   Some((date, *sum)) }).collect())
	).collect())
    ];

    for (group,regions) in groups {
	//regions.sort();
	case_graphs(&graph_path, &smoothings,
		    &format!("belgium/cases/{}", group),
		    group, "confirmed COVID-19 cases",
		    &regions)?;
    }

    Ok(())

}


fn sciensano_hospitalization_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let data = sciensano::hospitalizations(&cache_path)?;

    let mut hosp_in_by_province = BTreeMap::new();
    let mut hosp_in_by_region = BTreeMap::new();
    let mut hosp_in_by_country = BTreeMap::new();
    let mut hosp_by_province = BTreeMap::new();
    let mut hosp_by_region = BTreeMap::new();
    let mut hosp_by_country = BTreeMap::new();
    let mut icu_by_province = BTreeMap::new();
    let mut icu_by_region = BTreeMap::new();
    let mut icu_by_country = BTreeMap::new();

    for row in &data {

	let date = NaiveDate::parse_from_str(row.date.as_ref().map(|d| d.as_str())
					     .unwrap_or("2020-02-29"), "%Y-%m-%d")?;

	if let Some(province) = row.province.clone() {
	    *hosp_in_by_province.entry(province.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.new_in as f64;
	    *hosp_by_province.entry(province.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.total_in as f64;
	    *icu_by_province.entry(province).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.total_in_icu as f64;
	}

	if let Some(region) = row.region.clone() {
	    *hosp_in_by_region.entry(region.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.new_in as f64;
	    *hosp_by_region.entry(region.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.total_in as f64;
	    *icu_by_region.entry(region).or_insert_with(BTreeMap::new)
		.entry(date).or_insert(0.0) += row.total_in_icu as f64;
	}

	*hosp_in_by_country.entry(date).or_insert(0.0) += row.new_in as f64;
	*hosp_by_country.entry(date).or_insert(0.0) += row.total_in as f64;
	*icu_by_country.entry(date).or_insert(0.0) += row.total_in_icu as f64;

    }

    let date_range = NaiveDateRange(*hosp_by_country.keys().min().ok_or(Error::MissingData)?,
				    Some(*hosp_by_country.keys().max().ok_or(Error::MissingData)?));

    let hosp_in_groups = vec![
	("country", vec![("Belgium".to_string(), date_range.clone().map(
	    |date| (date, hosp_in_by_country.remove(&date).unwrap_or(0.0))
	).collect())]),
	("province", hosp_in_by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect()),
	("region", hosp_in_by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect())
    ];

    let hosp_groups = vec![
	("country", vec![("Belgium".to_string(), date_range.clone().map(
	    |date| (date, hosp_by_country.remove(&date).unwrap_or(0.0))
	).collect())]),
	("province", hosp_by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect()),
	("region", hosp_by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect())
    ];

    let icu_groups = vec![
	("country", vec![("Belgium".to_string(), date_range.clone().map(
	    |date| (date, icu_by_country.remove(&date).unwrap_or(0.0))
	).collect())]),
	("province", icu_by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect()),
	("region", icu_by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or(0.0))
	    ).collect())
	).collect())
    ];

    for (group,regions) in hosp_in_groups {
	case_graphs(&graph_path, &smoothings,
		    &format!("belgium/hospitalizations-in/{}", group),
		    group, "COVID-19 hospitalizations in", &regions.iter().map(
			|(k,v)| (k.clone(), cumsum(v))
		    ).collect())?;
    }

    for (group,regions) in hosp_groups {
	active_graphs(&graph_path, &smoothings,
		      &format!("belgium/hospitalizations/{}", group),
		      group, "COVID-19 hospitalizations net", &regions)?;
    }

    for (group,regions) in icu_groups {
	active_graphs(&graph_path, &smoothings,
		      &format!("belgium/hospitalizations-icu/{}", group),
		      group, "COVID-19 patients in icu", &regions)?;
    }

    Ok(())

}

fn sciensano_test_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let data = sciensano::tests(&cache_path)?;
    let mut by_province = BTreeMap::new();
    let mut by_region = BTreeMap::new();
    let mut by_country = BTreeMap::new();
    
    for row in &data {
	let date = NaiveDate::parse_from_str(row.date.as_ref().map(|d| d.as_str())
					     .unwrap_or("2020-02-29"), "%Y-%m-%d")?;
	if let Some(province) = row.province.clone() {
	    let ent = by_province.entry(province.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert((0.0,0.0));
	    ent.0 += row.tests_all_pos as f64;
	    ent.1 += row.tests_all as f64;
	}
	if let Some(region) = row.region.as_ref() {
	    let ent = by_region.entry(region.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert((0.0,0.0));
	    ent.0 += row.tests_all_pos as f64;
	    ent.1 += row.tests_all as f64;
	}
	let ent = by_country.entry(date).or_insert((0.0,0.0));
	ent.0 += row.tests_all_pos as f64;
	ent.1 += row.tests_all as f64;
    }

    let date_range = NaiveDateRange(*by_country.keys().min().ok_or(Error::MissingData)?,
				    Some(*by_country.keys().max().ok_or(Error::MissingData)?));

    let groups : Vec<(&str,Vec<(String,_)>)> = vec![
	("region", by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or((0.0,0.0)))
	    ).collect())
	).collect()),
	("province", by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or((0.0,0.0)))
	    ).collect())
	).collect())
    ];

    test_graphs(&graph_path, &smoothings, "belgium/tests/country", "Belgium",
		&date_range.clone().map(
		    |date| (date.clone(), by_country.remove(&date).unwrap_or((0.0,0.0)))
		).collect())?;

    for (group,regions) in groups {
	test_graphs_regions(&graph_path, &smoothings,
			    &format!("belgium/tests/{}", group),
			    group, &regions)?;
    }

    Ok(())

}

fn sus_test_graphs(graph_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let estados = vec![
	("AC", "Acre"),
	("AL", "Alagoas"),
	("AP", "Amapá"),
	("AM", "Amazonas"),
	("BA", "Bahia"),
	("CE", "Ceará"),
	("DF", "Distrito Federal"),
	("ES", "Espírito Santo"),
	("GO", "Goiás"),
	("MA", "Maranhão"),
	("MT", "Mato Grosso"),
	("MS", "Mato Grosso do Sul"),
	("MG", "Minas Gerais"),
	("PA", "Pará"),
	("PB", "Paraíba"),
	("PR", "Paraná"),
	("PE", "Pernambuco"),
	("PI", "Piauí"),
	("RJ", "Rio de Janeiro"),
	("RN", "Rio Grande do Norte"),
	("RS", "Rio Grande do Sul"),
	("RO", "Rondônia"),
	("RR", "Roraima"),
	("SC", "Santa Catarina"),
	("SP", "São Paulo"),
	("SE", "Sergipe"),
	("TO", "Tocantins")
    ];

    let data : Vec<(String,TestsData)> = estados.iter().filter_map(
	|(codigo,estado)| match sus::tests(estado, None, &codigo.to_lowercase()) {
	    Ok(data) => Some((estado.to_string(), data)),
	    Err(_) => { println!("Warning: query for {} failed!", estado); None }
	}).collect();

    let mut summed_data = BTreeMap::new();

    for (estado,data) in data.iter() {

	test_graphs(&graph_path, &smoothings, &format!("brazil/estados/{}", unidecode(estado)),
		    estado, data)?;

	for (date,ent) in data {
	    let sum = summed_data.entry(date.clone()).or_insert((0.0,0.0));
	    sum.0 += ent.0;
	    sum.1 += ent.1;
	}

    }

    let date_range = NaiveDateRange(*summed_data.keys().min().ok_or(Error::MissingData)?,
				    Some(*summed_data.keys().max().ok_or(Error::MissingData)?));
    
    test_graphs_regions(&graph_path, &smoothings, "brazil/pais",
			"Brazil", &data)?;
    test_graphs(&graph_path, &smoothings, "brazil/pais",
		"Brazil", &date_range.map(
		    |date| (date.clone(), summed_data.remove(&date).unwrap_or((0.0,0.0)))
		).collect())?;

    Ok(())

}

fn case_graphs(graph_path: &Path, smoothings: &Vec<usize>, group: &str, level: &str,
	       var: &str, data: &CasesData) -> Result<()> {
    graph::cases_graph(graph_path, group, level, var, &json!({"type":"log"}), &vec![], &data)?;
    for smoothing in smoothings {
	graph::daily_graph(graph_path, group, level, var, &vec![], *smoothing, &data.iter().map(
	    |(region,series)| (region.clone(), average(&daily(series), *smoothing))
	).collect())?;
	if *smoothing != 1 {
	    graph::growth_graph(graph_path, group, level, var, *smoothing, &data.iter().map(
		|(region,series)| (region.clone(), growths(&average(&daily(series), *smoothing), *smoothing))
	    ).collect())?;
	}
    }
    Ok(())
}

fn active_graphs(graph_path: &Path, smoothings: &Vec<usize>, group: &str, level: &str,
		 var: &str, data: &CasesData) -> Result<()> {
    graph::cases_graph(graph_path, group, level, var, &json!({}), &vec![0.0], &data)?;
    for smoothing in smoothings {
	graph::daily_graph(graph_path, group, level, var, &vec![0.0], *smoothing, &data.iter().map(
	    |(region,series)| (region.clone(), average(&daily(series), *smoothing))
	).collect())?;
	if *smoothing != 1 {
	    graph::growth_graph(graph_path, group, level, var, *smoothing, &data.iter().map(
		|(region,series)| (region.clone(), growths(&average(series, *smoothing), *smoothing))
	    ).collect())?;
	}
    }
    Ok(())
}

fn test_graphs(graph_path: &Path, smoothings: &Vec<usize>,
	       group: &str, region: &str, data: &TestsData) -> Result<()> {

    for smoothing in smoothings {
	graph::tests_graph(graph_path, group, region, *smoothing,
			   &average_tests(data, *smoothing))?;
    }

    Ok(())

}

fn test_graphs_regions(graph_path: &Path, smoothings: &Vec<usize>, group: &str,
		       level: &str, data: &Vec<(String,TestsData)>) -> Result<()> {

    for smoothing in smoothings {
	let averaged_data = data.iter().map(
	    |(region,data)| (region.clone(), average_tests(data, *smoothing))
	).collect();
	graph::test_positivity_graph(graph_path, group, level, *smoothing,
				     &averaged_data)?;
	graph::total_tests_graph(graph_path, group, level, *smoothing,
				 &averaged_data)?;
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

fn cumsum(data: &Series) -> Series {
    let mut sum = 0.0;
    data.into_iter().map(
	|(k,v)| {sum += v; (k.clone(), sum)}
    ).collect()
}


fn daily(data: &Series) -> Series {
    (1..data.len()).map(
	|i| (data[i].0, data[i].1 - data[i-1].1)
    ).collect()
}


fn growths(data: &Series, avg: usize) -> Series {
    (0..data.len()).map(
	|i| {
	    let f = (data[i].1 / data[i - avg.min(i)].1).powf(1.0 / avg as f64);
	    (data[i].0, match /* !f.is_normal() || */ f == 0.0 {
		true => 1.0,
		false => f
	    })
	}
    ).collect()
}


fn average(data: &Series, avg: usize) -> Series {
    let mut sum = 0.0;
    (0..data.len()).map(|i| {
	sum += data[i].1 - if i >= avg {data[i-avg].1} else {0.0};
	(data[i].0, sum / avg.min(i+1) as f64)
    }).collect()
}


fn average_tests(data: &TestsData, avg: usize) -> TestsData {
    let mut cases = 0.0;
    let mut tests = 0.0;
    (0..data.len()).map(|i| {
	cases += (data[i].1).0 - if i >= avg {(data[i-avg].1).0} else {0.0};
	tests += (data[i].1).1 - if i >= avg {(data[i-avg].1).1} else {0.0};
	(data[i].0, (cases / avg.min(i+1) as f64,
		     tests / avg.min(i+1) as f64))
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

#[derive(Clone,Debug)]
pub struct NaiveDateRange(NaiveDate,Option<NaiveDate>);

impl Iterator for NaiveDateRange {
    type Item = NaiveDate;
    fn next(&mut self) -> Option<NaiveDate> {
	match self.1.map_or(true, |end| self.0 <= end) {
	    false => None,
	    true => {
		let current = self.0;
		self.0 = self.0.succ();
		Some(current)
	    }
	}
    }
}
