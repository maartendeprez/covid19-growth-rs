mod graph;
mod error;
mod sus;
mod csse;
mod sciensano;

use std::fs;
use std::path::{PathBuf,Path};
use std::collections::{BTreeMap,HashMap};

use chrono::naive::NaiveDate;
use serde_json::json;
use unidecode::unidecode;
use lazy_static::lazy_static;

use graph::{Series,CasesData,TestsData,Population,Refs};
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
	("europe", vec![
	    ("Italy",          60062012, vec!["Italy"]),
	    ("Spain",          47329981, vec!["Spain"]),
	    ("Belgium",        11535652, vec!["Belgium"]),
	    ("Netherlands",    17523131, vec!["Netherlands"]),
	    ("Romania",        19317984, vec!["Romania"]),
	    ("Switzerland",     8632703, vec!["Switzerland"]),
	    ("Austria",         8915382, vec!["Austria"]),
	    ("France",         67132000, vec!["France"]),
	    ("Germany",        83122889, vec!["Germany"]),
	    ("Sweden",         10367232, vec!["Sweden"]),
	    ("Norway",          5374807, vec!["Norway"]),
	    ("Finland",         5503335, vec!["Finland"]),
	    ("United Kingdom", 66796807, vec!["United Kingdom"]),
	    ("Portugal",       10295909, vec!["Portugal"]),
	]),
	("america", vec![
	    ("Brazil",   212245791, vec!["Brazil"]),
	    ("Chile",     19458310, vec!["Chile"]),
	    ("Peru",      32625948, vec!["Peru"]),
	    ("Argentina", 45376763, vec!["Argentina"]),
	    ("Ecuador",   17595980, vec!["Ecuador"]),
	    ("Bolivia",   11633371, vec!["Bolivia"]),
	    ("Colombia",  50372424, vec!["Colombia"]),
	    ("Mexico",   127792286, vec!["Mexico"]),
	    ("US",       330533177, vec!["US"]),
	    ("Canada",    38220052, vec![
		"Northwest Territories,Canada", "Saskatchewan,Canada",
		"Prince Edward Island,Canada", "Alberta,Canada",
		"Nova Scotia,Canada", "Yukon,Canada", "British Columbia,Canada",
		"Newfoundland and Labrador,Canada", "New Brunswick,Canada",
		"Ontario,Canada", "Quebec,Canada", "Manitoba,Canada"])]),
	("africa", vec![
	    ("South Africa",      59622350, vec!["South Africa"]),
	    ("Congo (Kinshasa)", 101935800, vec!["Congo (Kinshasa)"]),
	    ("Ghana",             30955202, vec!["Ghana"]),
	    ("Egypt",            101092069, vec!["Egypt"]),
	    ("Israel",             9268700, vec!["Israel"])]),
	("rest", vec![
	    ("South Korea",  51841786, vec!["Korea, South"]),
	    ("Japan",       125880000, vec!["Japan"]),
	    ("Russia",      146748590, vec!["Russia"]),
	    ("India",      1368830362, vec!["India"]),
	    ("China",      1405021280, vec![
		"Anhui,China", "Xinjiang,China", "Henan,China",
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
	    ("Australia",    25686212, vec![
		"South Australia,Australia",
		"Australian Capital Territory,Australia",
		"New South Wales,Australia",
		"Victoria,Australia",
		"Western Australia,Australia",
		"Queensland,Australia",
		"Northern Territory,Australia",
		"Tasmania,Australia"]),
	    ("Iran",         83893073, vec!["Iran"]),
	    ("Iraq",         40150200, vec!["Iraq"]),
	    ("Turkey",       83154997, vec!["Turkey"])])
    ];

    let data = csse::confirmed(&cache_path)?;

    for (group,mut regions) in groups {

	regions.sort();

	case_graphs(graph_path, smoothings,
		    &format!("csse/{}", group), "country", "confirmed COVID-19 cases",
		    &regions.iter().map(
			|(region,_,keys)| Ok((region.to_string(), sum_series(&keys.iter().map(
			    |key| data.get(*key).ok_or(Error::MissingRegion(*key))
			).collect::<Result<_>>()?)))
		    ).collect::<Result<_>>()?,
		    &regions.iter().map(
			|(region,population,_)| (*region, *population)
		    ).collect(), &vec![])?;

    }

    Ok(())

}


fn sciensano_muni_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let belgium = vec![
	(sciensano::Level::Municipality, vec![
	    ("Scherpenheuvel-Zichem", 23078),
	    ("Holsbeek",              10062),
	    ("Aarschot",              30183),
	    ("Kortrijk",              77109),
	    ("Herselt",               14521),
	    ("Wervik",                18909),
	    ("Leuven",               102275),
	    ("Brussel",              185103),
	    ("Mechelen",              86921),
	    ("Antwerpen",            529247),
	    ("Gent",                 263927),
	    ("Tienen",                35293),
	    ("Hasselt",               78714),
	    ("Sint-Truiden",          40672),
	    ("Westerlo",              25119),
	    ("Heist-op-den-Berg",     42950),
	])
    ];

    let refs = vec![
	(Some("Niveau 2"),  20.0 / 14.0),
	(Some("Niveau 3"), 120.0 / 14.0),
	(Some("Niveau 4"), 400.0 / 14.0)
    ];
    
    let data = sciensano::cases_muni(&cache_path)?;

    for (level,mut regions) in belgium {

	regions.sort();

	case_graphs(&graph_path, &smoothings,
		    &format!("belgium/cases/{}", level.name()), level.name(),
		    "confirmed COVID-19 cases",
		    &regions.iter().map(|(region,_)| {
			let series = sciensano::cases_muni_series(&data, |cs| level.filter_muni(region, cs));
			(region.to_string(), sciensano::cases_muni_dates().zip(interpolate(series)).collect())
		    }).collect(),
		    &regions.iter().map(
			|(region,population)| (*region, *population)
		    ).collect(), &refs)?;

    }

    Ok(())

}


lazy_static! {
    static ref POPULATION : HashMap<&'static str,Population> = vec![
	("country", vec![
	    ("Belgium", 11000638)
	].into_iter().collect()),
	("province", vec![
	    ("Antwerpen",      1869730),
	    ("BrabantWallon",   406019),
	    ("Brussels",       1218255),
	    ("Hainaut",        1346840),
	    ("Limburg",         877370),
	    ("Liège",          1109800),
	    ("Luxembourg",      286752),
	    ("Namur",           495832),
	    ("OostVlaanderen", 1525255),
	    ("VlaamsBrabant",  1155843),
	    ("WestVlaanderen", 1200945),
	].into_iter().collect()),
	("region", vec![
	    ("Brussels", 1218255),
	    ("Flanders", 6629143),
	    ("Wallonia", 3645243),
	].into_iter().collect()),
	("age", vec![
	    ("0-9",   1269068),
	    ("10-19", 1300254),
	    ("20-29", 1407645),
	    ("30-39", 1492290),
	    ("40-49", 1504539),
	    ("50-59", 1590628),
	    ("60-69", 1347139),
	    ("70-79",  924291),
	    ("80-89",  539390),
	    ("90+",    117397),
	].into_iter().collect())
    ].into_iter().collect();
}


fn sciensano_agesex_graphs(graph_path: &Path, cache_path: &Path, smoothings: &Vec<usize>) -> Result<()> {

    let data = sciensano::cases_agesex(&cache_path)?;

    let mut by_province = BTreeMap::new();
    let mut by_region = BTreeMap::new();
    let mut by_country = BTreeMap::new();
    let mut by_agegroup = BTreeMap::new();

    let refs = vec![
	(Some("Niveau 2"),  20.0 / 14.0),
	(Some("Niveau 3"), 120.0 / 14.0),
	(Some("Niveau 4"), 400.0 / 14.0)
    ];
    
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
		    &regions, &POPULATION[group],
		    &refs)?;
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

    let refs = vec![
	(Some("Niveau 3"),  3.5 / 7.0),
	(Some("Niveau 4"), 14.0 / 7.0)
    ];

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
		    ).collect(), &POPULATION[group], &refs)?;
    }

    for (group,regions) in hosp_groups {
	active_graphs(&graph_path, &smoothings,
		      &format!("belgium/hospitalizations/{}", group),
		      group, "COVID-19 hospitalizations net", &regions,
		      &POPULATION[group])?;
    }

    for (group,regions) in icu_groups {
	active_graphs(&graph_path, &smoothings,
		      &format!("belgium/hospitalizations-icu/{}", group),
		      group, "COVID-19 patients in icu", &regions,
		      &POPULATION[group])?;
    }

    Ok(())

}

fn sciensano_test_graphs(graph_path: &Path, cache_path: &Path,
			 smoothings: &Vec<usize>) -> Result<()> {

    let data = sciensano::tests(&cache_path)?;
    let mut by_province = BTreeMap::new();
    let mut by_region = BTreeMap::new();
    let mut by_country = BTreeMap::new();

    let refs = vec![
	(Some("Niveau 3"), 0.03),
	(Some("Niveau 4"), 0.06)
    ];
    
    for row in &data {
	let date = NaiveDate::parse_from_str(row.date.as_ref().map(|d| d.as_str())
					     .unwrap_or("2020-02-29"), "%Y-%m-%d")?;
	if let Some(province) = row.province.clone() {
	    let (pos,neg,all) = by_province.entry(province.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert((0.0,0.0,0.0));
	    *pos += row.tests_all_pos as f64;
	    *neg += (row.tests_all - row.tests_all_pos) as f64;
	    *all += row.tests_all as f64;
	}
	if let Some(region) = row.region.as_ref() {
	    let (pos,neg,all) = by_region.entry(region.clone()).or_insert_with(BTreeMap::new)
		.entry(date).or_insert((0.0,0.0,0.0));
	    *pos += row.tests_all_pos as f64;
	    *neg += (row.tests_all - row.tests_all_pos) as f64;
	    *all += row.tests_all as f64;
	}
	let (pos,neg,all) = by_country.entry(date).or_insert((0.0,0.0,0.0));
	*pos += row.tests_all_pos as f64;
	*neg += (row.tests_all - row.tests_all_pos) as f64;
	*all += row.tests_all as f64;
    }

    let date_range = NaiveDateRange(*by_country.keys().min().ok_or(Error::MissingData)?,
				    Some(*by_country.keys().max().ok_or(Error::MissingData)?));

    let groups : Vec<(&str,Vec<(String,_)>)> = vec![
	("region", by_region.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or((0.0,0.0,0.0)))
	    ).collect())
	).collect()),
	("province", by_province.into_iter().map(
	    |(key,mut series)| (key, date_range.clone().map(
		|date| (date, series.remove(&date).unwrap_or((0.0,0.0,0.0)))
	    ).collect())
	).collect())
    ];

    test_graphs(&graph_path, &smoothings, "belgium/tests/country", "Belgium",
		&date_range.clone().map(
		    |date| (date.clone(), by_country.remove(&date).unwrap_or((0.0,0.0,0.0)))
		).collect(), &refs)?;

    for (group,regions) in groups {
	test_graphs_regions(&graph_path, &smoothings,
			    &format!("belgium/tests/{}", group),
			    group, &regions, &refs)?;
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

    let municipios = [
	("CE", "Fortaleza"),
	("CE", "Canindé"),
	("MT", "Barra do Garças")
    ];

    let data : Vec<(String,TestsData)> = estados.iter().filter_map(
	|(codigo,estado)| match sus::tests(estado, None, &codigo.to_lowercase()) {
	    Ok(data) => Some((estado.to_string(), data)),
	    Err(_) => { println!("Warning: query for {} failed!", estado); None }
	}
    ).collect();

    let muni_data : Vec<(String,TestsData)> = municipios.iter().filter_map(
	|(index,muni)| match sus::tests(muni, Some(json!({"term": {"municipio": muni}})), &index.to_lowercase()) {
	    Ok(data) => Some((muni.to_string(), data)),
	    Err(_) => { println!("Warning: query for {} failed!", muni); None }
	}
    ).collect();

    let mut summed_data = BTreeMap::new();

    for (estado,data) in data.iter() {

	test_graphs(&graph_path, &smoothings, &format!("brazil/estados/{}", unidecode(estado)),
		    estado, data, &vec![])?;

	for (date,(pos,neg,all)) in data {
	    let sum = summed_data.entry(date.clone()).or_insert((0.0,0.0,0.0));
	    sum.0 += pos;
	    sum.1 += neg;
	    sum.2 += all;
	}

    }

    let date_range = NaiveDateRange(*summed_data.keys().min().ok_or(Error::MissingData)?,
				    Some(*summed_data.keys().max().ok_or(Error::MissingData)?));
    
    test_graphs_regions(&graph_path, &smoothings, "brazil/pais",
			"Brazil", &data, &vec![])?;
    test_graphs(&graph_path, &smoothings, "brazil/pais",
		"Brazil", &date_range.map(
		    |date| (date.clone(), summed_data.remove(&date).unwrap_or((0.0,0.0,0.0)))
		).collect(), &vec![])?;

    for (muni,data) in muni_data.iter() {
	test_graphs(&graph_path, &smoothings, &format!("brazil/municipios/{}", unidecode(muni)),
		    muni, data, &vec![])?;
    }

    Ok(())

}

fn case_graphs(graph_path: &Path, smoothings: &Vec<usize>, group: &str,
	       level: &str, var: &str, data: &CasesData,
	       population: &Population, refs: &Refs) -> Result<()> {
    graph::cases_graph(graph_path, group, level, var,
		       &json!({"type":"log"}), &vec![], &data)?;
    graph::relative_graph(graph_path, group, level, var,
			  &json!({"type":"log"}), &vec![],
			  &data.iter().map(|(region,series)| (region.clone(), incidence(series, population[region.as_str()]))
			  ).collect())?;
    for smoothing in smoothings {
	graph::daily_graph(graph_path, group, level, var, &vec![], *smoothing, &data.iter().map(
	    |(region,series)| (region.clone(), average(&daily(series), *smoothing))
	).collect())?;
	graph::incidence_graph(graph_path, group, level, var, &refs.iter().map(|(n,r)| (*n, *r * *smoothing as f64)).collect(), *smoothing, &data.iter().map(
	    |(region,series)| (region.clone(), sum(&daily(&incidence(series, population[region.as_str()])), *smoothing))
	).collect())?;
	if *smoothing != 1 {
	    graph::growth_graph(graph_path, group, level, var, *smoothing, &data.iter().map(
		|(region,series)| (region.clone(), growths(&average(&daily(series), *smoothing), *smoothing))
	    ).collect())?;
	}
    }
    Ok(())
}

fn active_graphs(graph_path: &Path, smoothings: &Vec<usize>, group: &str,
		 level: &str, var: &str, data: &CasesData,
		 population: &Population) -> Result<()> {
    graph::cases_graph(graph_path, group, level, var, &json!({}),
		       &vec![(None, 0.0)], &data)?;
    graph::relative_graph(graph_path, group, level, var, &json!({}),
			  &vec![(None, 0.0)], &data.iter().map(
			      |(region, series)| (region.clone(), incidence(series, population[region.as_str()]))
			  ).collect())?;
    for smoothing in smoothings {
	graph::daily_graph(graph_path, group, level, var, &vec![(None, 0.0)], *smoothing, &data.iter().map(
	    |(region,series)| (region.clone(), average(&daily(series), *smoothing))
	).collect())?;
	graph::incidence_graph(graph_path, group, level, var, &vec![], *smoothing, &data.iter().map(
	    |(region, series)| (region.clone(), sum(&daily(&incidence(series, population[region.as_str()])), *smoothing))
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
	       group: &str, region: &str, data: &TestsData,
	       refs: &Refs) -> Result<()> {

    for smoothing in smoothings {
	graph::tests_graph(graph_path, group, region, *smoothing,
			   &average_tests(data, *smoothing),
			   refs)?;
    }

    Ok(())

}

fn test_graphs_regions(graph_path: &Path, smoothings: &Vec<usize>, group: &str,
		       level: &str, data: &Vec<(String,TestsData)>,
		       refs: &Refs) -> Result<()> {

    for smoothing in smoothings {
	let averaged_data = data.iter().map(
	    |(region,data)| (region.clone(), average_tests(data, *smoothing))
	).collect();
	graph::test_positivity_graph(graph_path, group, level, *smoothing,
				     &averaged_data, refs)?;
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


fn incidence(data: &Series, population: u64) -> Series {
    data.iter().map(
	|(date,n)| (*date, *n * 100000.0 / population as f64)
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


fn sum(data: &Series, len: usize) -> Series {
    let mut sum = 0.0;
    (0..data.len()).map(|i| {
	sum += data[i].1 - if i >= len {data[i-len].1} else {0.0};
	(data[i].0, sum)
    }).collect()
}


fn average_tests(data: &TestsData, avg: usize) -> TestsData {
    let mut pos = 0.0;
    let mut neg = 0.0;
    let mut all = 0.0;
    (0..data.len()).map(|i| {
	pos += (data[i].1).0 - if i >= avg {(data[i-avg].1).0} else {0.0};
	neg += (data[i].1).1 - if i >= avg {(data[i-avg].1).1} else {0.0};
	all += (data[i].1).2 - if i >= avg {(data[i-avg].1).2} else {0.0};
	(data[i].0, (pos / avg.min(i+1) as f64,
		     neg / avg.min(i+1) as f64,
		     all / avg.min(i+1) as f64,))
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
