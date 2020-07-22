use std::io;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use chrono::naive::NaiveDate;
use serde_json::{Value,json};

use super::error::Result;


pub type Series = Vec<(NaiveDate,f64)>;
pub type GraphData = Vec<(&'static str,Series)>;


pub fn cases_graph(graph_path: &Path, group: &str, level: &str,
		   data: &GraphData) -> Result<()> {
    graph(graph_path, &format!("{}-absolute.html", group),
	  &format!("Number of confirmed COVID-19 cases by {}", level),
	  "Count", json!({"type":"log"}), vec![], data)
}


pub fn growth_graph(graph_path: &Path, group: &str, level: &str,
		    smoothing: usize, data: &GraphData) -> Result<()> {
    graph(graph_path, &format!("{}-growth-{}days.html", group, smoothing),
	  &format!("Average daily growth of {}-day average confirmed \
		    COVID-19 cases by {}", smoothing, level),
	  "Factor", json!({"domain":[0.5, 1.5]}), vec![1.0], data)
}


fn graph(graph_path: &Path, path: &str, title: &str, ytitle: &str, scale: Value,
	 refs: Vec<f64>, data: &Vec<(&str,Vec<(NaiveDate,f64)>)>) -> Result<()> {

    let mut out = io::BufWriter::new(File::create(graph_path.join(path))?);

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
