use std::{io,fs};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::collections::HashMap;

use chrono::naive::NaiveDate;
use serde_json::{Value,json};

use super::error::Result;


pub type Series = Vec<(NaiveDate,f64)>;
pub type CasesData = Vec<(String,Series)>;
pub type TestsData = Vec<(NaiveDate,(f64,f64,f64))>;
pub type Population = HashMap<&'static str,u64>;
pub type Refs = Vec<(Option<&'static str>, f64)>;

pub fn cases_graph(graph_path: &Path, group: &str, level: &str,
		   var: &str, scale: &Value, refs: &Refs,
		   data: &CasesData) -> Result<()> {
    let graph_path = graph_path.join(group);
    graph(&graph_path, "absolute.html",
	  &format!("Number of total {} by {}", var, level),
	  "Count", scale, refs, data)
}

pub fn relative_graph(graph_path: &Path, group: &str, level: &str,
		      var: &str, scale: &Value, refs: &Refs,
		      data: &CasesData) -> Result<()> {
    let graph_path = graph_path.join(group);
    graph(&graph_path, "relative.html",
	  &format!("Number of total {} per 100k by {}", var, level),
	  "Count / 100k", scale, refs, data)
}


pub fn daily_graph(graph_path: &Path, group: &str, level: &str, var: &str, refs: &Refs,
		   smoothing: usize, data: &CasesData) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("daily.html"),
	n => format!("daily-{}days.html", n),
    };
    let title = match smoothing {
	1 => format!("Number of daily {} by {}", var, level),
	n => format!("{}-day average number of daily {} by {}",
		     n, var, level),
    };
    graph(&graph_path, &filename, &title, "Count",
	  &json!({}), refs, data)
}

pub fn incidence_graph(graph_path: &Path, group: &str, level: &str, var: &str, refs: &Refs,
		       smoothing: usize, data: &CasesData) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("incidence.html"),
	n => format!("incidence-{}days.html", n),
    };
    let title = match smoothing {
	n => format!("{}-day incidence of {} by {}",
		     n, var, level),
    };
    graph(&graph_path, &filename, &title, "Incidence",
	  &json!({}), refs, data)
}


pub fn growth_graph(graph_path: &Path, group: &str, level: &str,
		    var: &str, smoothing: usize, data: &CasesData) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("growth.html"),
	n => format!("growth-{}days.html", n),
    };
    let title = match smoothing {
	1 => format!("Daily growth of {} by {}", var, level),
	n => format!("Average daily growth of {}-day average {} by {}",
		     n, var, level)
    };
    graph(&graph_path, &filename, &title, "Factor",
	  &json!({"domain":[0.5, 1.5]}), &vec![(None, 1.0)], data)
}


pub fn tests_graph(graph_path: &Path, group: &str, region: &str,
		   smoothing: usize, data: &TestsData,
		   refs: &Refs) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("tests.html"),
	n => format!("tests-{}days.html", n),
    };
    let title = match smoothing {
	1 => format!("Evolution of COVID-19 test results ({})", region),
	n => format!("{}-day averaged evolution of COVID-19 \
		      test results ({})", n, region)
    };
    graph_tests(&graph_path, &filename, &title, data, refs)
}

pub fn test_positivity_graph(graph_path: &Path, group: &str, level: &str,
			     smoothing: usize, data: &Vec<(String,TestsData)>,
			     refs: &Refs) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("positive-tests.html"),
	n => format!("positive-tests-{}days.html", n),
    };
    let title = match smoothing {
	1 => format!("Evolution of COVID-19 test positivity ratio by {}", level),
	n => format!("{}-day averaged evolution of COVID-19 \
		      test positivity ratio by {}", n, level)
    };
    graph(&graph_path, &filename, &title, "Proportion of positive tests",
	  &json!({"domain":[0.0, 1.0]}), refs, &data.iter().map(
	      |(region,series)| (region.clone(), series.iter().map(
		  |(date,(pos,neg,_all))| (date.clone(), pos / (pos + neg))
	      ).collect())
	  ).collect())
}

pub fn total_tests_graph(graph_path: &Path, group: &str, level: &str,
			 smoothing: usize, data: &Vec<(String,TestsData)>) -> Result<()> {
    let graph_path = graph_path.join(group);
    let filename = match smoothing {
	1 => format!("total-tests.html"),
	n => format!("total-tests-{}days.html", n),
    };
    let title = match smoothing {
	1 => format!("Evolution of COVID-19 test count by {}", level),
	n => format!("{}-day averaged evolution of COVID-19 \
		      test count by {}", n, level)
    };
    graph(&graph_path, &filename, &title, "Number of tests",
	  &json!({}), &vec![], &data.iter().map(
	      |(region,series)| (region.clone(), series.iter().map(
		  |(date,(_pos,_neg,all))| (date.clone(), *all)
	      ).collect())
	  ).collect())
}


fn graph(graph_path: &Path, path: &str, title: &str, ytitle: &str,
	 scale: &Value, refs: &Refs, data: &CasesData) -> Result<()> {

    fs::create_dir_all(graph_path)?;
    let mut out = io::BufWriter::new(File::create(graph_path.join(path))?);

    write!(out, "<!DOCTYPE html><html><head>")?;
    write!(out, "<meta charset=\"UTF-8\">")?;
    write!(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")?;
    write!(out, "<title>{}</title>", title)?;
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
		    move |(date,val)| match val.is_finite() {
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
		"data": {
		    "values": refs.iter().map(
			|(name,y)| json!({
			    "Name": name.unwrap_or(""),
			    "Value": y
			})).collect::<Vec<_>>()
		},
		"encoding": {
		    "y": {
			"field":"Value",
			"type":"quantitative"
		    }
		},
		"layer": [
		    {
			"mark": {
			    "color": "red",
			    "opacity": 0.5,
			    "size": 1,
			    "type":"rule"
			}
		    },
		    {
			"mark": {
			    "type": "text",
			    "color": "red"
			},
			"encoding": {
			    "text": {"field": "Name"}
			}
		    }
		]
	    }
	]
    }))?;

    write!(out, ";vegaEmbed('#vis', spec,{{}}).then(function(result) {{")?;
    write!(out, "}}).catch(console.error);")?;
    write!(out, "</script>")?;
    write!(out, "</body></html>")?;

    Ok(())

}


fn graph_tests(graph_path: &Path, path: &str, title: &str,
	       data: &TestsData, refs: &Refs) -> Result<()> {

    fs::create_dir_all(graph_path)?;
    let mut out = io::BufWriter::new(File::create(graph_path.join(path))?);

    write!(out, "<!DOCTYPE html><html><head>")?; 
    write!(out, "<meta charset=\"UTF-8\">")?;
    write!(out, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")?;
    write!(out, "<title>{}</title>", title)?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>")?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega-lite@4\"></script>")?;
    write!(out, "<script src=\"https://cdn.jsdelivr.net/npm/vega-embed\"></script>")?;
    write!(out, "</head>")?;
    write!(out, "<body>")?;
    write!(out, "<div id=\"vis\" style=\"overflow: hidden; position: absolute;top: 0; left: 0; right: 0; bottom: 0;\"></div>")?;
    write!(out, "<script type=\"text/javascript\">")?;
    write!(out, "var spec = ")?;

    serde_json::to_writer_pretty(out.by_ref(), &json!({
	"height": "container",
	"width": "container",
	"$schema": "https://vega.github.io/schema/vega-lite/v4.json",
	"title": title,
	"layer": [
	    {
		"data": {
		    "values": data.iter().filter_map(
			|(date,(pos,neg,all))| match *pos + *neg == 0.0 {
			    true => None,
			    false => Some(json!({
				"Date": format!("{}", date.format("%Y-%m-%d")),
				"Total": all,
				"Positive": 1f64.min(pos / (pos + neg))
			    }))
			}
		    ).collect::<Vec<_>>(),
		},
		"encoding": {
		    "x": {
			"field": "Date",
			"timeUnit": "utcyearmonthdate",
			"title": "Date",
			"type": "temporal"
		    }
		},
		"resolve": {
		    "scale": {
			"y": "independent"
		    }
		},
		"layer": [
		    {
			"mark": {
			    "color": "red",
			    "type": "line"
			},
			"selection": {
			    "Grid1": {"bind":"scales","type":"interval"}
			},
			"encoding": {
			    "y": {
				"field": "Positive",
				"scale": {
				    "domain": [
					0,
					1
				    ],
				    "type": "linear"
				},
				"type": "quantitative",
				"axis": {
				    "titleColor": "red",
				    "title": "Proportion of positive tests"
				}
			    }
			}
		    },
		    {
			"mark": {
			    "color": "blue",
			    "type": "line"
			},
			"selection": {
			    "Grid2": {"bind":"scales","type":"interval"}
			},
			"encoding": {
			    "y": {
				"field": "Total",
				"scale": {
				    "type": "linear",
				    "domainMin": 0
				},
				"type": "quantitative",
				"axis": {
				    "titleColor": "blue",
				    "title": "Total number of tests"
				}
			    }
			}
		    },
		    {
			"mark": {
			    "color": "gray",
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
			    "tooltip": [
				{"field": "Date", "type": "temporal"},
				{"field": "Total", "type": "quantitative",
				 "format": ".0f"},
				{"field": "Positive", "type": "quantitative",
				 "format": ".3f"}
			    ]
			}
		    }
		]
	    },
	    {
		"data": {
		    "values": refs.iter().map(
			|(name,y)| json!({
			    "Name": name.unwrap_or(""),
			    "Value": y
			})).collect::<Vec<_>>()
		},
		"encoding": {
		    "y": {
			"field":"Value",
			"type":"quantitative",
			"scale": {
			    "domain": [0, 1]
			},
			"axis": {
			    "title": null
			}
		    }
		},
		"layer": [
		    {
			"mark": {
			    "color": "red",
			    "opacity": 0.5,
			    "size": 1,
			    "type":"rule"
			},
			"selection": {
			    "Grid3": {"bind":"scales","type":"interval"}
			}
		    },
		    {
			"mark": {
			    "type": "text",
			    "color": "red"
			},
			"encoding": {
			    "text": {
				"field": "Name"
			    }
			}
		    }
		]
	    }
	]
    }))?;

    write!(out, ";vegaEmbed('#vis', spec,{{}}).then(function(result) {{")?;
    write!(out, "}}).catch(console.error);")?;
    write!(out, "</script>")?;
    write!(out, "</body></html>")?;

    Ok(())

}
