use std::collections::BTreeMap;
use chrono::{NaiveDateTime,Utc};
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use super::graph::TestsData;
use super::error::{Result,Error};
use super::NaiveDateRange;


#[derive(Deserialize)]
#[serde(untagged)]
enum ESResult<T> {
    Success(ESResponse<T>),
    Unexpected(Value)
}

#[derive(Deserialize)]
struct ESResponse<T> {
    #[serde(flatten)]
    sub: T
}

#[derive(Deserialize)]
struct ESAggr<T> {
    aggregations: T
}

#[derive(Deserialize)]
struct ESBuckets<K, T = Null> {
    buckets: Vec<ESBucket<K,T>>
}

#[derive(Deserialize)]
struct ESBucket<K, T = Null> {
    //key_as_string: Option<String>,
    key: K,
    doc_count: usize,
    #[serde(flatten)]
    sub: T
}

#[derive(Deserialize)]
struct Null {
}

#[derive(Deserialize)]
struct DateAggr<T = Null> {
    date: ESBuckets<i64,T>,
}

#[derive(Deserialize)]
struct ResultsAggr<T = Null> {
    resultados: ESBuckets<TestResult,T>
}

#[derive(Deserialize)]
enum TestResult {
    Descartado,
    #[serde(alias = "Confirmado Laboratorial")]
    #[serde(alias = "Confirmação Laboratorial")]
    #[serde(alias = "Confirmado por Critério Clínico")]
    #[serde(alias = "Confirmado Clínico-Epidemiológico")]
    #[serde(alias = "Confirmação Clínico Epidemiológico")]
    #[serde(alias = "Confirmado Clínico-Imagem")]
    #[serde(alias = "Síndrome Gripal Não Especificada")]
    #[serde(alias = "Sindrome Gripal Nao Especificada")]
    Confirmado,
    #[serde(other)]
    Unexpected
}


pub fn tests(name: &str, filter: Option<Value>, indices: &str) -> Result<TestsData> {

    println!("Querying test data for {}...", name);

    let data : ESResult<ESAggr<DateAggr<ResultsAggr>>> = reqwest::blocking::Client::new()
	.post(&format!("https://elasticsearch-saps.saude.gov.br/desc-notificacoes-esusve-{}/_search", indices))
	.basic_auth("user-public-notificacoes", Some("Za4qNXdyQNSa9YaA"))
	.json(&json!({
	    "size": 0,
	    "query": { "bool": { "must": vec![
		filter,
		Some(json!({ "range": { "dataTeste": { "gte": "2020-03-01T00:00:00.00Z",
							"lt": &format!("{}", Utc::now().format("%Y-%m-%dT%H:%M:%S.00Z")) } } }))
	    ].into_iter().flatten().collect::<Vec<_>>() } },
	    "aggs": {
		"date": {
		    "date_histogram": {
			"field": "dataTeste",
			"calendar_interval": "day"
		    },
		    "aggs": {
			"resultados": {
			    "terms": { "field": "classificacaoFinal.keyword" }
			}
		    }		
		}
	    }
	})).send()?.json()?;

    
    match data {
	ESResult::Success(result) => {

	    let mut tests = BTreeMap::new();
	    
	    for date_bucket in result.sub.aggregations.date.buckets {
		let (pos,neg,all) = tests.entry(NaiveDateTime::from_timestamp(date_bucket.key / 1000, 0).date())
		    .or_insert((0.0,0.0,0.0));
		for result_bucket in date_bucket.sub.resultados.buckets {
		    match result_bucket.key {
			TestResult::Confirmado => {
			    *pos += result_bucket.doc_count as f64;
			    *all += result_bucket.doc_count as f64;
			},
			TestResult::Descartado => {
			    *neg += result_bucket.doc_count as f64;
			    *all += result_bucket.doc_count as f64;
			},
			TestResult::Unexpected => {
			    *all += result_bucket.doc_count as f64;
			}
		    }
		}
	    }

	    let date_range = NaiveDateRange(*tests.keys().min().ok_or(Error::MissingData)?,
					    Some(*tests.keys().max().ok_or(Error::MissingData)?));

	    Ok(date_range.map(
		|date| (date.clone(), tests.remove(&date).unwrap_or((0.0,0.0,0.0)))
	    ).collect())

	},
	ESResult::Unexpected(v) => Err(Error::ESQueryFailed(format!("unexpected result: {:?}", v)))
    }

}

    /*
    {
  "took" : 263,
  "timed_out" : false,
  "_shards" : {
    "total" : 28,
    "successful" : 28,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 10000,
      "relation" : "gte"
    },
    "max_score" : null,
    "hits" : [ ]
  },
  "aggregations" : {
    "date" : {
      "buckets" : [
        {
          "key_as_string" : "2020-03-01T00:00:00.000Z",
          "key" : 1583020800000,
          "doc_count" : 11,
          "resultados" : {
            "doc_count_error_upper_bound" : 0,
            "sum_other_doc_count" : 0,
            "buckets" : [
              {
                "key" : "Descartado",
                "doc_count" : 5
              },
              {
                "key" : "Confirmado Laboratorial",
                "doc_count" : 2
              }
            ]
          }
        },
        {
          "key_as_string" : "2020-03-02T00:00:00.000Z",
          "key" : 1583107200000,
          "doc_count" : 16,
          "resultados" : {
            "doc_count_error_upper_bound" : 0,
            "sum_other_doc_count" : 0,
            "buckets" : [
              {
                "key" : "Confirmado Laboratorial",
                "doc_count" : 7
              },
              {
                "key" : "Descartado",
                "doc_count" : 7
              }
            ]
          }
        },
        {
          "key_as_string" : "2020-03-03T00:00:00.000Z",
          "key" : 1583193600000,
          "doc_count" : 12,
          "resultados" : {
            "doc_count_error_upper_bound" : 0,
            "sum_other_doc_count" : 0,
            "buckets" : [
              {
                "key" : "Descartado",
                "doc_count" : 4
              },
              {
                "key" : "Confirmado Laboratorial",
                "doc_count" : 2
              }
            ]
          }
        },
        {
*/
