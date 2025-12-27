#![allow(unused)]

use strum_macros::Display;

#[derive(Debug, Display)]
pub enum ToSqlError {
    InvalidOperandValue(String),
    InvalidRegexValue(String),
    UnsupportedOperator(String),
    MissingOperator(String),
    InvalidStage(String),
}

#[derive(Debug, Default, PartialEq)]
pub struct SortField {
    pub name: String,
    pub descending: bool,
}

#[derive(Debug, Default, PartialEq)]
pub struct QueryParams {
    pub filter: Option<String>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    pub fields: Option<Vec<String>>,
    pub sort: Option<Vec<SortField>>,
}

pub fn parse(stage: &serde_json::Value) -> Result<QueryParams, ToSqlError> {
    let mut query_params = QueryParams::default();

    // filter (where clause)
    let filter = parse_filter(stage)?;
    query_params.filter = filter;

    if let Some(stage_obj) = stage.as_object() {
        for (key, value) in stage_obj.iter() {
            // limit and offset
            if ["$limit", "$offset"].contains(&key.as_str()) {
                let v =
                    value
                        .as_u64()
                        .ok_or(ToSqlError::InvalidOperandValue(format!(
                            "Invalid operand: {}, value {}",
                            key.to_string(),
                            value
                        )))?;
                if key.as_str() == "$limit" {
                    query_params.limit = Some(v);
                } else if key.as_str() == "$offset" {
                    query_params.offset = Some(v);
                }
            }

            // fields for project (select clause)
            if key.as_str() == "$fields" {
                let fields = match value {
                    serde_json::Value::Array(a) => a
                        .iter()
                        .map(|v| format!("{}", v.as_str().unwrap()))
                        .collect::<Vec<_>>(),
                    _ => vec![format!("{}", value)],
                };
                query_params.fields = Some(fields);
            }

            // sort (order by clause)
            if key.as_str() == "$sort" {
                if let Some(sort_object) = value.as_object() {
                    let mut sort_fields = vec![];
                    for (field, order) in sort_object.iter() {
                        // Default ASC = 1
                        let order = order.as_i64().unwrap_or(1);
                        let descending = if order == 1 { false } else { true };
                        sort_fields.push(SortField {
                            name: field.clone(),
                            descending,
                        });
                    }
                    if sort_fields.len() > 0 {
                        query_params.sort = Some(sort_fields);
                    }
                }
            }
        }
    }

    Ok(query_params)
}

fn parse_filter(stage: &serde_json::Value) -> Result<Option<String>, ToSqlError> {
    let mut filter = String::new();
    if let Some(stage_obj) = stage.as_object() {
        let op_keys = ["$and", "$or", "$nor"];
        let exclude_keys = ["$fields", "$sort", "$limit", "$offset"];
        let mut op_values: Vec<&serde_json::Value> = Vec::new();
        for (key, value) in stage_obj.iter() {
            if exclude_keys.contains(&key.as_str()) {
                continue;
            }
            if op_keys.contains(&key.as_str()) {
                if let serde_json::Value::Array(a) = value {
                    op_values = a.iter().collect();
                } else {
                    return Err(ToSqlError::InvalidOperandValue(format!(
                        "Invalid operand: {}",
                        key.to_string()
                    )));
                }
            } else if let serde_json::Value::Object(op) = value {
                if let Some(op_key) = op.keys().next() {
                    let op_value = op.get(op_key).unwrap();
                    match op_key.as_str() {
                        "$gte" => filter.push_str(&format!("{} >= {}", key, op_value)),
                        "$gt" => filter.push_str(&format!("{} > {}", key, op_value)),
                        "$lte" => filter.push_str(&format!("{} <= {}", key, op_value)),
                        "$lt" => filter.push_str(&format!("{} < {}", key, op_value)),
                        "$eq" => {
                            if op_value.is_string() {
                                filter.push_str(&format!(
                                    "{} = '{}'",
                                    key,
                                    parse_str(op_value)?
                                ))
                            } else {
                                filter.push_str(&format!("{} = {}", key, op_value))
                            }
                        }
                        "$ne" => {
                            if op_value.is_string() {
                                filter.push_str(&format!(
                                    "{} != '{}'",
                                    key,
                                    parse_str(op_value)?
                                ))
                            } else {
                                filter.push_str(&format!("{} != {}", key, op_value))
                            }
                        }
                        "$in" => {
                            let vals = match op_value {
                                serde_json::Value::Array(a) => a
                                    .iter()
                                    .map(|v| {
                                        if v.is_string() {
                                            format!("'{}'", parse_str(v).unwrap())
                                        } else {
                                            format!("{}", v)
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                                _ => {
                                    if op_value.is_string() {
                                        vec![format!("'{}'", parse_str(op_value)?)]
                                    } else {
                                        vec![format!("{}", op_value)]
                                    }
                                }
                            };
                            filter.push_str(&format!("{} IN ({})", key, vals.join(", ")));
                        }
                        "$nin" => {
                            let vals = match op_value {
                                serde_json::Value::Array(a) => a
                                    .iter()
                                    .map(|v| {
                                        if v.is_string() {
                                            format!("'{}'", parse_str(v).unwrap())
                                        } else {
                                            format!("{}", v)
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                                _ => {
                                    if op_value.is_string() {
                                        vec![format!("'{}'", parse_str(op_value)?)]
                                    } else {
                                        vec![format!("{}", op_value)]
                                    }
                                }
                            };
                            filter.push_str(&format!(
                                "{} NOT IN ({})",
                                key,
                                vals.join(", ")
                            ));
                        }
                        "$like" => {
                            if op_value.is_string() {
                                filter.push_str(&format!(
                                    "{} LIKE '{}'",
                                    key,
                                    parse_str(op_value)?
                                ))
                            } else {
                                return Err(ToSqlError::InvalidOperandValue(format!(
                                    "$like operator only support string operand, but got: {}",
                                    op_value
                                )));
                            }
                        }
                        "$nlike" => {
                            if op_value.is_string() {
                                filter.push_str(&format!(
                                    "{} NOT LIKE '{}'",
                                    key,
                                    parse_str(op_value)?
                                ))
                            } else {
                                return Err(ToSqlError::InvalidOperandValue(format!(
                                    "$nlike operator only support string operand, but got: {}",
                                    op_value
                                )));
                            }
                        }
                        "$regex" => filter.push_str(&format!(
                            "regexp_like({}, '{}')",
                            key,
                            op_value.as_str().ok_or_else(|| {
                                ToSqlError::InvalidRegexValue(format!(
                                    "Invalid regexp: {}",
                                    op_value.clone()
                                ))
                            })?
                        )),
                        "$options" => {}
                        _ => {
                            return Err(ToSqlError::UnsupportedOperator(format!(
                                "Unsupported operator: {}",
                                op_key.to_string()
                            )))
                        }
                    }
                } else {
                    return Err(ToSqlError::MissingOperator(format!(
                        "Missing operator: {}",
                        key.to_string()
                    )));
                }
            } else {
                if value.is_string() {
                    filter.push_str(&format!("{} = '{}'", key, parse_str(value)?));
                } else {
                    filter.push_str(&format!("{} = {}", key, value));
                }
            }
        }

        if !op_values.is_empty() {
            let sub_sql = op_values
                .into_iter()
                .map(|sub_stage| parse_filter(sub_stage))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|s| format!("({})", s.unwrap()))
                .collect::<Vec<_>>();
            let sub_sql = sub_sql.join(if stage_obj.contains_key("$and") {
                " AND "
            } else {
                " OR "
            });
            filter.push_str(&format!("({})", sub_sql));
        }
    } else {
        return Err(ToSqlError::InvalidStage(format!(
            "Invalid stage: {}",
            stage.to_owned()
        )));
    }
    Ok(Some(filter))
}

fn parse_str(v: &serde_json::Value) -> Result<&str, ToSqlError> {
    v.as_str().ok_or(ToSqlError::InvalidOperandValue(format!(
        "Invalid string operand: {}",
        v
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_gte() {
        let stage = json!({
           "age": { "$gte": 21 },
           "$offset": 5,
           "$limit": 10,
           "$sort": { "id": 1, "device_id": -1 },
           "$fields": ["id", "device_id", "lon", "lat", "ts"]
        });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "age >= 21");
    }

    #[test]
    fn test_parse_gt() {
        let stage = json!({ "age": { "$gt": 21 } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "age > 21");
    }

    #[test]
    fn test_parse_eq_number() {
        let stage = json!({ "age": { "$eq": 21 } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "age = 21");
    }

    #[test]
    fn test_parse_lte() {
        let stage = json!({ "age": { "$lte": 21 } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "age <= 21");
    }

    #[test]
    fn test_parse_lt() {
        let stage = json!({ "age": { "$lt": 21 } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "age < 21");
    }

    #[test]
    fn test_parse_eq_str() {
        let stage = json!({ "name": { "$eq": "John" } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "name = 'John'");
    }

    #[test]
    fn test_parse_ne() {
        let stage = json!({ "name": { "$ne": "John" } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "name != 'John'");
    }

    #[test]
    fn test_parse_in() {
        let stage = json!({ "status": { "$in": ["active", "pending"] } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "status IN ('active', 'pending')");
    }

    #[test]
    fn test_parse_nin() {
        let stage = json!({ "status": { "$nin": ["active", "pending"] } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "status NOT IN ('active', 'pending')");
    }

    #[test]
    fn test_parse_and() {
        let stage = json!({
            "$and": [
                { "status": "active" },
                { "age": { "$gte": 21 } }
            ]
        });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "((status = 'active') AND (age >= 21))");
    }

    #[test]
    fn test_parse_or() {
        let stage = json!({
            "$or": [
                { "status": "active" },
                { "age": { "$gte": 21 } }
            ]
        });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "((status = 'active') OR (age >= 21))");
    }

    #[test]
    fn test_parse_or_and() {
        let stage = json!({
            "$or": [
                { "$and": [
                    { "status": { "$ne": "active" } },
                    { "code": 4345654 }
                ] },
                { "age": { "$gte": 21 } }
            ]
        });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(
            filter,
            "((((status != 'active') AND (code = 4345654))) OR (age >= 21))"
        );
    }

    #[test]
    fn test_parse_like() {
        let stage = json!({ "name": { "$like": "%ohn" } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "name LIKE '%ohn'");
    }

    #[test]
    fn test_parse_not_like() {
        let stage = json!({ "name": { "$nlike": "%ohn" } });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "name NOT LIKE '%ohn'");
    }

    #[test]
    fn test_parse_regex() {
        let stage = json!({
            "name": {
                "$regex": "^joh?n$"
            }
        });
        let filter = parse_filter(&stage).unwrap().unwrap();
        assert_eq!(filter, "regexp_like(name, '^joh?n$')");
    }

    #[test]
    fn test_parse_unsupported_operator() {
        let stage = json!({ "name": { "$foo": "bar" } });
        let res = parse_filter(&stage);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_query_params() {
        let stage = json!({
            "$or": [
                { "$and": [
                    { "status": { "$ne": "active" } },
                    { "code": 4345654 }
                ] },
                { "age": { "$gte": 21 } }
            ],
           "$offset": 5,
           "$limit": 10,
           "$sort": { "id": 1 },
           "$fields": ["id", "device_id", "lon", "lat", "ts"]
        });
        let qp = parse(&stage).unwrap();

        let expected_qp = QueryParams {
            filter: Some(String::from(
                "((((status != 'active') AND (code = 4345654))) OR (age >= 21))",
            )),
            sort: Some(vec![SortField {
                name: "id".to_string(),
                descending: false,
            }]),
            fields: Some(vec![
                "id".to_string(),
                "device_id".to_string(),
                "lon".to_string(),
                "lat".to_string(),
                "ts".to_string(),
            ]),
            offset: Some(5),
            limit: Some(10),
        };
        assert_eq!(expected_qp, qp);
    }
}
