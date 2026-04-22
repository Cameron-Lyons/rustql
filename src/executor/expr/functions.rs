use super::date::{days_to_ymd, parse_date_components, ymd_to_days};
use super::*;

pub(super) fn evaluate_scalar_function(
    name: &ScalarFunctionType,
    args: &[Expression],
    columns: &[ColumnDefinition],
    row: &[Value],
    db: Option<&dyn DatabaseCatalog>,
) -> Result<Value, RustqlError> {
    let evaluated_args: Vec<Value> = args
        .iter()
        .map(|a| evaluate_value_expression_with_db(a, columns, row, db))
        .collect::<Result<Vec<_>, _>>()?;
    match name {
        ScalarFunctionType::Upper => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "UPPER requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Lower => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "LOWER requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Length => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Integer(s.len() as i64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "LENGTH requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Substring => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "SUBSTRING requires a text first argument".to_string(),
                    ));
                }
            };
            let start = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => (*i as usize).saturating_sub(1),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "SUBSTRING requires an integer start position".to_string(),
                    ));
                }
            };
            let chars: Vec<char> = s.chars().collect();
            if start >= chars.len() {
                return Ok(Value::Text(String::new()));
            }
            let len = match evaluated_args.get(2) {
                Some(Value::Integer(l)) => *l as usize,
                None => chars.len() - start,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "SUBSTRING length must be an integer".to_string(),
                    ));
                }
            };
            let result: String = chars.iter().skip(start).take(len).collect();
            Ok(Value::Text(result))
        }
        ScalarFunctionType::Abs => match evaluated_args.first() {
            Some(Value::Integer(i)) => Ok(Value::Integer(i.abs())),
            Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "ABS requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Round => {
            let val = match evaluated_args.first() {
                Some(Value::Float(f)) => *f,
                Some(Value::Integer(i)) => *i as f64,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "ROUND requires a numeric argument".to_string(),
                    ));
                }
            };
            let decimals = match evaluated_args.get(1) {
                Some(Value::Integer(d)) => *d as i32,
                None => 0,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "ROUND decimals must be an integer".to_string(),
                    ));
                }
            };
            let factor = 10f64.powi(decimals);
            Ok(Value::Float((val * factor).round() / factor))
        }
        ScalarFunctionType::Coalesce => {
            for arg in &evaluated_args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        ScalarFunctionType::Trim => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim().to_string())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "TRIM requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Replace => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "REPLACE requires a text first argument".to_string(),
                    ));
                }
            };
            let from = match evaluated_args.get(1) {
                Some(Value::Text(s)) => s.clone(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "REPLACE requires a text second argument".to_string(),
                    ));
                }
            };
            let to = match evaluated_args.get(2) {
                Some(Value::Text(s)) => s.clone(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "REPLACE requires a text third argument".to_string(),
                    ));
                }
            };
            Ok(Value::Text(s.replace(&from, &to)))
        }
        ScalarFunctionType::ConcatFn => {
            let mut result = String::new();
            for arg in &evaluated_args {
                match arg {
                    Value::Null => {}
                    other => result.push_str(&format_value(other)),
                }
            }
            Ok(Value::Text(result))
        }
        ScalarFunctionType::Position => {
            let needle = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "POSITION requires a text first argument".to_string(),
                    ));
                }
            };
            let haystack = match evaluated_args.get(1) {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "POSITION requires a text second argument".to_string(),
                    ));
                }
            };
            match haystack.find(&needle) {
                Some(pos) => Ok(Value::Integer(pos as i64 + 1)),
                None => Ok(Value::Integer(0)),
            }
        }
        ScalarFunctionType::Instr => {
            let haystack = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "INSTR requires a text first argument".to_string(),
                    ));
                }
            };
            let needle = match evaluated_args.get(1) {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "INSTR requires a text second argument".to_string(),
                    ));
                }
            };
            match haystack.find(&needle) {
                Some(pos) => Ok(Value::Integer(pos as i64 + 1)),
                None => Ok(Value::Integer(0)),
            }
        }
        ScalarFunctionType::Ceil => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Integer(f.ceil() as i64)),
            Some(Value::Integer(i)) => Ok(Value::Integer(*i)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "CEIL requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Floor => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Integer(f.floor() as i64)),
            Some(Value::Integer(i)) => Ok(Value::Integer(*i)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "FLOOR requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Sqrt => match evaluated_args.first() {
            Some(Value::Float(f)) => {
                if *f < 0.0 {
                    Err(RustqlError::TypeMismatch(
                        "SQRT of a negative number".to_string(),
                    ))
                } else {
                    Ok(Value::Float(f.sqrt()))
                }
            }
            Some(Value::Integer(i)) => {
                let f = *i as f64;
                if f < 0.0 {
                    Err(RustqlError::TypeMismatch(
                        "SQRT of a negative number".to_string(),
                    ))
                } else {
                    Ok(Value::Float(f.sqrt()))
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "SQRT requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Power => {
            let base = match evaluated_args.first() {
                Some(Value::Float(f)) => *f,
                Some(Value::Integer(i)) => *i as f64,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "POWER requires a numeric first argument".to_string(),
                    ));
                }
            };
            let exp = match evaluated_args.get(1) {
                Some(Value::Float(f)) => *f,
                Some(Value::Integer(i)) => *i as f64,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "POWER requires a numeric second argument".to_string(),
                    ));
                }
            };
            Ok(Value::Float(base.powf(exp)))
        }
        ScalarFunctionType::Mod => {
            let left = match evaluated_args.first() {
                Some(Value::Integer(i)) => *i,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "MOD requires integer arguments".to_string(),
                    ));
                }
            };
            let right = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "MOD requires integer arguments".to_string(),
                    ));
                }
            };
            if right == 0 {
                Err(RustqlError::DivisionByZero)
            } else {
                Ok(Value::Integer(left % right))
            }
        }
        ScalarFunctionType::Now => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let days = secs / 86400;
            let remaining = secs % 86400;
            let hours = remaining / 3600;
            let minutes = (remaining % 3600) / 60;
            let seconds = remaining % 60;

            let (year, month, day) = days_to_ymd(days as i64);
            Ok(Value::DateTime(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                year, month, day, hours, minutes, seconds
            )))
        }
        ScalarFunctionType::Year => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((y, _, _)) => Ok(Value::Integer(y)),
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract YEAR from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "YEAR requires a date/datetime argument".to_string(),
            )),
        },
        ScalarFunctionType::Month => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((_, m, _)) => Ok(Value::Integer(m)),
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract MONTH from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "MONTH requires a date/datetime argument".to_string(),
            )),
        },
        ScalarFunctionType::Day => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((_, _, d)) => Ok(Value::Integer(d)),
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract DAY from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "DAY requires a date/datetime argument".to_string(),
            )),
        },
        ScalarFunctionType::DateAdd => {
            let date_str = match evaluated_args.first() {
                Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => d.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATE_ADD requires a date first argument".to_string(),
                    ));
                }
            };
            let days_to_add = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATE_ADD requires an integer second argument".to_string(),
                    ));
                }
            };
            let (y, m, d) = parse_date_components(&date_str)
                .ok_or_else(|| RustqlError::TypeMismatch("Invalid date format".to_string()))?;
            let jdn = ymd_to_days(y, m, d);
            let new_jdn = jdn + days_to_add;
            let (ny, nm, nd) = days_to_ymd(new_jdn);
            Ok(Value::Date(format!("{:04}-{:02}-{:02}", ny, nm, nd)))
        }
        ScalarFunctionType::Datediff => {
            let date1_str = match evaluated_args.first() {
                Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => d.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATEDIFF requires a date first argument".to_string(),
                    ));
                }
            };
            let date2_str = match evaluated_args.get(1) {
                Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => d.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATEDIFF requires a date second argument".to_string(),
                    ));
                }
            };
            let (y1, m1, d1) = parse_date_components(&date1_str).ok_or_else(|| {
                RustqlError::TypeMismatch("Invalid first date format".to_string())
            })?;
            let (y2, m2, d2) = parse_date_components(&date2_str).ok_or_else(|| {
                RustqlError::TypeMismatch("Invalid second date format".to_string())
            })?;
            let jdn1 = ymd_to_days(y1, m1, d1);
            let jdn2 = ymd_to_days(y2, m2, d2);
            Ok(Value::Integer(jdn1 - jdn2))
        }
        ScalarFunctionType::Nullif => {
            if evaluated_args.len() < 2 {
                return Err(RustqlError::TypeMismatch(
                    "NULLIF requires two arguments".to_string(),
                ));
            }
            if evaluated_args[0] == evaluated_args[1] {
                Ok(Value::Null)
            } else {
                Ok(evaluated_args[0].clone())
            }
        }
        ScalarFunctionType::Greatest => {
            let non_null: Vec<&Value> = evaluated_args
                .iter()
                .filter(|v| !matches!(v, Value::Null))
                .collect();
            if non_null.is_empty() {
                Ok(Value::Null)
            } else {
                let mut max = non_null[0];
                for v in &non_null[1..] {
                    if (*v).cmp(max) == std::cmp::Ordering::Greater {
                        max = *v;
                    }
                }
                Ok(max.clone())
            }
        }
        ScalarFunctionType::Least => {
            let non_null: Vec<&Value> = evaluated_args
                .iter()
                .filter(|v| !matches!(v, Value::Null))
                .collect();
            if non_null.is_empty() {
                Ok(Value::Null)
            } else {
                let mut min = non_null[0];
                for v in &non_null[1..] {
                    if (*v).cmp(min) == std::cmp::Ordering::Less {
                        min = *v;
                    }
                }
                Ok(min.clone())
            }
        }
        ScalarFunctionType::Lpad => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "LPAD requires a text first argument".to_string(),
                    ));
                }
            };
            let len = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i as usize,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "LPAD requires an integer second argument".to_string(),
                    ));
                }
            };
            let pad = match evaluated_args.get(2) {
                Some(Value::Text(p)) => p.clone(),
                None => " ".to_string(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "LPAD pad argument must be text".to_string(),
                    ));
                }
            };
            let chars: Vec<char> = s.chars().collect();
            if chars.len() >= len {
                Ok(Value::Text(chars[..len].iter().collect()))
            } else {
                let needed = len - chars.len();
                let pad_chars: Vec<char> = pad.chars().collect();
                let mut result = String::new();
                for i in 0..needed {
                    result.push(pad_chars[i % pad_chars.len()]);
                }
                result.extend(chars);
                Ok(Value::Text(result))
            }
        }
        ScalarFunctionType::Rpad => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "RPAD requires a text first argument".to_string(),
                    ));
                }
            };
            let len = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i as usize,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "RPAD requires an integer second argument".to_string(),
                    ));
                }
            };
            let pad = match evaluated_args.get(2) {
                Some(Value::Text(p)) => p.clone(),
                None => " ".to_string(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "RPAD pad argument must be text".to_string(),
                    ));
                }
            };
            let chars: Vec<char> = s.chars().collect();
            if chars.len() >= len {
                Ok(Value::Text(chars[..len].iter().collect()))
            } else {
                let needed = len - chars.len();
                let pad_chars: Vec<char> = pad.chars().collect();
                let mut result: String = chars.into_iter().collect();
                for i in 0..needed {
                    result.push(pad_chars[i % pad_chars.len()]);
                }
                Ok(Value::Text(result))
            }
        }
        ScalarFunctionType::LeftFn => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "LEFT requires a text first argument".to_string(),
                    ));
                }
            };
            let n = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i as usize,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "LEFT requires an integer second argument".to_string(),
                    ));
                }
            };
            let result: String = s.chars().take(n).collect();
            Ok(Value::Text(result))
        }
        ScalarFunctionType::RightFn => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "RIGHT requires a text first argument".to_string(),
                    ));
                }
            };
            let n = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i as usize,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "RIGHT requires an integer second argument".to_string(),
                    ));
                }
            };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            let result: String = chars[start..].iter().collect();
            Ok(Value::Text(result))
        }
        ScalarFunctionType::Reverse => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.chars().rev().collect())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "REVERSE requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Repeat => {
            let s = match evaluated_args.first() {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "REPEAT requires a text first argument".to_string(),
                    ));
                }
            };
            let n = match evaluated_args.get(1) {
                Some(Value::Integer(i)) => *i as usize,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "REPEAT requires an integer second argument".to_string(),
                    ));
                }
            };
            Ok(Value::Text(s.repeat(n)))
        }
        ScalarFunctionType::Log => {
            if evaluated_args.len() == 1 {
                match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.ln())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).ln())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "LOG requires a numeric argument".to_string(),
                    )),
                }
            } else {
                let base = match evaluated_args.first() {
                    Some(Value::Float(f)) => *f,
                    Some(Value::Integer(i)) => *i as f64,
                    Some(Value::Null) => return Ok(Value::Null),
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "LOG requires numeric arguments".to_string(),
                        ));
                    }
                };
                let x = match evaluated_args.get(1) {
                    Some(Value::Float(f)) => *f,
                    Some(Value::Integer(i)) => *i as f64,
                    Some(Value::Null) => return Ok(Value::Null),
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "LOG requires numeric arguments".to_string(),
                        ));
                    }
                };
                Ok(Value::Float(x.ln() / base.ln()))
            }
        }
        ScalarFunctionType::Exp => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.exp())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).exp())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "EXP requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Sign => match evaluated_args.first() {
            Some(Value::Integer(i)) => {
                if *i > 0 {
                    Ok(Value::Integer(1))
                } else if *i < 0 {
                    Ok(Value::Integer(-1))
                } else {
                    Ok(Value::Integer(0))
                }
            }
            Some(Value::Float(f)) => {
                if *f > 0.0 {
                    Ok(Value::Integer(1))
                } else if *f < 0.0 {
                    Ok(Value::Integer(-1))
                } else {
                    Ok(Value::Integer(0))
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "SIGN requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::DateTrunc => {
            let part = match evaluated_args.first() {
                Some(Value::Text(s)) => s.to_lowercase(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATE_TRUNC requires a text part argument".to_string(),
                    ));
                }
            };
            let date_str = match evaluated_args.get(1) {
                Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => d.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "DATE_TRUNC requires a date second argument".to_string(),
                    ));
                }
            };
            let (y, m, d) = parse_date_components(&date_str)
                .ok_or_else(|| RustqlError::TypeMismatch("Invalid date format".to_string()))?;
            match part.as_str() {
                "year" => Ok(Value::Date(format!("{:04}-01-01", y))),
                "month" => Ok(Value::Date(format!("{:04}-{:02}-01", y, m))),
                "day" => Ok(Value::Date(format!("{:04}-{:02}-{:02}", y, m, d))),
                _ => Err(RustqlError::TypeMismatch(format!(
                    "DATE_TRUNC unsupported part: {}",
                    part
                ))),
            }
        }
        ScalarFunctionType::Extract => {
            let part = match evaluated_args.first() {
                Some(Value::Text(s)) => s.to_lowercase(),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "EXTRACT requires a text part argument".to_string(),
                    ));
                }
            };
            let date_str = match evaluated_args.get(1) {
                Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => d.clone(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "EXTRACT requires a date/datetime second argument".to_string(),
                    ));
                }
            };
            match part.as_str() {
                "year" => {
                    let (y, _, _) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    Ok(Value::Integer(y))
                }
                "month" => {
                    let (_, m, _) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    Ok(Value::Integer(m))
                }
                "day" => {
                    let (_, _, d) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    Ok(Value::Integer(d))
                }
                "hour" | "minute" | "second" => {
                    let time_part = if let Some(space_pos) = date_str.find(' ') {
                        &date_str[space_pos + 1..]
                    } else {
                        &date_str
                    };
                    let parts: Vec<&str> = time_part.split(':').collect();
                    match part.as_str() {
                        "hour" => {
                            let h = parts
                                .first()
                                .and_then(|s| s.parse::<i64>().ok())
                                .unwrap_or(0);
                            Ok(Value::Integer(h))
                        }
                        "minute" => {
                            let m = parts
                                .get(1)
                                .and_then(|s| s.parse::<i64>().ok())
                                .unwrap_or(0);
                            Ok(Value::Integer(m))
                        }
                        "second" => {
                            let s = parts
                                .get(2)
                                .and_then(|s| s.parse::<i64>().ok())
                                .unwrap_or(0);
                            Ok(Value::Integer(s))
                        }
                        _ => Err(RustqlError::TypeMismatch(format!(
                            "Unsupported EXTRACT part '{}'",
                            part
                        ))),
                    }
                }
                "quarter" => {
                    let (_, m, _) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    Ok(Value::Integer((m - 1) / 3 + 1))
                }
                "week" => {
                    let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    let day_of_year = ymd_to_days(y, m, d) - ymd_to_days(y, 1, 1);
                    Ok(Value::Integer(day_of_year / 7 + 1))
                }
                "dow" | "dayofweek" => {
                    let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    let days = ymd_to_days(y, m, d);
                    Ok(Value::Integer(((days % 7 + 7 + 4) % 7) + 1))
                }
                _ => Err(RustqlError::TypeMismatch(format!(
                    "EXTRACT unsupported part: {}",
                    part
                ))),
            }
        }
        ScalarFunctionType::Ltrim => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_start().to_string())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "LTRIM requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Rtrim => match evaluated_args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_string())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "RTRIM requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Ascii => match evaluated_args.first() {
            Some(Value::Text(s)) => match s.chars().next() {
                Some(ch) => Ok(Value::Integer(ch as i64)),
                None => Ok(Value::Null),
            },
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "ASCII requires a text argument".to_string(),
            )),
        },
        ScalarFunctionType::Chr => match evaluated_args.first() {
            Some(Value::Integer(i)) => match char::from_u32(*i as u32) {
                Some(c) => Ok(Value::Text(c.to_string())),
                None => Ok(Value::Null),
            },
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "CHR requires an integer argument".to_string(),
            )),
        },
        ScalarFunctionType::Sin => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.sin())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).sin())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "SIN requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Cos => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.cos())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).cos())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "COS requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Tan => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.tan())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).tan())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "TAN requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Asin => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.asin())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).asin())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "ASIN requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Acos => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.acos())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).acos())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "ACOS requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Atan => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.atan())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).atan())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "ATAN requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Atan2 => {
            let y = match evaluated_args.first() {
                Some(Value::Float(f)) => *f,
                Some(Value::Integer(i)) => *i as f64,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "ATAN2 requires numeric arguments".to_string(),
                    ));
                }
            };
            let x = match evaluated_args.get(1) {
                Some(Value::Float(f)) => *f,
                Some(Value::Integer(i)) => *i as f64,
                Some(Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "ATAN2 requires numeric arguments".to_string(),
                    ));
                }
            };
            Ok(Value::Float(y.atan2(x)))
        }
        ScalarFunctionType::Random => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let nanos = now.subsec_nanos() as u64;
            let secs = now.as_secs();
            let seed = secs.wrapping_mul(6364136223846793005).wrapping_add(nanos);
            let val = (seed as f64) / (u64::MAX as f64);
            Ok(Value::Float(val.abs()))
        }
        ScalarFunctionType::Degrees => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.to_degrees())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).to_degrees())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "DEGREES requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Radians => match evaluated_args.first() {
            Some(Value::Float(f)) => Ok(Value::Float(f.to_radians())),
            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).to_radians())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "RADIANS requires a numeric argument".to_string(),
            )),
        },
        ScalarFunctionType::Quarter => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((_, m, _)) => Ok(Value::Integer((m - 1) / 3 + 1)),
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract QUARTER from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "QUARTER requires a date argument".to_string(),
            )),
        },
        ScalarFunctionType::Week => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((y, m, d_val)) => {
                        let day_of_year = ymd_to_days(y, m, d_val) - ymd_to_days(y, 1, 1);
                        Ok(Value::Integer(day_of_year / 7 + 1))
                    }
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract WEEK from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "WEEK requires a date argument".to_string(),
            )),
        },
        ScalarFunctionType::DayOfWeek => match evaluated_args.first() {
            Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                match parse_date_components(d) {
                    Some((y, m, d_val)) => {
                        let days = ymd_to_days(y, m, d_val);
                        Ok(Value::Integer(((days % 7 + 7 + 4) % 7) + 1))
                    }
                    None => Err(RustqlError::TypeMismatch(
                        "Cannot extract DAYOFWEEK from value".to_string(),
                    )),
                }
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "DAYOFWEEK requires a date argument".to_string(),
            )),
        },
        ScalarFunctionType::Pi => Ok(Value::Float(std::f64::consts::PI)),
        ScalarFunctionType::Trunc => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let precision = if args.len() > 1 {
                match evaluate_value_expression_with_db(&args[1], columns, row, db)? {
                    Value::Integer(n) => n as i32,
                    Value::Float(f) => f as i32,
                    _ => 0,
                }
            } else {
                0
            };
            match val {
                Value::Float(f) => {
                    let factor = 10f64.powi(precision);
                    Ok(Value::Float((f * factor).trunc() / factor))
                }
                Value::Integer(n) => {
                    if precision >= 0 {
                        Ok(Value::Integer(n))
                    } else {
                        let factor = 10i64.pow((-precision) as u32);
                        Ok(Value::Integer((n / factor) * factor))
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Log10 => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            match val {
                Value::Float(f) => Ok(Value::Float(f.log10())),
                Value::Integer(n) => Ok(Value::Float((n as f64).log10())),
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Log2 => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            match val {
                Value::Float(f) => Ok(Value::Float(f.log2())),
                Value::Integer(n) => Ok(Value::Float((n as f64).log2())),
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Cbrt => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            match val {
                Value::Float(f) => Ok(Value::Float(f.cbrt())),
                Value::Integer(n) => Ok(Value::Float((n as f64).cbrt())),
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Gcd => {
            let a = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let b = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            match (a, b) {
                (Value::Integer(mut a), Value::Integer(mut b)) => {
                    a = a.abs();
                    b = b.abs();
                    while b != 0 {
                        let t = b;
                        b = a % b;
                        a = t;
                    }
                    Ok(Value::Integer(a))
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Lcm => {
            let a = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let b = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            match (a, b) {
                (Value::Integer(a), Value::Integer(b)) => {
                    if a == 0 && b == 0 {
                        Ok(Value::Integer(0))
                    } else {
                        let mut ga = a.abs();
                        let mut gb = b.abs();
                        let prod = ga * gb;
                        while gb != 0 {
                            let t = gb;
                            gb = ga % gb;
                            ga = t;
                        }
                        Ok(Value::Integer(prod / ga))
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Initcap => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            match val {
                Value::Text(s) => {
                    let mut result = String::new();
                    let mut capitalize_next = true;
                    for ch in s.chars() {
                        if ch.is_whitespace() || !ch.is_alphanumeric() {
                            capitalize_next = true;
                            result.push(ch);
                        } else if capitalize_next {
                            result.extend(ch.to_uppercase());
                            capitalize_next = false;
                        } else {
                            result.extend(ch.to_lowercase());
                        }
                    }
                    Ok(Value::Text(result))
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::SplitPart => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let delim = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            let part = evaluate_value_expression_with_db(&args[2], columns, row, db)?;
            match (val, delim, part) {
                (Value::Text(s), Value::Text(d), Value::Integer(n)) => {
                    let parts: Vec<&str> = s.split(&d).collect();
                    let idx = (n - 1) as usize;
                    if idx < parts.len() {
                        Ok(Value::Text(parts[idx].to_string()))
                    } else {
                        Ok(Value::Text(String::new()))
                    }
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::Translate => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let from = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            let to = evaluate_value_expression_with_db(&args[2], columns, row, db)?;
            match (val, from, to) {
                (Value::Text(s), Value::Text(from_chars), Value::Text(to_chars)) => {
                    let from_v: Vec<char> = from_chars.chars().collect();
                    let to_v: Vec<char> = to_chars.chars().collect();
                    let result: String = s
                        .chars()
                        .filter_map(|ch| {
                            if let Some(pos) = from_v.iter().position(|&fc| fc == ch) {
                                if pos < to_v.len() {
                                    Some(to_v[pos])
                                } else {
                                    None
                                }
                            } else {
                                Some(ch)
                            }
                        })
                        .collect();
                    Ok(Value::Text(result))
                }
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::RegexpMatch => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let pattern = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            match (val, pattern) {
                (Value::Text(s), Value::Text(p)) => match regex::Regex::new(&p) {
                    Ok(re) => {
                        if let Some(m) = re.find(&s) {
                            Ok(Value::Text(m.as_str().to_string()))
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    Err(_) => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }
        ScalarFunctionType::RegexpReplace => {
            let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
            let pattern = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
            let replacement = evaluate_value_expression_with_db(&args[2], columns, row, db)?;
            match (val, pattern, replacement) {
                (Value::Text(s), Value::Text(p), Value::Text(r)) => match regex::Regex::new(&p) {
                    Ok(re) => Ok(Value::Text(re.replace_all(&s, r.as_str()).to_string())),
                    Err(_) => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }
    }
}
