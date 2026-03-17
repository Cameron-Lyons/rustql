use rustql::{Engine, storage};
use std::env;
use std::io::{self, IsTerminal, Read, Write};
use std::path::PathBuf;

fn main() {
    let engine = match build_engine_from_args() {
        Ok(Some(engine)) => engine,
        Ok(None) => return,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(2);
        }
    };

    if std::io::stdin().is_terminal() {
        println!("RustQL - SQL Engine in Rust");
        println!("Type 'exit' to quit\n");

        loop {
            print!("rustql> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();

            let query = input.trim();

            if query.to_lowercase() == "exit" {
                println!("Goodbye!");
                break;
            }

            if query.is_empty() {
                continue;
            }

            match engine.process_query(query) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    } else {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input).unwrap();
        let query = input.trim();

        if !query.is_empty() {
            match engine.process_query(query) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

fn build_engine_from_args() -> Result<Option<Engine>, String> {
    let mut args = env::args().skip(1);
    let mut storage_mode: Option<String> = None;
    let mut json_path: Option<PathBuf> = None;
    let mut btree_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                return Ok(None);
            }
            "--storage" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--storage requires a value".to_string())?;
                storage_mode = Some(value);
            }
            "--json-path" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--json-path requires a value".to_string())?;
                json_path = Some(PathBuf::from(value));
            }
            "--btree-path" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--btree-path requires a value".to_string())?;
                btree_path = Some(PathBuf::from(value));
            }
            _ => return Err(format!("unknown argument: {}", arg)),
        }
    }

    match storage_mode.as_deref() {
        None => match (json_path, btree_path) {
            (Some(path), None) => Ok(Some(Engine::with_json_path(path))),
            (None, Some(path)) => Ok(Some(Engine::with_btree_path(path))),
            (None, None) => Ok(Some(Engine::with_default_storage())),
            (Some(_), Some(_)) => {
                Err("choose either --json-path or --btree-path, not both".to_string())
            }
        },
        Some("memory") => {
            if json_path.is_some() || btree_path.is_some() {
                return Err("--storage memory does not accept path flags".to_string());
            }
            Ok(Some(Engine::new()))
        }
        Some("json") => {
            if btree_path.is_some() {
                return Err("--storage json does not accept --btree-path".to_string());
            }
            let path = json_path.unwrap_or_else(|| storage::default_storage_paths().json_path);
            Ok(Some(Engine::with_json_path(path)))
        }
        Some("btree") => {
            if json_path.is_some() {
                return Err("--storage btree does not accept --json-path".to_string());
            }
            let path = btree_path.unwrap_or_else(|| storage::default_storage_paths().btree_path);
            Ok(Some(Engine::with_btree_path(path)))
        }
        Some(other) => Err(format!(
            "unsupported storage backend '{}'; expected memory, json, or btree",
            other
        )),
    }
}

fn print_usage() {
    println!("Usage: rustql [--storage memory|json|btree] [--json-path PATH] [--btree-path PATH]");
    println!();
    println!("Without flags, RustQL uses the default storage backend from RUSTQL_STORAGE.");
    println!("RUSTQL_JSON_PATH and RUSTQL_BTREE_PATH override the default file locations.");
}
