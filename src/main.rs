use rustql::{Engine, EngineOptions, format_query_results};
use std::io::{self, IsTerminal, Write};

fn main() {
    let engine = match Engine::open(EngineOptions::default()) {
        Ok(engine) => engine,
        Err(e) => {
            eprintln!("Error: {}", e);
            return;
        }
    };
    let mut session = engine.session();

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

            match session.execute(query) {
                Ok(results) => println!("{}", format_query_results(&results)),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    } else {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let query = input.trim();

        if !query.is_empty() {
            match session.execute(query) {
                Ok(results) => println!("{}", format_query_results(&results)),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}
