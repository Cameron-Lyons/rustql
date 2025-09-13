mod ast;
mod executor;
mod lexer;
mod parser;

use std::io::{self, Write};

fn main() {
    // Check if running in non-interactive mode (input is piped)
    if atty::is(atty::Stream::Stdin) {
        // Interactive mode
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

            match process_query(query) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    } else {
        // Non-interactive mode - process single query from stdin
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let query = input.trim();

        if !query.is_empty() {
            match process_query(query) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

fn process_query(query: &str) -> Result<String, String> {
    let tokens = lexer::tokenize(query)?;
    let statement = parser::parse(tokens)?;
    executor::execute(statement)
}
