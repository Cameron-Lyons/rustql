use rustql::process_query;
use std::io::{self, Write};

fn main() {
    if atty::is(atty::Stream::Stdin) {
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
