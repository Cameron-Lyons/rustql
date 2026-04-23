#![no_main]

use libfuzzer_sys::fuzz_target;
use rustql::lexer::tokenize;
use rustql::parser::parse_script;

fuzz_target!(|data: &[u8]| {
    let sql = String::from_utf8_lossy(data);
    if let Ok(tokens) = tokenize(sql.as_ref()) {
        let _ = parse_script(tokens);
    }
});
