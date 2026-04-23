#![no_main]

use libfuzzer_sys::fuzz_target;
use rustql::lexer::tokenize_spanned;
use rustql::parser::parse_script_spanned;

fuzz_target!(|data: &[u8]| {
    let sql = String::from_utf8_lossy(data);
    if let Ok(tokens) = tokenize_spanned(sql.as_ref()) {
        let _ = parse_script_spanned(tokens);
    }
});
