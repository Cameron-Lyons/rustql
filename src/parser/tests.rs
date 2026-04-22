use super::parse_script;

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

fn fuzz_sql(seed: u64) -> String {
    const ALPHABET: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_ ',;()*=<>!+-/\n\t";

    let mut rng = Lcg::new(seed);
    let len = (rng.next_u64() % 128) as usize;
    let mut sql = String::with_capacity(len);
    for _ in 0..len {
        let index = (rng.next_u64() as usize) % ALPHABET.len();
        sql.push(ALPHABET[index] as char);
    }
    sql
}

#[test]
fn parser_fuzz_inputs_do_not_panic() {
    for case in 0..512u64 {
        let sql = fuzz_sql(case.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Ok(tokens) = crate::lexer::tokenize(&sql) {
                let _ = parse_script(tokens);
            }
        }));
        assert!(
            result.is_ok(),
            "parser panicked on fuzz case {case}: {sql:?}"
        );
    }
}
