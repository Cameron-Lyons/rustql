use rustql::{process_query, reset_database};
use std::time::{Duration, Instant};

fn main() {
    run_case("filtered_scan_age_ge_40", setup_filtered_scan, || {
        let _ = process_query("SELECT * FROM bench_users WHERE age >= 40").unwrap();
    });

    run_case("join_on_grp", setup_join, || {
        let _ = process_query("SELECT * FROM bench_a JOIN bench_b ON bench_a.grp = bench_b.grp")
            .unwrap();
    });

    run_case(
        "explain_analyze_simple_filter",
        setup_explain_analyze,
        || {
            let _ = process_query("EXPLAIN ANALYZE SELECT * FROM bench_metrics WHERE score >= 20")
                .unwrap();
        },
    );
}

fn run_case<F, G>(name: &str, setup: F, run: G)
where
    F: FnOnce(),
    G: Fn(),
{
    setup();

    let iterations = 30usize;
    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        run();
        total += start.elapsed();
    }

    let avg_micros = total.as_micros() as f64 / iterations as f64;
    println!(
        "{:<32} iterations={} avg_us={:.2} total_ms={:.2}",
        name,
        iterations,
        avg_micros,
        total.as_secs_f64() * 1000.0
    );
}

fn setup_filtered_scan() {
    reset_database();
    process_query("CREATE TABLE bench_users (id INTEGER, name TEXT, age INTEGER)").unwrap();
    for i in 0..1000 {
        let q = format!(
            "INSERT INTO bench_users VALUES ({}, 'user{}', {})",
            i,
            i,
            18 + (i % 60)
        );
        process_query(&q).unwrap();
    }
    process_query("CREATE INDEX bench_users_age_idx ON bench_users (age)").unwrap();
}

fn setup_join() {
    reset_database();
    process_query("CREATE TABLE bench_a (id INTEGER, grp INTEGER)").unwrap();
    process_query("CREATE TABLE bench_b (id INTEGER, grp INTEGER)").unwrap();
    for i in 0..600 {
        process_query(&format!("INSERT INTO bench_a VALUES ({}, {})", i, i % 100)).unwrap();
        process_query(&format!("INSERT INTO bench_b VALUES ({}, {})", i, i % 100)).unwrap();
    }
}

fn setup_explain_analyze() {
    reset_database();
    process_query("CREATE TABLE bench_metrics (id INTEGER, score INTEGER)").unwrap();
    for i in 0..500 {
        process_query(&format!(
            "INSERT INTO bench_metrics VALUES ({}, {})",
            i,
            i % 50
        ))
        .unwrap();
    }
}
