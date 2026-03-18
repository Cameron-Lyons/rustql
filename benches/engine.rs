use rustql::{Engine, EngineOptions, Session, StorageMode};
use std::env;
use std::fmt;
use std::hint::black_box;
use std::process;
use std::time::{Duration, Instant};

const INSERT_BATCH_SIZE: usize = 25;

fn main() {
    let config = BenchConfig::from_env_and_args();
    let definitions = bench_definitions();

    if config.list_only {
        print_case_list(&definitions);
        return;
    }

    let selected: Vec<_> = definitions
        .into_iter()
        .filter(|definition| {
            config.matches(definition.name) || config.matches(definition.description)
        })
        .collect();

    if selected.is_empty() {
        eprintln!(
            "No benchmark cases matched filter {:?}. Use --list to see available cases.",
            config.filter
        );
        process::exit(2);
    }

    println!(
        "rustql benchmark suite profile={} warmup={} samples={} filter={}",
        config.profile,
        config.warmup,
        config.samples,
        config.filter.as_deref().unwrap_or("*")
    );
    println!(
        "{:<28} {:<24} {:>10} {:>8} {:>11} {:>11} {:>11} {:>11} {:>11} {:>11}",
        "case",
        "scale",
        "setup_ms",
        "samples",
        "mean_us",
        "median_us",
        "p95_us",
        "min_us",
        "max_us",
        "stddev_us"
    );

    for definition in selected {
        let setup_start = Instant::now();
        let mut prepared = (definition.prepare)(config.profile);
        let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;
        let stats = run_benchmark(&config, &mut prepared);
        print_stats(
            definition.name,
            &prepared.scale,
            setup_ms,
            config.samples,
            &stats,
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum BenchProfile {
    Smoke,
    Default,
    Large,
}

impl BenchProfile {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "smoke" => Some(Self::Smoke),
            "default" => Some(Self::Default),
            "large" => Some(Self::Large),
            _ => None,
        }
    }

    fn default_warmup(self) -> usize {
        match self {
            Self::Smoke => 1,
            Self::Default => 3,
            Self::Large => 5,
        }
    }

    fn default_samples(self) -> usize {
        match self {
            Self::Smoke => 3,
            Self::Default => 10,
            Self::Large => 15,
        }
    }

    fn indexed_filter_rows(self) -> usize {
        match self {
            Self::Smoke => 2_000,
            Self::Default => 15_000,
            Self::Large => 75_000,
        }
    }

    fn join_rows(self) -> usize {
        match self {
            Self::Smoke => 1_000,
            Self::Default => 4_000,
            Self::Large => 12_000,
        }
    }

    fn lateral_users(self) -> usize {
        match self {
            Self::Smoke => 500,
            Self::Default => 2_000,
            Self::Large => 8_000,
        }
    }

    fn aggregate_rows(self) -> usize {
        match self {
            Self::Smoke => 5_000,
            Self::Default => 20_000,
            Self::Large => 80_000,
        }
    }

    fn window_rows(self) -> usize {
        match self {
            Self::Smoke => 5_000,
            Self::Default => 20_000,
            Self::Large => 80_000,
        }
    }
}

impl fmt::Display for BenchProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Smoke => "smoke",
            Self::Default => "default",
            Self::Large => "large",
        };
        f.write_str(name)
    }
}

#[derive(Debug)]
struct BenchConfig {
    filter: Option<String>,
    warmup: usize,
    samples: usize,
    profile: BenchProfile,
    list_only: bool,
}

impl BenchConfig {
    fn from_env_and_args() -> Self {
        let mut profile = env::var("RUSTQL_BENCH_PROFILE")
            .ok()
            .as_deref()
            .and_then(BenchProfile::parse)
            .unwrap_or(BenchProfile::Default);
        let mut filter = env::var("RUSTQL_BENCH_FILTER")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let mut warmup = env_usize("RUSTQL_BENCH_WARMUP");
        let mut samples = env_usize("RUSTQL_BENCH_SAMPLES");
        let mut list_only = false;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bench" => {}
                "--filter" => filter = Some(next_arg_value(&mut args, "--filter")),
                "--warmup" => {
                    warmup = Some(parse_usize_arg(
                        "--warmup",
                        &next_arg_value(&mut args, "--warmup"),
                    ))
                }
                "--samples" => {
                    samples = Some(parse_usize_arg(
                        "--samples",
                        &next_arg_value(&mut args, "--samples"),
                    ))
                }
                "--profile" => {
                    let value = next_arg_value(&mut args, "--profile");
                    profile = BenchProfile::parse(&value).unwrap_or_else(|| {
                        eprintln!(
                            "Unsupported profile '{value}'. Expected one of: smoke, default, large."
                        );
                        process::exit(2);
                    });
                }
                "--list" => list_only = true,
                "--help" | "-h" => {
                    print_usage();
                    process::exit(0);
                }
                other => {
                    eprintln!("Unrecognized argument '{other}'. Use --help for usage.");
                    process::exit(2);
                }
            }
        }

        let warmup = warmup.unwrap_or_else(|| profile.default_warmup());
        let samples = samples.unwrap_or_else(|| profile.default_samples());
        if samples == 0 {
            eprintln!("--samples must be at least 1.");
            process::exit(2);
        }

        Self {
            filter,
            warmup,
            samples,
            profile,
            list_only,
        }
    }

    fn matches(&self, value: &str) -> bool {
        let Some(filter) = self.filter.as_deref() else {
            return true;
        };
        value
            .to_ascii_lowercase()
            .contains(&filter.to_ascii_lowercase())
    }
}

struct BenchDefinition {
    name: &'static str,
    description: &'static str,
    prepare: fn(BenchProfile) -> PreparedBench,
}

struct PreparedBench {
    scale: String,
    run: Box<dyn FnMut()>,
}

#[derive(Debug)]
struct BenchStats {
    mean_us: f64,
    median_us: f64,
    p95_us: f64,
    min_us: f64,
    max_us: f64,
    stddev_us: f64,
}

fn bench_definitions() -> Vec<BenchDefinition> {
    vec![
        BenchDefinition {
            name: "indexed_filter",
            description: "Memory benchmark for indexed filtering with a bounded result shape.",
            prepare: prepare_indexed_filter,
        },
        BenchDefinition {
            name: "grouped_join",
            description: "Memory benchmark for join execution followed by grouped aggregation.",
            prepare: prepare_grouped_join,
        },
        BenchDefinition {
            name: "lateral_top1",
            description: "Memory benchmark for LEFT JOIN LATERAL top-1 lookups per outer row.",
            prepare: prepare_lateral_top1,
        },
        BenchDefinition {
            name: "grouped_multi_aggregate",
            description: "Memory benchmark for grouped COUNT/SUM/AVG/MODE aggregation.",
            prepare: prepare_grouped_multi_aggregate,
        },
        BenchDefinition {
            name: "window_rank",
            description: "Memory benchmark for partitioned window ranking with final ordering.",
            prepare: prepare_window_rank,
        },
        BenchDefinition {
            name: "rollup",
            description: "Memory benchmark for grouped aggregation with ROLLUP.",
            prepare: prepare_rollup,
        },
    ]
}

fn print_case_list(definitions: &[BenchDefinition]) {
    println!("Available benchmark cases:");
    for definition in definitions {
        println!("  {:<24} {}", definition.name, definition.description);
    }
}

fn print_usage() {
    println!("Usage: cargo bench --bench engine -- [options]");
    println!();
    println!("Options:");
    println!("  --list                List benchmark cases and exit");
    println!("  --filter <text>       Run only cases whose name or description matches");
    println!("  --profile <name>      Use smoke, default, or large scale sizes");
    println!("  --warmup <count>      Override warmup iterations");
    println!("  --samples <count>     Override measured sample count");
    println!("  --help                Show this help text");
    println!();
    println!("Environment overrides:");
    println!("  RUSTQL_BENCH_FILTER");
    println!("  RUSTQL_BENCH_PROFILE");
    println!("  RUSTQL_BENCH_WARMUP");
    println!("  RUSTQL_BENCH_SAMPLES");
}

fn run_benchmark(config: &BenchConfig, bench: &mut PreparedBench) -> BenchStats {
    for _ in 0..config.warmup {
        (bench.run)();
    }

    let mut samples_us = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let start = Instant::now();
        (bench.run)();
        samples_us.push(duration_us(start.elapsed()));
    }

    compute_stats(samples_us)
}

fn compute_stats(mut samples_us: Vec<f64>) -> BenchStats {
    samples_us.sort_by(|left, right| left.total_cmp(right));
    let count = samples_us.len() as f64;
    let mean_us = samples_us.iter().copied().sum::<f64>() / count;
    let median_us = median(&samples_us);
    let p95_us = percentile(&samples_us, 0.95);
    let min_us = *samples_us.first().unwrap_or(&0.0);
    let max_us = *samples_us.last().unwrap_or(&0.0);
    let variance = samples_us
        .iter()
        .map(|sample| {
            let delta = sample - mean_us;
            delta * delta
        })
        .sum::<f64>()
        / count;

    BenchStats {
        mean_us,
        median_us,
        p95_us,
        min_us,
        max_us,
        stddev_us: variance.sqrt(),
    }
}

fn median(sorted_samples: &[f64]) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }

    let mid = sorted_samples.len() / 2;
    if sorted_samples.len().is_multiple_of(2) {
        (sorted_samples[mid - 1] + sorted_samples[mid]) / 2.0
    } else {
        sorted_samples[mid]
    }
}

fn percentile(sorted_samples: &[f64], percentile: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }

    let rank = ((sorted_samples.len() - 1) as f64 * percentile).ceil() as usize;
    sorted_samples[rank.min(sorted_samples.len() - 1)]
}

fn print_stats(name: &str, scale: &str, setup_ms: f64, samples: usize, stats: &BenchStats) {
    println!(
        "{:<28} {:<24} {:>10.2} {:>8} {:>11.2} {:>11.2} {:>11.2} {:>11.2} {:>11.2} {:>11.2}",
        name,
        scale,
        setup_ms,
        samples,
        stats.mean_us,
        stats.median_us,
        stats.p95_us,
        stats.min_us,
        stats.max_us,
        stats.stddev_us
    );
}

fn prepare_indexed_filter(profile: BenchProfile) -> PreparedBench {
    let rows = profile.indexed_filter_rows();
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_users (
                    id INTEGER,
                    name TEXT,
                    age INTEGER,
                    grp INTEGER,
                    score INTEGER
                );
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_users", rows, |index| {
            format!(
                "({}, 'user{}', {}, {}, {})",
                index,
                index,
                18 + (index % 60),
                index % 128,
                1_000 - (index % 1_000)
            )
        });
    }

    let query = "SELECT COUNT(*) AS total_rows FROM bench_users WHERE age >= 40";
    PreparedBench {
        scale: format!("{rows} rows"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn prepare_grouped_join(profile: BenchProfile) -> PreparedBench {
    let rows = profile.join_rows();
    let group_count = 200usize;
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_a (id INTEGER, grp INTEGER, amount INTEGER);
                CREATE TABLE bench_b (id INTEGER, grp INTEGER, flag INTEGER);
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_a", rows, |index| {
            format!(
                "({}, {}, {})",
                index,
                index % group_count,
                10 + (index % 900)
            )
        });
        insert_rows(&mut session, "bench_b", rows, |index| {
            format!("({}, {}, {})", index, index % group_count, index % 2)
        });
    }

    let query = "SELECT bench_a.grp, COUNT(*) AS total, SUM(bench_a.amount) AS total_amount \
                 FROM bench_a \
                 JOIN bench_b ON bench_a.grp = bench_b.grp \
                 GROUP BY bench_a.grp \
                 ORDER BY bench_a.grp";
    PreparedBench {
        scale: format!("{rows}x{rows} rows"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn prepare_lateral_top1(profile: BenchProfile) -> PreparedBench {
    let users = profile.lateral_users();
    let orders_per_user = 6usize;
    let total_orders = users * orders_per_user;
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_users (id INTEGER, region INTEGER);
                CREATE TABLE bench_orders (id INTEGER, user_id INTEGER, amount INTEGER);
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_users", users, |index| {
            format!("({}, {})", index + 1, index % 32)
        });
        insert_rows(&mut session, "bench_orders", total_orders, |index| {
            let user_id = (index / orders_per_user) + 1;
            let ordinal = index % orders_per_user;
            let amount = ((user_id * 37) + (ordinal * 113)) % 10_000;
            format!("({}, {}, {})", index + 1, user_id, amount)
        });
        session
            .execute_one("CREATE INDEX bench_orders_user_idx ON bench_orders (user_id)")
            .unwrap();
    }

    let query = "SELECT bench_users.id, recent.amount \
                 FROM bench_users \
                 LEFT JOIN LATERAL ( \
                     SELECT amount \
                     FROM bench_orders \
                     WHERE bench_orders.user_id = bench_users.id \
                     ORDER BY amount DESC \
                     FETCH FIRST 1 ROW ONLY \
                 ) AS recent \
                 ORDER BY bench_users.id";
    PreparedBench {
        scale: format!("{users} users/{total_orders} orders"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn prepare_grouped_multi_aggregate(profile: BenchProfile) -> PreparedBench {
    let rows = profile.aggregate_rows();
    let region_count = 32usize;
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_metrics (id INTEGER, region TEXT, amount INTEGER);
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_metrics", rows, |index| {
            format!(
                "({}, 'region_{:02}', {})",
                index,
                index % region_count,
                100 + ((index * 17) % 4_000)
            )
        });
    }

    let query = "SELECT region, \
                        COUNT(*) AS total_rows, \
                        SUM(amount) AS sum_amount, \
                        AVG(amount) AS avg_amount, \
                        MODE(amount) AS modal_amount \
                 FROM bench_metrics \
                 GROUP BY region \
                 ORDER BY region";
    PreparedBench {
        scale: format!("{rows} rows"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn prepare_window_rank(profile: BenchProfile) -> PreparedBench {
    let rows = profile.window_rows();
    let team_count = 64usize;
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_scores (id INTEGER, team TEXT, score INTEGER);
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_scores", rows, |index| {
            format!(
                "({}, 'team_{:02}', {})",
                index,
                index % team_count,
                ((rows - index) % 5_000) + (index % 37)
            )
        });
    }

    let query = "SELECT team, \
                        score, \
                        ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC) AS rn, \
                        DENSE_RANK() OVER (PARTITION BY team ORDER BY score DESC) AS dr \
                 FROM bench_scores \
                 ORDER BY team, score DESC \
                 FETCH FIRST 500 ROWS ONLY";
    PreparedBench {
        scale: format!("{rows} rows"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn prepare_rollup(profile: BenchProfile) -> PreparedBench {
    let rows = profile.aggregate_rows();
    let region_count = 16usize;
    let product_count = 24usize;
    let engine = open_memory_engine();

    {
        let mut session = engine.session();
        session
            .execute_script(
                "
                CREATE TABLE bench_sales (
                    id INTEGER,
                    region TEXT,
                    product TEXT,
                    amount INTEGER
                );
                ",
            )
            .unwrap();
        insert_rows(&mut session, "bench_sales", rows, |index| {
            format!(
                "({}, 'region_{:02}', 'product_{:02}', {})",
                index,
                index % region_count,
                index % product_count,
                50 + ((index * 19) % 5_000)
            )
        });
    }

    let query = "SELECT region, product, SUM(amount) AS total \
                 FROM bench_sales \
                 GROUP BY ROLLUP(region, product) \
                 ORDER BY region, product";
    PreparedBench {
        scale: format!("{rows} rows"),
        run: Box::new(move || {
            let mut session = engine.session();
            black_box(session.execute_one(query).unwrap());
        }),
    }
}

fn open_memory_engine() -> Engine {
    Engine::open(EngineOptions {
        storage: StorageMode::Memory,
    })
    .unwrap()
}

fn insert_rows<F>(session: &mut Session<'_>, table: &str, row_count: usize, row_sql: F)
where
    F: FnMut(usize) -> String,
{
    insert_rows_with_batch(session, table, row_count, INSERT_BATCH_SIZE, row_sql);
}

fn insert_rows_with_batch<F>(
    session: &mut Session<'_>,
    table: &str,
    row_count: usize,
    batch_size: usize,
    mut row_sql: F,
) where
    F: FnMut(usize) -> String,
{
    let mut batch = Vec::with_capacity(batch_size);
    for index in 0..row_count {
        batch.push(row_sql(index));
        if batch.len() == batch_size {
            flush_insert_batch(session, table, &mut batch);
        }
    }

    if !batch.is_empty() {
        flush_insert_batch(session, table, &mut batch);
    }
}

fn flush_insert_batch(session: &mut Session<'_>, table: &str, batch: &mut Vec<String>) {
    let sql = format!("INSERT INTO {table} VALUES {}", batch.join(", "));
    session.execute_one(&sql).unwrap();
    batch.clear();
}

fn duration_us(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn env_usize(name: &str) -> Option<usize> {
    let value = env::var(name).ok()?;
    Some(parse_usize_arg(name, &value))
}

fn parse_usize_arg(name: &str, value: &str) -> usize {
    value.parse::<usize>().unwrap_or_else(|_| {
        eprintln!("Expected a positive integer for {name}, got '{value}'.");
        process::exit(2);
    })
}

fn next_arg_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next().unwrap_or_else(|| {
        eprintln!("Missing value for {flag}.");
        process::exit(2);
    })
}
