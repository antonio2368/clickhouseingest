use async_channel;
use clap::Parser;
use clickhouse_rs::{Block, Options, Pool, ClientHandle};
use log::{debug, info, trace};
use rand::prelude::*;
use std::error::Error;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use std::collections::vec_deque::VecDeque;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long)]
    host: String,
    #[arg(long)]
    password: String,
    #[arg(long, default_value_t = String::from("ingest"))]
    database: String,
    #[arg(long, default_value_t = 1)]
    table_count: u64,
    #[arg(long, default_value_t = 1)]
    insert_workers: u64,
    #[arg(long, default_value_t = false)]
    clean: bool,
    #[arg(long, default_value_t = 1)]
    chunk_size: u64,
    #[arg(long, default_value_t = 0.0)]
    duplication_rate: f64,
}

struct Stats {
    start_time: SystemTime,
    start_count: u64,
}

impl Stats {
    fn new(start_count: u64) -> Self {
        Stats {
            start_time: SystemTime::now(),
            start_count,
        }
    }

    fn print_stats(&self, current_count: u64) {
        let elapsed = self.start_time.elapsed().unwrap();
        if elapsed.as_secs() == 0 {
            return
        }

        info!(
            "Total {} rows in all tables after {}s, speed: {} rows/s",
            current_count,
            elapsed.as_secs(),
            (current_count - self.start_count) / elapsed.as_secs()
        );
    }
}
async fn get_total_count(tables: &Arc<Vec<String>>, client: &mut ClientHandle) -> Result<u64, Box<dyn Error>> {
    debug!("Collecting row counts");
    let tables_set: Vec<String> = tables.iter().map(|table| format!("'{}'", table)).collect();
    let block = client
        .query(format!("SELECT table, total_rows FROM system.tables WHERE database || '.' || table IN ({})", tables_set.join(", ")))
        .fetch_all()
        .await
        .unwrap();

    let total_count = block.rows().map(|row| {
        let total_rows: Option<u64> = row.get("total_rows").unwrap();
        let table: &str = row.get("table").unwrap();
        debug!("{} rows in {}", total_rows.unwrap(), table);

        total_rows.unwrap()
    }).sum();

    Ok(total_count)
}

async fn stats_collector(
    stats: Stats,
    tables: Arc<Vec<String>>,
    pool: Pool,
) -> Result<(), Box<dyn Error>> {
    loop {
        let mut client = pool.get_handle().await.unwrap();
        stats.print_stats(get_total_count(&tables, &mut client).await?);
        sleep(Duration::from_secs(1)).await;
    }
}

async fn insert_worker(
    tables: Arc<Vec<String>>,
    value_receiver: async_channel::Receiver<u64>,
    chunk_size: u64,
    pool: Pool,
    start_barrier: Arc<Barrier>,
    index: u64,
) -> Result<(), Box<dyn Error>> {
    start_barrier.wait().await;
    while let Ok(value) = value_receiver.recv().await {
        let mut client = pool.get_handle().await?;
        let element = rand::thread_rng().gen_range(0..tables.len());
        let table: &String = tables.get(element).unwrap();
        let block: Block =
            Block::new().column("value", (value..(value + chunk_size)).collect::<Vec<u64>>());
        trace!(
            "{}: Inserting from {} to {} into {}",
            index,
            value,
            value + chunk_size,
            table
        );
        client.insert(table, block).await?;
    }

    info!("worker {}: Queue closed, stopping insert worker", index);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let args = Args::parse();

    let url = format!("tcp://{}:9440", args.host);

    info!("Connecting to instance");
    let options = Options::new(url::Url::parse(&url)?)
        .pool_max((args.insert_workers + 100).try_into().unwrap())
        .secure(true)
        .username("default")
        .password(&args.password)
        .connection_timeout(Duration::from_secs(60));

    let pool = Pool::new(options);
    let mut client = pool.get_handle().await?;

    info!("Connected to the instance");

    info!("Will use '{}' database", args.database);

    if args.clean {
        info!("Dropping database {}", args.database);
        client
            .execute(format!("DROP DATABASE IF EXISTS {}", args.database))
            .await?;
        info!("Cleanup done");
    }

    info!("Creating database '{}'", args.database);
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {}", args.database))
        .await?;
    info!("Database created!");

    let tables: Arc<Vec<String>> = Arc::new(
        (0..args.table_count)
            .map(|number| format!("{}.ingest_table{}", args.database, number))
            .collect(),
    );

    info!("Creating {} tables", args.table_count);

    for table in tables.as_ref() {
        debug!("Creating {}", table);
        client
            .execute(format!(
                "CREATE TABLE IF NOT EXISTS {} (value UInt64) ENGINE = MergeTree ORDER BY value",
                table
            ))
            .await?;
        debug!("Created {}", table);
    }

    info!("Created tables!");

    let mut join_handles = Vec::<JoinHandle<()>>::new();

    let (value_sender, value_receiver) = async_channel::bounded(1000);

    let insert_start_barrier =
        Arc::new(Barrier::new((args.insert_workers + 1).try_into().unwrap()));

    info!("Adding insert workers");
    for i in 0..args.insert_workers {
        let tables = Arc::clone(&tables);
        let value_receiver = async_channel::Receiver::clone(&value_receiver);

        debug!("Adding worker {}", i);
        let barrier = insert_start_barrier.clone();
        let pool = pool.clone();
        join_handles.push(tokio::spawn(async move {
            insert_worker(tables, value_receiver, args.chunk_size, pool, barrier, i)
                .await
                .unwrap();
        }));
    }
    let stats = Stats::new(get_total_count(&tables, &mut client).await?);

    insert_start_barrier.wait().await;

    join_handles.push(tokio::spawn(async move {
        stats_collector(stats, tables, pool).await.unwrap();
    }));

    let mut rng = rand::thread_rng();
    let mut old_values: VecDeque<u64> = VecDeque::new();
    loop {
        let value = {
            let should_duplicate = !old_values.is_empty() && rng.gen::<f64>() < args.duplication_rate;
            if should_duplicate {
                let index = rng.gen_range(0..old_values.len());
                debug!("Duplicating value");
                *old_values.get(index).unwrap()
            }
            else
            {
                let value = rng.next_u64();

                if old_values.len() == 100 {
                    old_values.pop_front();
                }

                old_values.push_back(value);
                value
            }
        };
        value_sender.send(value).await?;
    }
}
