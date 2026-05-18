use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use slatedb::admin::{self, Admin, AdminBuilder};
use slatedb::compactor::{
    CompactionRequest, CompactionSchedulerSupplier, SizeTieredCompactionSchedulerSupplier,
};
use slatedb::config::{
    CheckpointOptions, CompactorOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::seq_tracker::FindOption;
use slatedb::SstReader;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;
use uuid::Uuid;

mod args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_test_writer()
        .init();

    let args: CliArgs = parse_args();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file)?;
    let cancellation_token = CancellationToken::new();
    let admin = AdminBuilder::new(path, object_store).build();

    let ct = cancellation_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
        debug!("intercepted SIGINT ... shutting down background processes");
        // if we cant send a shutdown message it's probably because it's already closed
        ct.cancel();
    });

    match args.command {
        CliCommands::ReadManifest { id } => exec_read_manifest(&admin, id).await?,
        CliCommands::ListManifests { start, end } => exec_list_manifest(&admin, start, end).await?,
        CliCommands::ReadCompactions { id } => exec_read_compactions(&admin, id).await?,
        CliCommands::ListCompactions { start, end } => {
            exec_list_compactions(&admin, start, end).await?
        }
        CliCommands::ReadCompaction { id, compactions_id } => {
            exec_read_compaction(&admin, id, compactions_id).await?
        }
        CliCommands::CreateCheckpoint {
            lifetime,
            source,
            name,
        } => exec_create_checkpoint(&admin, lifetime, source, name).await?,
        CliCommands::RefreshCheckpoint { id, lifetime } => {
            exec_refresh_checkpoint(&admin, id, lifetime).await?;
        }
        CliCommands::DeleteCheckpoint { id } => exec_delete_checkpoint(&admin, id).await?,
        CliCommands::ListCheckpoints { name } => exec_list_checkpoints(&admin, name).await?,
        CliCommands::RunGarbageCollection { resource, min_age } => {
            exec_gc_once(&admin, resource, min_age).await?
        }
        CliCommands::RunCompactor => admin.run_compactor(cancellation_token.clone()).await?,
        CliCommands::ScheduleGarbageCollection {
            manifest,
            wal,
            compacted,
            compactions,
        } => schedule_gc(&admin, manifest, wal, compacted, compactions).await?,
        CliCommands::SubmitCompaction { scheduler, request } => {
            exec_submit_compaction(&admin, scheduler, request).await?
        }

        CliCommands::SeqToTs { seq, round } => {
            exec_seq_to_ts(&admin, seq, matches!(round, FindOption::RoundUp)).await?
        }
        CliCommands::TsToSeq { ts_secs, round } => {
            exec_ts_to_seq(&admin, ts_secs, matches!(round, FindOption::RoundUp)).await?
        }
        CliCommands::ReadLocalSst {
            cache_dir,
            sst_id,
            part_size,
        } => exec_read_local_sst(&args.path, &cache_dir, &sst_id, part_size).await?,
    }

    Ok(())
}

async fn exec_read_manifest(admin: &Admin, id: Option<u64>) -> Result<(), Box<dyn Error>> {
    match admin.read_manifest(id).await? {
        None => {
            println!("no manifest file found")
        }
        Some(manifest) => {
            println!("{}", serde_json::to_string(&manifest)?);
        }
    }
    Ok(())
}

async fn exec_read_compactions(admin: &Admin, id: Option<u64>) -> Result<(), Box<dyn Error>> {
    match admin.read_compactions(id).await? {
        None => {
            println!("no compactions file found")
        }
        Some(compactions) => {
            println!("{}", serde_json::to_string(&compactions)?);
        }
    }
    Ok(())
}

async fn exec_list_manifest(
    admin: &Admin,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let range = match (start, end) {
        (Some(s), Some(e)) => s..e,
        (Some(s), None) => s..u64::MAX,
        (None, Some(e)) => u64::MIN..e,
        _ => u64::MIN..u64::MAX,
    };

    println!(
        "{}",
        serde_json::to_string(&admin.list_manifests(range).await?)?
    );
    Ok(())
}

async fn exec_list_compactions(
    admin: &Admin,
    start: Option<u64>,
    end: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let range = match (start, end) {
        (Some(s), Some(e)) => s..e,
        (Some(s), None) => s..u64::MAX,
        (None, Some(e)) => u64::MIN..e,
        _ => u64::MIN..u64::MAX,
    };

    println!(
        "{}",
        serde_json::to_string(&admin.list_compactions(range).await?)?
    );
    Ok(())
}

async fn exec_submit_compaction(
    admin: &Admin,
    scheduler: String,
    request: CompactionRequest,
) -> Result<(), Box<dyn Error>> {
    let state = admin.read_compactor_state_view().await?;
    let supplier = match scheduler.as_str() {
        "size-tiered" => SizeTieredCompactionSchedulerSupplier,
        _ => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported scheduler: {scheduler}"),
            )))
        }
    };
    let scheduler = supplier.compaction_scheduler(&CompactorOptions::default());
    let specs = scheduler.generate(&state, &request)?;
    let mut compactions = Vec::with_capacity(specs.len());
    for spec in specs {
        compactions.push(admin.submit_compaction(spec).await?);
    }
    let compaction_json = serde_json::to_string(&compactions)?;
    println!("{}", compaction_json);
    Ok(())
}

async fn exec_read_compaction(
    admin: &Admin,
    id: String,
    compactions_id: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let compaction_id = Ulid::from_string(&id)?;
    match admin.read_compaction(compaction_id, compactions_id).await? {
        None => {
            println!("no compaction found");
        }
        Some(compaction) => {
            let compaction_json = serde_json::to_string(&compaction)?;
            println!("{}", compaction_json);
        }
    }
    Ok(())
}

async fn exec_create_checkpoint(
    admin: &Admin,
    lifetime: Option<Duration>,
    source: Option<Uuid>,
    name: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let result = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime,
            source,
            name,
        })
        .await?;
    println!("{:?}", result);
    Ok(())
}

async fn exec_refresh_checkpoint(
    admin: &Admin,
    id: Uuid,
    lifetime: Option<Duration>,
) -> Result<(), Box<dyn Error>> {
    println!("{:?}", admin.refresh_checkpoint(id, lifetime).await?);
    Ok(())
}

async fn exec_delete_checkpoint(admin: &Admin, id: Uuid) -> Result<(), Box<dyn Error>> {
    println!("{:?}", admin.delete_checkpoint(id).await?);
    Ok(())
}

async fn exec_list_checkpoints(
    admin: &Admin,
    name_filter: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let checkpoint = admin.list_checkpoints(name_filter.as_deref()).await?;
    let checkpoint_json = serde_json::to_string(&checkpoint)?;
    println!("{}", checkpoint_json);
    Ok(())
}

async fn exec_gc_once(
    admin: &Admin,
    resource: GcResource,
    min_age: Duration,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(min_age: Duration) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age,
        })
    }
    let gc_opts = match resource {
        GcResource::Manifest => GarbageCollectorOptions {
            manifest_options: create_gc_dir_opts(min_age),
            wal_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
        },
        GcResource::Wal => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: create_gc_dir_opts(min_age),
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
        },
        GcResource::Compacted => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            compacted_options: create_gc_dir_opts(min_age),
            compactions_options: None,
            detach_options: None,
        },
        GcResource::Compactions => GarbageCollectorOptions {
            manifest_options: None,
            wal_options: None,
            compacted_options: None,
            compactions_options: create_gc_dir_opts(min_age),
            detach_options: None,
        },
    };
    admin.run_gc_once(gc_opts).await?;
    Ok(())
}

async fn schedule_gc(
    admin: &Admin,
    manifest_schedule: Option<GcSchedule>,
    wal_schedule: Option<GcSchedule>,
    compacted_schedule: Option<GcSchedule>,
    compactions_schedule: Option<GcSchedule>,
) -> Result<(), Box<dyn Error>> {
    fn create_gc_dir_opts(schedule: GcSchedule) -> Option<GarbageCollectorDirectoryOptions> {
        Some(GarbageCollectorDirectoryOptions {
            interval: Some(schedule.period),
            min_age: schedule.min_age,
        })
    }
    let gc_opts = GarbageCollectorOptions {
        manifest_options: manifest_schedule.and_then(create_gc_dir_opts),
        wal_options: wal_schedule.and_then(create_gc_dir_opts),
        compacted_options: compacted_schedule.and_then(create_gc_dir_opts),
        compactions_options: compactions_schedule.and_then(create_gc_dir_opts),
        detach_options: None,
    };

    admin.run_gc(gc_opts).await?;
    Ok(())
}

async fn exec_seq_to_ts(admin: &Admin, seq: u64, round_up: bool) -> Result<(), Box<dyn Error>> {
    match admin.get_timestamp_for_sequence(seq, round_up).await? {
        Some(ts) => println!("{}", ts.to_rfc3339()),
        None => println!("not found"),
    }
    Ok(())
}

async fn exec_ts_to_seq(admin: &Admin, ts_secs: i64, round_up: bool) -> Result<(), Box<dyn Error>> {
    let ts = Utc
        .timestamp_opt(ts_secs, 0)
        .single()
        .ok_or("invalid unix seconds")?;
    match admin.get_sequence_for_timestamp(ts, round_up).await? {
        Some(seq) => println!("{}", seq),
        None => println!("not found"),
    }
    Ok(())
}

async fn exec_read_local_sst(
    db_path: &str,
    cache_dir: &str,
    sst_id: &str,
    part_size: usize,
) -> Result<(), Box<dyn Error>> {
    let ulid = Ulid::from_string(sst_id)?;

    // Construct the path to the SST's cache folder
    let sst_relative_path = format!("{}/compacted/{}.sst", db_path, ulid);
    let sst_cache_folder = PathBuf::from(cache_dir).join(&sst_relative_path);

    // Determine part size name for filenames
    let part_size_name = if part_size.is_multiple_of(1024 * 1024) {
        format!("{}mb", part_size / (1024 * 1024))
    } else {
        format!("{}kb", part_size / 1024)
    };

    // Read the _head file to determine total size
    let head_path = sst_cache_folder.join("_head");
    let head_content = std::fs::read_to_string(&head_path)?;
    let head: serde_json::Value = serde_json::from_str(&head_content)?;
    let total_size = head["size"].as_u64().ok_or("missing size in head")? as usize;

    println!("SST ID: {}", ulid);
    println!("Total size: {} bytes", total_size);
    println!("Part size: {} bytes", part_size);

    // Read all parts in order
    let mut assembled = BytesMut::with_capacity(total_size);
    let mut part_number = 0u64;
    loop {
        let part_filename = format!("_part{}-{:09}", part_size_name, part_number);
        let part_path = sst_cache_folder.join(&part_filename);
        match std::fs::read(&part_path) {
            Ok(data) => {
                assembled.extend_from_slice(&data);
                part_number += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => return Err(Box::new(e)),
        }
    }

    if assembled.len() < total_size {
        eprintln!(
            "Warning: assembled {} bytes but expected {}. Some parts may be missing.",
            assembled.len(),
            total_size
        );
    }

    // Truncate to actual SST size (last part may be padded/larger)
    let actual_len = total_size.min(assembled.len());
    let sst_bytes = assembled.freeze().slice(..actual_len);

    // Create an in-memory object store and put the SST data at the right path
    let mem_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let obj_path = Path::from(format!("{}/compacted/{}.sst", db_path, ulid));
    mem_store
        .put(&obj_path, PutPayload::from(sst_bytes))
        .await?;

    // Use SstReader to open and inspect the SST
    let reader = SstReader::new(db_path, mem_store, None, None);
    let sst_file = reader.open(ulid).await?;

    // Print SST info
    let info = sst_file.info();
    println!("\n=== SST Info ===");
    println!("{:?}", info);

    // Print stats
    match sst_file.stats().await? {
        Some(stats) => {
            println!("\n=== SST Stats ===");
            println!("{:#?}", stats);
        }
        None => println!("\n=== No stats available ==="),
    }

    // Print index
    let index = sst_file.index().await?;
    println!("\n=== SST Index ({} blocks) ===", index.len());
    for (i, (offset, first_key)) in index.iter().enumerate() {
        let key_str = String::from_utf8_lossy(first_key);
        println!("  Block {}: offset={}, first_key={:?}", i, offset, key_str);
    }

    // Print all row entries from all blocks
    println!("\n=== Row Entries ===");
    for block_idx in 0..index.len() {
        let rows = sst_file.read_block(block_idx).await?;
        for row in &rows {
            println!("{:?}", row);
        }
    }

    Ok(())
}
