use crate::args::{parse_args, CliArgs, CliCommands, GcResource, GcSchedule};
use bytes::{Buf, BytesMut};
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

    // Handle commands that don't need a remote object store connection
    if let CliCommands::ReadLocalSst { sst_dir, part_size } = &args.command {
        return exec_read_local_sst(sst_dir, *part_size)
            .await
            .map_err(Into::into);
    }

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
        CliCommands::ReadLocalSst { .. } => unreachable!("handled above"),
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

/// Describes which part file a given byte range falls into.
#[allow(dead_code)]
struct PartFileMapping {
    /// The part file index
    part_number: u64,
    /// The filename of the part file
    filename: String,
    /// The full path to the part file
    path: PathBuf,
    /// The byte range within the SST that this part covers
    sst_byte_range: std::ops::Range<usize>,
}

/// Validate a CRC32 checksum on a byte slice. Returns Ok(()) or an error
/// describing the mismatch including the exact byte offset of the checksum.
fn validate_checksum_at(
    data: &[u8],
    region_name: &str,
    region_start_offset: usize,
) -> Result<(), String> {
    if data.len() < 4 {
        return Err(format!(
            "{}: region too small ({} bytes) to contain a checksum",
            region_name,
            data.len()
        ));
    }
    let payload = &data[..data.len() - 4];
    let stored_checksum =
        u32::from_be_bytes(data[data.len() - 4..].try_into().expect("4 bytes for u32"));
    let computed_checksum = crc32fast::hash(payload);
    if computed_checksum != stored_checksum {
        let checksum_offset = region_start_offset + data.len() - 4;
        Err(format!(
            "{}: CHECKSUM MISMATCH at SST offset {}..{} (checksum bytes at offset {})\n  \
             stored=0x{:08x}, computed=0x{:08x}, payload_size={} bytes",
            region_name,
            region_start_offset,
            region_start_offset + data.len(),
            checksum_offset,
            stored_checksum,
            computed_checksum,
            payload.len(),
        ))
    } else {
        Ok(())
    }
}

/// Map a byte offset in the SST to the part file that contains it.
fn offset_to_part_info(
    offset: usize,
    part_size: usize,
    part_size_name: &str,
) -> (u64, String, usize) {
    let part_number = (offset / part_size) as u64;
    let filename = format!("_part{}-{:09}", part_size_name, part_number);
    let offset_within_part = offset % part_size;
    (part_number, filename, offset_within_part)
}

async fn exec_read_local_sst(sst_dir: &str, part_size: usize) -> Result<(), Box<dyn Error>> {
    let sst_cache_folder = PathBuf::from(sst_dir);

    // Validate the directory exists
    if !sst_cache_folder.is_dir() {
        return Err(format!(
            "SST directory does not exist or is not a directory: {}\n\
             This should be the folder containing _head and _part* files.",
            sst_cache_folder.display()
        )
        .into());
    }

    // Determine part size name for filenames
    let part_size_name = if part_size.is_multiple_of(1024 * 1024) {
        format!("{}mb", part_size / (1024 * 1024))
    } else {
        format!("{}kb", part_size / 1024)
    };

    // Read the _head file to determine total size and object store location
    let head_path = sst_cache_folder.join("_head");
    let head_content = std::fs::read_to_string(&head_path).map_err(|e| {
        format!(
            "Failed to read _head file at {}:\n  {}\n\
             The _head file should be inside the SST cache folder.\n\
             Directory contents: {}",
            head_path.display(),
            e,
            list_dir_contents(&sst_cache_folder),
        )
    })?;
    let head: serde_json::Value = serde_json::from_str(&head_content).map_err(|e| {
        format!(
            "Failed to parse _head JSON at {}:\n  {}\n\
             Content (first 200 chars): {:?}",
            head_path.display(),
            e,
            &head_content[..head_content.len().min(200)]
        )
    })?;
    let total_size = head["size"].as_u64().ok_or_else(|| {
        format!(
            "Missing 'size' field in _head JSON.\n  Full content: {}",
            head_content
        )
    })? as usize;
    let location = head["location"]
        .as_str()
        .ok_or_else(|| {
            format!(
                "Missing 'location' field in _head JSON.\n  Full content: {}",
                head_content
            )
        })?
        .to_string();

    // Derive the ULID from the location path (e.g. "my-db/compacted/01KRTAT2C8QPNR6D01PF2CQKM0.sst")
    let sst_filename = location
        .rsplit('/')
        .next()
        .ok_or_else(|| format!("Cannot parse SST filename from location: {:?}", location))?;
    let ulid_str = sst_filename.strip_suffix(".sst").unwrap_or(sst_filename);
    let ulid = Ulid::from_string(ulid_str).map_err(|e| {
        format!(
            "Cannot parse ULID from SST filename {:?} (extracted {:?}): {}\n\
             Location from _head: {:?}",
            sst_filename, ulid_str, e, location
        )
    })?;

    // Derive the db_path (the root path prefix before "compacted/<id>.sst")
    let db_path = location
        .strip_suffix(&format!("compacted/{}.sst", ulid))
        .map(|p| p.trim_end_matches('/'))
        .unwrap_or("");

    println!("=== Local SST Reader (Corruption Debugger) ===");
    println!("SST directory: {}", sst_cache_folder.display());
    println!("SST ID (ULID): {}", ulid);
    println!("Object store location: {:?}", location);
    println!("Derived DB path: {:?}", db_path);
    println!("Expected total size: {} bytes", total_size);
    println!("Part size: {} bytes ({})", part_size, part_size_name);
    println!();

    // Read all parts in order, tracking which file contributes which bytes
    let mut assembled = BytesMut::with_capacity(total_size);
    let mut part_mappings: Vec<PartFileMapping> = Vec::new();
    let mut part_number = 0u64;
    loop {
        let part_filename = format!("_part{}-{:09}", part_size_name, part_number);
        let part_path = sst_cache_folder.join(&part_filename);
        match std::fs::read(&part_path) {
            Ok(data) => {
                let start = assembled.len();
                assembled.extend_from_slice(&data);
                let end = assembled.len();
                part_mappings.push(PartFileMapping {
                    part_number,
                    filename: part_filename,
                    path: part_path,
                    sst_byte_range: start..end,
                });
                part_number += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => {
                return Err(format!(
                    "Error reading part file {}:\n  {}",
                    sst_cache_folder
                        .join(&format!("_part{}-{:09}", part_size_name, part_number))
                        .display(),
                    e
                )
                .into());
            }
        }
    }

    if part_mappings.is_empty() {
        return Err(format!(
            "No part files found in {}\n\
             Expected files matching pattern: _part{}-*\n\
             Directory contents: {}",
            sst_cache_folder.display(),
            part_size_name,
            list_dir_contents(&sst_cache_folder),
        )
        .into());
    }

    println!("=== Part File Mapping ===");
    for pm in &part_mappings {
        println!(
            "  {} -> SST bytes [{}..{}) ({} bytes)",
            pm.filename,
            pm.sst_byte_range.start,
            pm.sst_byte_range.end,
            pm.sst_byte_range.end - pm.sst_byte_range.start,
        );
    }
    println!();

    if assembled.len() < total_size {
        eprintln!(
            "ERROR: Assembled {} bytes from {} part files, but _head says size is {} bytes.",
            assembled.len(),
            part_mappings.len(),
            total_size
        );
        eprintln!(
            "  Expected at least {} parts of {} bytes each.",
            (total_size + part_size - 1) / part_size,
            part_size
        );
        eprintln!("  PARTS ARE MISSING. Continuing with partial data...");
        eprintln!();
    }

    // Truncate to actual SST size
    let actual_len = total_size.min(assembled.len());
    let sst_bytes = assembled.freeze().slice(..actual_len);

    // --- Parse the SST footer (last 10 bytes) ---
    const NUM_FOOTER_BYTES: usize = 10;
    if sst_bytes.len() <= NUM_FOOTER_BYTES {
        return Err(format!(
            "SST data is too small ({} bytes) to contain a valid footer (need > {} bytes).\n\
             The file is either truncated or not a valid SST.",
            sst_bytes.len(),
            NUM_FOOTER_BYTES
        )
        .into());
    }

    let footer_start = sst_bytes.len() - NUM_FOOTER_BYTES;
    let footer = sst_bytes.slice(footer_start..);
    let metadata_offset = footer.slice(0..8).get_u64() as usize;
    let version = footer.slice(8..10).get_u16();

    println!(
        "=== SST Footer (last {} bytes at offset {}) ===",
        NUM_FOOTER_BYTES, footer_start
    );
    println!(
        "  Metadata offset: {} (0x{:x})",
        metadata_offset, metadata_offset
    );
    println!("  Format version: {}", version);
    {
        let (_pn, pf, off_in_part) = offset_to_part_info(footer_start, part_size, &part_size_name);
        println!(
            "  Footer located in: {} at byte {} within that part",
            pf, off_in_part
        );
    }
    println!();

    if metadata_offset >= footer_start {
        return Err(format!(
            "CORRUPTION: metadata_offset ({}) >= footer_start ({}).\n\
             The footer is invalid — the file may be truncated or completely corrupted.",
            metadata_offset, footer_start
        )
        .into());
    }

    // --- Validate info/metadata block ---
    let info_region = &sst_bytes[metadata_offset..footer_start];
    println!(
        "=== Info/Metadata Block (offset {}..{}, {} bytes) ===",
        metadata_offset,
        footer_start,
        info_region.len()
    );
    {
        let (_pn, pf, off_in_part) =
            offset_to_part_info(metadata_offset, part_size, &part_size_name);
        println!(
            "  Starts in: {} at byte {} within that part",
            pf, off_in_part
        );
    }
    match validate_checksum_at(info_region, "Info/Metadata", metadata_offset) {
        Ok(()) => println!("  Checksum: OK"),
        Err(e) => {
            eprintln!("  CORRUPTION: {}", e);
            let checksum_sst_offset = metadata_offset + info_region.len() - 4;
            let (_pn, pf, off_in_part) =
                offset_to_part_info(checksum_sst_offset, part_size, &part_size_name);
            eprintln!(
                "  Checksum bytes located in: {} at byte {} within that part",
                pf, off_in_part
            );
        }
    }
    println!();

    // Now use SstReader to parse the SST for structural info
    let mem_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let obj_path = Path::from(location.as_str());
    mem_store
        .put(&obj_path, PutPayload::from(sst_bytes.clone()))
        .await?;

    let reader = SstReader::new(db_path, mem_store, None, None);
    let sst_file = match reader.open(ulid).await {
        Ok(f) => f,
        Err(e) => {
            return Err(format!(
                "Failed to open SST via SstReader: {}\n\
                 The info/metadata block is likely corrupted (see above).\n\
                 Location used: {:?}, DB path: {:?}",
                e, location, db_path
            )
            .into());
        }
    };

    let info = sst_file.info();
    println!("=== SST Info ===");
    println!("  {:?}", info);
    println!(
        "  filter_offset: {}, filter_len: {}",
        info.filter_offset, info.filter_len
    );
    println!(
        "  index_offset: {}, index_len: {}",
        info.index_offset, info.index_len
    );
    println!(
        "  stats_offset: {}, stats_len: {}",
        info.stats_offset, info.stats_len
    );
    println!();

    // --- Validate filter block ---
    if info.filter_len > 0 {
        let f_start = info.filter_offset as usize;
        let f_end = f_start + info.filter_len as usize;
        println!(
            "=== Filter Block (offset {}..{}, {} bytes) ===",
            f_start, f_end, info.filter_len
        );
        if f_end <= sst_bytes.len() {
            let filter_region = &sst_bytes[f_start..f_end];
            let (_, pf, off_in_part) = offset_to_part_info(f_start, part_size, &part_size_name);
            println!(
                "  Starts in: {} at byte {} within that part",
                pf, off_in_part
            );
            match validate_checksum_at(filter_region, "Filter", f_start) {
                Ok(()) => println!("  Checksum: OK"),
                Err(e) => {
                    eprintln!("  CORRUPTION: {}", e);
                    let cs_off = f_start + filter_region.len() - 4;
                    let (_, pf2, off2) = offset_to_part_info(cs_off, part_size, &part_size_name);
                    eprintln!(
                        "  Checksum bytes located in: {} at byte {} within that part",
                        pf2, off2
                    );
                }
            }
        } else {
            eprintln!(
                "  ERROR: filter block extends beyond SST data (need offset {} but file is only {} bytes)",
                f_end,
                sst_bytes.len()
            );
        }
        println!();
    }

    // --- Validate index block ---
    {
        let i_start = info.index_offset as usize;
        let i_end = i_start + info.index_len as usize;
        println!(
            "=== Index Block (offset {}..{}, {} bytes) ===",
            i_start, i_end, info.index_len
        );
        if i_end <= sst_bytes.len() {
            let index_region = &sst_bytes[i_start..i_end];
            let (_, pf, off_in_part) = offset_to_part_info(i_start, part_size, &part_size_name);
            println!(
                "  Starts in: {} at byte {} within that part",
                pf, off_in_part
            );
            match validate_checksum_at(index_region, "Index", i_start) {
                Ok(()) => println!("  Checksum: OK"),
                Err(e) => {
                    eprintln!("  CORRUPTION: {}", e);
                    let cs_off = i_start + index_region.len() - 4;
                    let (_, pf2, off2) = offset_to_part_info(cs_off, part_size, &part_size_name);
                    eprintln!(
                        "  Checksum bytes located in: {} at byte {} within that part",
                        pf2, off2
                    );
                }
            }
        } else {
            eprintln!(
                "  ERROR: index block extends beyond SST data (need offset {} but file is only {} bytes)",
                i_end,
                sst_bytes.len()
            );
        }
        println!();
    }

    // --- Validate stats block ---
    if info.stats_len > 0 {
        let s_start = info.stats_offset as usize;
        let s_end = s_start + info.stats_len as usize;
        println!(
            "=== Stats Block (offset {}..{}, {} bytes) ===",
            s_start, s_end, info.stats_len
        );
        if s_end <= sst_bytes.len() {
            let stats_region = &sst_bytes[s_start..s_end];
            let (_, pf, off_in_part) = offset_to_part_info(s_start, part_size, &part_size_name);
            println!(
                "  Starts in: {} at byte {} within that part",
                pf, off_in_part
            );
            match validate_checksum_at(stats_region, "Stats", s_start) {
                Ok(()) => println!("  Checksum: OK"),
                Err(e) => {
                    eprintln!("  CORRUPTION: {}", e);
                    let cs_off = s_start + stats_region.len() - 4;
                    let (_, pf2, off2) = offset_to_part_info(cs_off, part_size, &part_size_name);
                    eprintln!(
                        "  Checksum bytes located in: {} at byte {} within that part",
                        pf2, off2
                    );
                }
            }
        } else {
            eprintln!(
                "  ERROR: stats block extends beyond SST data (need offset {} but file is only {} bytes)",
                s_end,
                sst_bytes.len()
            );
        }
        println!();
    }

    // --- Validate data blocks ---
    let index = match sst_file.index().await {
        Ok(idx) => idx,
        Err(e) => {
            eprintln!("ERROR: Failed to read SST index: {}", e);
            eprintln!("  The index block is likely corrupted (see above).");
            return Ok(());
        }
    };

    println!("=== Data Blocks ({} total) ===", index.len());
    let mut corruption_count = 0usize;
    for (block_idx, (block_offset, first_key)) in index.iter().enumerate() {
        // Determine block end: next block's offset, or filter_offset for the last block
        let block_start = *block_offset as usize;
        let block_end = if block_idx + 1 < index.len() {
            index[block_idx + 1].0 as usize
        } else {
            info.filter_offset as usize
        };

        let key_preview = String::from_utf8_lossy(first_key);
        let block_size = block_end - block_start;

        if block_end > sst_bytes.len() {
            eprintln!(
                "  Block {:4}: offset={}..{} ({} bytes) first_key={:?} -- ERROR: extends beyond SST data",
                block_idx, block_start, block_end, block_size, key_preview
            );
            corruption_count += 1;
            continue;
        }

        let block_data = &sst_bytes[block_start..block_end];
        let block_name = format!("DataBlock[{}]", block_idx);
        match validate_checksum_at(block_data, &block_name, block_start) {
            Ok(()) => {
                println!(
                    "  Block {:4}: offset={}..{} ({} bytes) first_key={:?} -- OK",
                    block_idx, block_start, block_end, block_size, key_preview
                );
            }
            Err(e) => {
                corruption_count += 1;
                let (_, pf_start, off_start) =
                    offset_to_part_info(block_start, part_size, &part_size_name);
                let checksum_sst_offset = block_start + block_data.len() - 4;
                let (_, pf_cs, off_cs) =
                    offset_to_part_info(checksum_sst_offset, part_size, &part_size_name);
                eprintln!(
                    "  Block {:4}: offset={}..{} ({} bytes) first_key={:?} -- CORRUPTED",
                    block_idx, block_start, block_end, block_size, key_preview
                );
                eprintln!("    {}", e);
                eprintln!(
                    "    Block starts in file: {} at byte {} within that part",
                    pf_start, off_start
                );
                eprintln!(
                    "    Checksum bytes in file: {} at byte {} within that part",
                    pf_cs, off_cs
                );
            }
        }
    }
    println!();

    // --- Summary ---
    println!("=== Validation Summary ===");
    if corruption_count == 0 {
        println!(
            "  All {} data blocks passed checksum validation.",
            index.len()
        );
    } else {
        eprintln!(
            "  {} of {} data blocks FAILED checksum validation.",
            corruption_count,
            index.len()
        );
    }

    // Try to read/decode rows from blocks to identify decode failures beyond checksum
    if corruption_count > 0 {
        println!();
        println!("=== Per-Block Decode Attempt ===");
        for block_idx in 0..index.len() {
            match sst_file.read_block(block_idx).await {
                Ok(rows) => {
                    println!("  Block {:4}: decode OK ({} rows)", block_idx, rows.len());
                }
                Err(e) => {
                    let block_start = index[block_idx].0 as usize;
                    let block_end = if block_idx + 1 < index.len() {
                        index[block_idx + 1].0 as usize
                    } else {
                        info.filter_offset as usize
                    };
                    let (_, pf, off_in_part) =
                        offset_to_part_info(block_start, part_size, &part_size_name);
                    eprintln!(
                        "  Block {:4}: DECODE FAILED at SST offset {}..{} (in file {} at byte {}) -- {}",
                        block_idx, block_start, block_end, pf, off_in_part, e
                    );
                }
            }
        }
    }

    Ok(())
}

/// List directory contents for error messages.
fn list_dir_contents(dir: &std::path::Path) -> String {
    match std::fs::read_dir(dir) {
        Ok(entries) => {
            let mut names: Vec<String> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            names.sort();
            if names.is_empty() {
                "(empty directory)".to_string()
            } else {
                names.join(", ")
            }
        }
        Err(e) => format!("(cannot list: {})", e),
    }
}
