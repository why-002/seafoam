use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
};

use super::LogEntry;

// this will need to be able to write an index file of usizes and a data file of bytes. Then it will handle offsetting.

// Probably should create a new Log class, which can then be used seamlessy across the app. OR it might be better to just write some functions that take a vec as input

fn get_logs_between_indices(
    log: Vec<LogEntry>,
    start_index: usize,
    end_index: usize,
) -> Result<Vec<LogEntry>, Box<dyn std::error::Error>> {
    if start_index > end_index {
        return Err("Start index must be less than end index".into());
    }
    let lowest_index = log.first().unwrap().get_index() as u64;
    if start_index >= lowest_index as usize {
        return Ok(
            log[lowest_index as usize - start_index..=end_index - start_index as usize].to_vec(),
        );
    }

    let mut index_file = File::open("LogIndex")?;
    let mut data_file = File::open("ServerLog")?;

    // Seek to the start position
    index_file.seek(SeekFrom::Start((start_index - 1) as u64 * 64))?;

    // Create a reader with a fixed length
    let mut chunk = index_file.try_clone().unwrap().take((1) as u64 * 64);
    let mut buf = [0, 0, 0, 0, 0, 0, 0, 0];
    chunk.read_exact(&mut buf)?;
    let index = u64::from_be_bytes(buf);
    chunk = index_file.take(1 * 64);
    chunk.read_exact(&mut buf)?;
    let end_index = u64::from_be_bytes(buf);

    data_file.seek(SeekFrom::Start(index))?;
    chunk = data_file.take(end_index - index);
    let mut data = Vec::new();
    chunk.read_to_end(&mut data);
    let log_entry = serde_json::from_slice(&data).unwrap();
    match log_entry {
        LogEntry::Cas {
            key,
            old_value,
            new_value,
            index,
            term,
        } => panic!(),
        _ => (),
    }
    Ok((Vec::new()))
}
