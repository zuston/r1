use std::fs;
use bytes::{Buf, Bytes};
use clap::{App, Arg};
use uniffle_worker::util::get_crc;

fn main() -> anyhow::Result<()> {
    let cli = App::new("r1_tool")
        .arg(Arg::with_name("index_file_path").takes_value(true))
        .arg(Arg::with_name("data_file_path").takes_value(true))
        .get_matches();

    let index_path = cli.value_of("index_file_path").unwrap();
    let data_path = cli.value_of("data_file_path").unwrap();

    let index_data = fs::read(index_path)?;
    let mut index_data = Bytes::copy_from_slice(&index_data);

    let data = fs::read(data_path)?;
    let data = Bytes::copy_from_slice(&data);

    // check the length
    let mut index_clone = index_data.clone();
    let mut expected_len = 0;
    let batch = index_clone.len() / 40;
    for idx in 0..batch {
        let offset = index_clone.get_i64();
        let length = index_clone.get_i32();
        let uncompress_len = index_clone.get_i32();
        let crc = index_clone.get_i64();
        let block_id = index_clone.get_i64();
        let task_id = index_clone.get_i64();

        expected_len += length as usize;
    }

    if expected_len != data.len() {
        println!("index recorded data len: {}. real: {}", expected_len, data.len());
        return Ok(());
    }

    for idx in 0..batch {
        let offset = index_data.get_i64();
        let length = index_data.get_i32();
        let uncompress_len = index_data.get_i32();
        let crc = index_data.get_i64();
        let block_id = index_data.get_i64();
        let task_id = index_data.get_i64();

        let partial = data.slice((offset as usize..(offset + (length as i64)) as usize));
        let data_crc = get_crc(&partial);
        if crc != data_crc {
            println!("blockId: {}, crc is not correct. real: {}, expected: {}", block_id, crc, data_crc);
            break;
        }
    }

    Ok(())
}