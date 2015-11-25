extern crate rand;
extern crate snappy;
extern crate time;
extern crate mmap;
extern crate lz4;

use rand::Rng;
use std::slice;
use std::fs;
use std::io::Write;
use std::io::Read;
use std::os::unix::io::AsRawFd;
use std::ptr;

// ------------------------------------------------------------------------
trait Encoder {
    fn name(&self) -> String;
    fn encode(&mut self, data: &[u8]) -> Vec<u8>;
    fn decode(&mut self, data: &[u8]) -> Vec<u8>;
}

// ------------------------------------------------------------------------
struct RawEncoder; 

impl RawEncoder 
{
    fn new() -> RawEncoder { RawEncoder }
}

impl Encoder for RawEncoder 
{
    fn name(&self) -> String { "Raw".to_string() }
    fn encode(&mut self, data: &[u8]) -> Vec<u8> { data.to_vec() }
    fn decode(&mut self, data: &[u8]) -> Vec<u8> { data.to_vec() }
}
// ------------------------------------------------------------------------
struct SnappyEncoder; 

impl SnappyEncoder 
{
    fn new() -> SnappyEncoder { SnappyEncoder }
}

impl Encoder for SnappyEncoder 
{
    fn name(&self) -> String { "Snappy".to_string() }
    fn encode(&mut self, data: &[u8]) -> Vec<u8> { snappy::compress(data) }
    fn decode(&mut self, data: &[u8]) -> Vec<u8> { snappy::uncompress(data).unwrap() }
}

// ------------------------------------------------------------------------
struct LZ4Encoder;

impl LZ4Encoder
{
    fn new() -> LZ4Encoder { LZ4Encoder }
}

impl Encoder for LZ4Encoder 
{
    fn name(&self) -> String { "LZ4".to_string() }

    fn encode(&mut self, data: &[u8]) -> Vec<u8> 
    {
        let buffer = Vec::<u8>::new();
        let mut encoder = lz4::EncoderBuilder::new()
//            .block_size(lz4::liblz4::BlockSize::Max256KB)
//            .level(1)
            .build(buffer)
            .ok().expect("Unable to create LZ4 encoder");
        encoder.write_all(data).ok().expect("Unable to compress data with LZ4 encoder");
        let (encoded, result) = encoder.finish();
        result.ok().expect("Unable to finish compressing data with LZ4");
        encoded
    }

    fn decode(&mut self, data: &[u8]) -> Vec<u8>
    {
        let mut decoder: lz4::Decoder<&[u8]> = lz4::Decoder::new(data).ok().expect("Unable to create LZ4 decoder");
        let mut result = Vec::<u8>::new();
        decoder.read_to_end(&mut result).ok().expect("Unable to uncompress data with LZ4 encoder");
        result
    }
}

// ------------------------------------------------------------------------
fn mmap_file(filename: &str) -> Result<mmap::MemoryMap, mmap::MapError>
{
    let file = fs::OpenOptions::new().read(true).open(filename).unwrap();
    let file_size = fs::metadata(filename).unwrap().len() as usize;
    mmap::MemoryMap::new(file_size, &[
                   mmap::MapOption::MapReadable,
                   mmap::MapOption::MapFd(file.as_raw_fd())
    ])
}

// ------------------------------------------------------------------------
fn benchmark<F, R>(mut f: F) -> R
    where F: FnMut() -> R 
{
    
    let tic = time::now();
    let res = f();
    let toc = time::now();
    
    let delta = toc - tic;
    println!("Elapsed: {} ms.", delta.num_milliseconds());
    res
}

// ------------------------------------------------------------------------
fn write_vector(file: &str, data: &Vec<u8>) 
{
    fs::File::create(file)
        .and_then(|mut f| f.write_all(&data[..]))
        .ok().expect("Could not write file");
}

// ------------------------------------------------------------------------
fn run_test(encoder: &mut Encoder, data: &Vec<u8>)
{
    println!("----- Testing \"{}\" encoder -----", encoder.name());
    println!("Compressing...");
    let compressed_data = benchmark(|| encoder.encode(&data[..]));
    let uncompressed_data_size = data.len();
    let compressed_data_size = compressed_data.len();
    let ratio = (compressed_data_size as f32) / (uncompressed_data_size as f32);
    println!("Uncompressed size: {}, compressed size: {}, ratio: {}", uncompressed_data_size, compressed_data_size, ratio);
    
    println!("Writing compressed vector to disk");

    let compressed_file_name = "/tmp/data.bin";
    benchmark(|| write_vector(compressed_file_name, &compressed_data));

    let compressed_mapped_file = mmap_file(compressed_file_name).unwrap();
    let compressed_mapped_data: &[u8] = unsafe { slice::from_raw_parts(compressed_mapped_file.data(), compressed_data_size) };
    benchmark(|| {
        println!("Uncompressing...");
        let uncompressed_data: Vec<u8> = benchmark(|| { encoder.decode(&compressed_mapped_data) });
        let sum = uncompressed_data.iter().fold(0u64, |acc, &item| acc + (item as u64));
        println!("Compressed sum: {}", sum);
    });
    
    drop(compressed_mapped_file);
}

// ------------------------------------------------------------------------
fn main() 
{
    let n = 1_000_000;
    //let n = 130_000_000;
    let null_probabilty = 0.9f32;

    let mut values: Vec<u8> = Vec::with_capacity(n);

    
    println!("Generating numbers");
    benchmark(|| {
        let mut rng = rand::thread_rng();
        for _ in (0..n) {
            let v = if rng.next_f32() > null_probabilty { rng.gen() } else { 0 };

            values.push(v);
        }
    });

    /*println!("Writing uncompressed vector to disk");
    let uncompressed_file_name = "/tmp/uncompressed.bin";
    benchmark(|| write_vector(uncompressed_file_name, &values));

    let uncompressed_mmapped_file = mmap_file(uncompressed_file_name).unwrap();
    benchmark(|| unsafe {
        let sum = slice::from_raw_parts(uncompressed_mmapped_file.data(), uncompressed_mmapped_file.len()).iter().fold(0u64, |acc, &item| acc + (item as u64));
        println!("Uncompressed sum: {}", sum);
    });*/

    run_test(&mut RawEncoder::new(), &values);
    run_test(&mut SnappyEncoder::new(), &values);
    run_test(&mut LZ4Encoder::new(), &values);
}
