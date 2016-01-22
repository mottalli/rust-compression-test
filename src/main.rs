extern crate rand;
extern crate snappy;
extern crate time;

use std::slice;
use std::mem;
use std::io;
use std::io::{Read, Write};
use std::fs;
use std::cmp;
use rand::Rng;

// -----------------------------------------------------------------------------------------------
trait Nullable {
    fn null_value() -> Self;
}

impl Nullable for i32 {
    fn null_value() -> i32 { std::i32::MIN }
}

impl Nullable for i64 {
    fn null_value() -> i64 { std::i64::MIN }
}

// -----------------------------------------------------------------------------------------------
trait RandomGenerator<T> {
    fn generate_next(&mut self) -> T;
}

impl RandomGenerator<i32> for rand::ThreadRng {
    fn generate_next(&mut self) -> i32 {
        (self.next_u32() as i32) % 1_000
    }
}

impl RandomGenerator<i64> for rand::ThreadRng {
    fn generate_next(&mut self) -> i64 {
        (self.next_u64() as i64) % 1_000_000
    }
}

// -----------------------------------------------------------------------------------------------
trait Compressor {
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8];
    fn decompress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8];
}

// -----------------------------------------------------------------------------------------------
struct NoCompression; 

impl NoCompression {
    fn new() -> NoCompression {
        NoCompression 
    }
}

impl Compressor for NoCompression {
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8] {
        data
    }

    fn decompress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8] {
        data
    }
}

// -----------------------------------------------------------------------------------------------
struct SnappyCompressor {
    buffer: Vec<u8>
}

impl SnappyCompressor {
    fn new() -> SnappyCompressor {
        SnappyCompressor {
            buffer: Vec::new()
        }
    }
}

impl Compressor for SnappyCompressor {
    fn compress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8] {
        self.buffer = snappy::compress(data);
        &self.buffer[..]
    }

    fn decompress<'a>(&'a mut self, data: &'a [u8]) -> &'a [u8] {
        self.buffer = snappy::uncompress(data).unwrap();
        &self.buffer[..]
    }
}

// -----------------------------------------------------------------------------------------------
struct BlockCompressor<C> 
    where C: Compressor
{
    compressor: C,
    block_size: usize
}

impl<C> BlockCompressor<C>
    where C: Compressor
{
    fn new(compressor: C, block_size: usize) -> BlockCompressor<C> {
        BlockCompressor {
            compressor: compressor,
            block_size: block_size
        }
    }

    fn compress(&mut self, data: &[u8], dest: &mut io::Write) {
        let mut lower_limit: usize = 0;
        let num_bytes = data.len();

        while lower_limit < num_bytes {
            let upper_limit = cmp::min(lower_limit+self.block_size, num_bytes);
            let chunk = &data[lower_limit..upper_limit];
            let compressed_chunk = self.compressor.compress(chunk);
            dest.write(compressed_chunk.len().to_raw_bytes()).unwrap();
            dest.write(compressed_chunk).unwrap();

            lower_limit = upper_limit;
        }
    }

    fn get_block_decompressor<'a, 'b>(&'a mut self, reader: &'b mut Read) -> BlockDecompressor<'a, 'b, C> {
        BlockDecompressor::new(&mut self.compressor, reader)
    }
}

// -----------------------------------------------------------------------------------------------
struct BlockDecompressor<'a, 'b, C> 
    where C: Compressor + 'a
{
    compressor: &'a mut C,
    reader: &'b mut Read,
    buffer: Vec<u8>
}

impl<'a, 'b, C> BlockDecompressor<'a, 'b, C>
    where C: Compressor + 'a
{
    fn new(compressor: &'a mut C, reader: &'b mut Read) -> BlockDecompressor<'a, 'b, C> {
        BlockDecompressor {
            compressor: compressor,
            reader: reader,
            buffer: Vec::new()
        }
    }

    fn next_block<'c>(&'c mut self) -> Option<&'c [u8]> {
        // Get the number of bytes
        let mut chunk_size: usize = 0;
        match self.reader.read(chunk_size.to_raw_bytes_mut()) {
            Err(_) => return None,
            Ok(0) => return None,
            _ => {}
        }

        self.buffer.resize(chunk_size, 0);
        self.reader.read(&mut self.buffer).unwrap();
        let decompressed_data = self.compressor.decompress(&self.buffer);
        Some(decompressed_data)
    }
}

// -----------------------------------------------------------------------------------------------
fn generate_random_vector<T, R>(size: usize, rng: &mut R, null_probabilty: f32) -> Vec<T>
    where T: Nullable,
          R: RandomGenerator<T> + rand::Rng
{
    let mut random_vector = Vec::with_capacity(size);
    for _ in 0..size {
        let value = if rng.next_f32() < null_probabilty {
            T::null_value()
        } else {
            rng.generate_next()
        };
        random_vector.push(value);
    }

    random_vector
}

// -----------------------------------------------------------------------------------------------
trait ToRawBytes {
    fn to_raw_bytes(&self) -> &[u8];
    fn to_raw_bytes_mut(&mut self) -> &mut [u8];
}

impl<T> ToRawBytes for Vec<T>
    where T: Sized
{
    fn to_raw_bytes(&self) -> &[u8] {
        let ptr = self.as_ptr() as *const u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts(ptr, size) }
    }

    fn to_raw_bytes_mut(&mut self) -> &mut [u8] {
        unimplemented!();
    }
}

impl ToRawBytes for usize
{
    fn to_raw_bytes(&self) -> &[u8] {
        let ptr = (self as *const Self) as *const u8;
        let size = mem::size_of::<Self>();
        unsafe { slice::from_raw_parts(ptr, size) }
    }

    fn to_raw_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = (self as *mut Self) as *mut u8;
        let size = mem::size_of::<Self>();
        unsafe { slice::from_raw_parts_mut(ptr, size) }
    }
}

// -----------------------------------------------------------------------------------------------
fn drop_caches() {
    fs::OpenOptions::new()
        .write(true)
        .create(false)
        .open("/proc/sys/vm/drop_caches")
        .and_then(|mut f| f.write(b"3"))
        .unwrap();
}
// -----------------------------------------------------------------------------------------------
fn do_test<C: Compressor>(values: &Vec<i32>, compressor: C) {
    let file_name = "/tmp/data.bin";

    let mut block_compressor = BlockCompressor::new(compressor, 256*1024);
    {
        let mut file = fs::File::create(file_name).unwrap();
        block_compressor.compress(values.to_raw_bytes(), &mut file);
    }


    let n = 20;
    let times = (0..n).map(|_| {
        let tic = time::now();

        drop_caches();
        let mut file = fs::File::open(file_name).unwrap();
        let mut block_decompressor = block_compressor.get_block_decompressor(&mut file);

        let mut sum: i64 = 0;
        while let Some(data) = block_decompressor.next_block() {
            let values = unsafe { slice::from_raw_parts(data.as_ptr() as *const i32, data.len() / mem::size_of::<i32>()) };
            sum += values.iter().filter(|&v| *v != i32::null_value()).fold(0i64, |ac, &v| ac + (v as i64));
            /*{
                let n = values.len();
                for i in 0..n {
                    let value = values[i];
                    if value == i32::null_value() {
                        continue;
                    }

                    sum += value as i64;
                }
            }*/
        }

        let toc = time::now();
        (toc-tic).num_milliseconds()
    }).collect::<Vec<_>>();

    let avg_time = times.iter().fold(0i64, |accum, &v| accum + v) / (times.len() as i64);
    println!("Avg. time: {} ms.", avg_time);
}


// -----------------------------------------------------------------------------------------------
fn main() {
    let size = 10_000_000;

    let mut rng = rand::thread_rng();
    println!("Generating {} random values...", size);
    let values = generate_random_vector::<i32, _>(size, &mut rng, 0.9f32);

    let sum = values.iter().filter(|&v| *v != i32::null_value()).fold(0i64, |ac, &v| ac + (v as i64));
    println!("Sum is {}", sum);

    println!("Benchmarking with no compression...");
    do_test(&values, NoCompression::new());
    println!("Benchmarking with Snappy...");
    do_test(&values, SnappyCompressor::new());

}
