extern crate rand;

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
    fn compress<'a>(&mut self, data: &'a [u8]) -> &'a [u8];
    fn decompress<'a>(&mut self, data: &'a [u8]) -> &'a [u8];
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
fn main() {
    let size = 1_000_000;

    let mut rng = rand::thread_rng();
    println!("Generating {} random values...", size);
    let values = generate_random_vector::<i32, _>(size, &mut rng, 0.9f32);
}
