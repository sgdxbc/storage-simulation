use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng as _, SeedableRng, rngs::StdRng};
use storage_simulation::{VanillaBin, VanillaTrie};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let find_size = 3;
    for num_node in [10_000, 50_000] {
        let mut group = c.benchmark_group(format!("Find3@{}k", num_node / 1000));
        let mut network = VanillaBin::new();
        for _ in 0..num_node {
            network.insert_node(rng.random());
        }
        group.bench_function("VanillaBin", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
        let mut network = VanillaTrie::new();
        for _ in 0..num_node {
            network.insert_node(rng.random());
        }
        group.bench_function("VanillaTrie", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
        network.compress();
        group.bench_function("VanillaTrieCompressed", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
