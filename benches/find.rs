use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng as _, SeedableRng, rngs::StdRng};
use storage_simulation::{Vanilla, VanillaTrie};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let mut group = c.benchmark_group("Find3@10k");
    let find_size = 3;
    let num_node = 10_000;
    let mut network = Vanilla::new();
    for _ in 0..num_node {
        network.insert_node(rng.random());
    }
    group.bench_function("Vanilla", |b| {
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
