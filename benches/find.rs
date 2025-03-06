use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{Rng as _, SeedableRng, rngs::StdRng};
use storage_simulation::{Class, Classified, DataId, VanillaBin, VanillaTrie};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = StdRng::seed_from_u64(42);
    let find_size = 3;
    for num_node in [10_000, 50_000] {
        let mut group = c.benchmark_group(format!("Find{find_size}@{}k", num_node / 1000));
        let mut network = VanillaBin::new();
        for _ in 0..num_node {
            network.insert_node(rng.random())
        }
        group.bench_function("VanillaBin", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
        let mut network = VanillaTrie::new();
        for _ in 0..num_node {
            network.insert_node(rng.random())
        }
        group.bench_function("VanillaTrie", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
        network.compress();
        group.bench_function("VanillaTrieCompressed", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
        let mut network = Classified::new();
        for _ in 0..num_node {
            network.insert_node(
                rng.random(),
                8 - (rng.random_range(1.0f32..256.).log2().floor() as Class + 1),
            )
        }
        network.optimize();
        group.bench_function("Classified@8", |b| {
            b.iter(|| network.find(rng.random(), find_size))
        });
    }

    let mut group = c.benchmark_group(format!("Find{find_size}@Small"));
    for num_node in (0..=10).map(|k| 1 << k) {
        let mut network = VanillaBin::new();
        let mut network_trie = VanillaTrie::new();
        let mut network_naive = Vec::new();
        for _ in 0..num_node {
            let node_id = rng.random();
            network.insert_node(node_id);
            network_trie.insert_node(node_id);
            network_naive.push(node_id)
        }
        network_trie.compress();
        if num_node >= 64 {
            group.bench_function(BenchmarkId::new("VanillaBin", num_node), |b| {
                b.iter(|| network.find(rng.random(), find_size))
            });
        }
        group.bench_function(BenchmarkId::new("VanillaTrie", num_node), |b| {
            b.iter(|| network_trie.find(rng.random(), find_size))
        });
        if num_node <= 128 {
            group.bench_function(BenchmarkId::new("VanillaNaive", num_node), |b| {
                b.iter(|| {
                    let data_id = rng.random::<DataId>();
                    network_naive.sort_unstable_by_key(|id| id ^ data_id);
                    network_naive
                        .iter()
                        .take(find_size)
                        .cloned()
                        .collect::<Vec<_>>()
                })
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
