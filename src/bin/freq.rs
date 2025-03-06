use std::{
    collections::HashMap,
    fs::{File, create_dir_all},
    io::Write,
    iter::repeat_with,
    time::UNIX_EPOCH,
};

use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng, rng, rngs::StdRng};
use rand_distr::{Distribution, Zipf};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use storage_simulation::{Classified, Network, VanillaBin};

fn main() -> anyhow::Result<()> {
    let num_find: u32 = 1_000_000;
    let num_node = 10_000;
    let find_size: usize = 3;

    let mut rng = rng();
    create_dir_all("data/freq")?;
    for classified in [false, true] {
        run(
            100,
            classified,
            num_node,
            num_find,
            find_size,
            8,
            1.,
            StdRng::from_rng(&mut rng),
        )?
    }

    Ok(())
}

fn run(
    num_sample: usize,
    classified: bool,
    num_node: usize,
    num_find: u32,
    find_size: usize,
    num_class: u8,
    skew: f32,
    mut rng: impl Rng,
) -> anyhow::Result<()> {
    eprintln!("Number of node {num_node} Number of class {num_class} Skew {skew}");

    let tag = UNIX_EPOCH.elapsed().unwrap().as_secs();
    let mut node_output = File::create(format!("data/freq/{tag}-node.csv"))?;
    let mut capacity_output = File::create(format!("data/freq/{tag}-capacity.csv"))?;
    let mut class_output = File::create(format!("data/freq/{tag}-class.csv"))?;
    let header = "strategy,num_node,num_find,find_size,num_class,skew";
    writeln!(node_output, "{header},freq,quantile")?;
    writeln!(capacity_output, "{header},freq,quantile")?;
    writeln!(
        class_output,
        "{header},class,num_class_node,class_capacity,class_hit_count"
    )?;
    let prefix = format!(
        "{},{num_node},{num_find},{find_size},{num_class},{skew}",
        if classified { "Classified" } else { "Vanilla" }
    );

    let capacity_distr = Zipf::new(((1usize << num_class) - 1) as f32, skew)?;
    #[derive(Default, Clone)]
    struct Class {
        num_node: u64,
        capacity: u64,
        hit_count: u64,
    }
    let (node_counts, capacity_counts, classes) = repeat_with(|| StdRng::from_rng(&mut rng))
        .take(num_sample)
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(move |mut rng| {
            let mut network = if classified {
                Network::Classified(Classified::new())
            } else {
                Network::Vanilla(VanillaBin::new())
            };

            struct Node {
                capacity: u64,
                hit_count: u64,
            }
            let mut nodes = HashMap::new();
            fn node_class(capacity: u64) -> u8 {
                (capacity as f32).log2().floor() as _
            }
            // let mut total_capacity = 0;
            for _ in 0..num_node {
                let node_id = rng.random();
                let capacity = capacity_distr.sample(&mut rng) as _;
                // total_capacity += capacity;
                nodes.insert(
                    node_id,
                    Node {
                        capacity,
                        hit_count: 0,
                    },
                );
                match &mut network {
                    Network::Vanilla(network) => network.insert_node(node_id),
                    Network::Classified(network) => {
                        network.insert_node(node_id, node_class(capacity))
                    }
                }
            }
            for _ in 0..num_find {
                let node_ids = network.find(rng.random(), find_size);
                for node_id in node_ids {
                    nodes.get_mut(&node_id).unwrap().hit_count += 1
                }
            }

            let mut node_counts = Histogram::<u32>::new(1).unwrap();
            let mut capacity_counts = Histogram::<u32>::new(1).unwrap();
            let mut classes = vec![Class::default(); num_class as _];
            for node in nodes.values() {
                node_counts.record(node.hit_count).unwrap();
                capacity_counts
                    .record_n(
                        node.hit_count * 1_000_000 / node.capacity,
                        node.capacity as _,
                    )
                    .unwrap();
                let class = &mut classes[node_class(node.capacity) as usize];
                class.num_node += 1;
                class.capacity += node.capacity;
                class.hit_count += node.hit_count
            }
            (node_counts, capacity_counts, classes)
        })
        .reduce(
            || {
                (
                    Histogram::<u32>::new(1).unwrap(),
                    Histogram::<u32>::new(1).unwrap(),
                    vec![Class::default(); num_class as _],
                )
            },
            |(a1, b1, c1), (a2, b2, c2)| {
                (
                    a1 + a2,
                    b1 + b2,
                    c1.into_iter()
                        .zip(c2)
                        .map(|(n1, n2)| Class {
                            num_node: n1.num_node + n2.num_node,
                            capacity: n1.capacity + n2.capacity,
                            hit_count: n1.hit_count + n2.hit_count,
                        })
                        .collect(),
                )
            },
        );
    eprintln!();

    for value in node_counts.iter_recorded() {
        writeln!(
            &mut node_output,
            "{prefix},{},{}",
            value.value_iterated_to() as f32 / (num_find * find_size as u32) as f32,
            value.quantile()
        )?
    }
    for value in capacity_counts.iter_recorded() {
        writeln!(
            &mut capacity_output,
            "{prefix},{},{}",
            value.value_iterated_to() as f32 / (num_find * find_size as u32) as f32 / 1_000_000.,
            value.quantile()
        )?
    }
    for (class, stats) in classes.into_iter().enumerate() {
        writeln!(
            &mut class_output,
            "{prefix},{class},{},{},{}",
            stats.num_node, stats.capacity, stats.hit_count
        )?
    }
    Ok(())
}
