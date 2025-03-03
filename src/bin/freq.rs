use std::{
    collections::HashMap,
    fmt::Write as _,
    fs::{File, create_dir_all},
    io::Write,
    time::UNIX_EPOCH,
};

use rand::{Rng, SeedableRng, rng, rngs::StdRng};
use rand_distr::{Distribution, Zipf};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use storage_simulation::{Classified, Overlay};

fn main() -> anyhow::Result<()> {
    let num_find: u32 = 1_000_000;
    let find_size: usize = 3;

    let mut rng = rng();
    create_dir_all("data/freq")?;
    let mut output = File::create(format!(
        "data/freq/{}.csv",
        UNIX_EPOCH.elapsed().unwrap().as_secs()
    ))?;
    writeln!(
        output,
        "num_node,num_find,find_size,num_class,skew,class,num_class_node,hit_count",
    )?;
    // let num_build = 1_000;
    // for num_node in [10_000, 20_000, 30_000, 40_000, 50_000] {
    //     run(
    //         num_node,
    //         num_build,
    //         num_find,
    //         find_size,
    //         8,
    //         24,
    //         0.,
    //         StdRng::from_rng(&mut rng),
    //         &mut output,
    //     )?
    // }
    let num_node = 10_000;
    run(
        1,
        num_node,
        num_find,
        find_size,
        12,
        1.,
        StdRng::from_rng(&mut rng),
        &mut output,
    )?;

    Ok(())
}

fn run(
    num_build: u32,
    num_node: usize,
    num_find: u32,
    find_size: usize,
    num_class: u8,
    skew: f32,
    mut rng: impl Rng,
    mut output: impl Write,
) -> anyhow::Result<()> {
    eprintln!("Number of node {num_node} Number of class {num_class} Skew {skew}");
    let capacity_distr = Zipf::new((1usize << (num_class - 1)) as f32, skew)?;
    let results = (0..num_build)
        .map(|i| (i, StdRng::from_rng(&mut rng)))
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(move |(build_i, mut rng)| {
            let mut network = Classified::new();
            let mut node_classes = HashMap::new();
            let mut num_class_node = vec![0; num_class as _];
            for _ in 0..num_node {
                let node_id = rng.random();
                let capacity = capacity_distr.sample(&mut rng);
                let class = capacity.log2().floor() as _;
                network.insert_node(node_id, class);
                node_classes.insert(node_id, class);
                num_class_node[class as usize] += 1;
            }
            let mut freqs = vec![0u32; num_class as _];
            for _ in 0..num_find {
                let node_ids = network.find(rng.random(), find_size);
                for node_id in node_ids {
                    freqs[node_classes[&node_id] as usize] += 1
                }
            }
            eprint!("\r{:80}", format!("{build_i}"));
            let mut lines = String::new();
            for class in 0..num_class {
                writeln!(
                    &mut lines,
                    "{num_node},{num_find},{find_size},{num_class},{skew},{class},{},{}",
                    num_class_node[class as usize], freqs[class as usize]
                )
                .unwrap()
            }
            lines
        })
        .collect::<Vec<_>>();
    eprintln!();
    for result in results {
        write!(&mut output, "{result}")?
    }
    Ok(())
}
