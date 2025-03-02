use std::{
    collections::HashMap,
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
    let num_build = 1_000;
    let find_size: usize = 3;

    let mut rng = rng();
    create_dir_all("data/freq")?;
    let mut output = File::create(format!(
        "data/freq/{}.csv",
        UNIX_EPOCH.elapsed().unwrap().as_secs()
    ))?;
    writeln!(
        output,
        "num_node,num_build,build_i,num_find,find_size,class_begin,class_end,skew,class,num_class_node,hit_count",
    )?;
    for num_node in [10_000, 20_000, 30_000, 40_000, 50_000] {
        run(
            num_node,
            num_build,
            num_find,
            find_size,
            8,
            24,
            0.,
            StdRng::from_rng(&mut rng),
            &mut output,
        )?
    }
    Ok(())
}

fn run(
    num_node: usize,
    num_build: u32,
    num_find: u32,
    find_size: usize,
    class_begin: u8,
    class_end: u8,
    skew: f32,
    mut rng: impl Rng,
    mut output: impl Write,
) -> anyhow::Result<()> {
    eprintln!("Number of node {num_node}");
    let class_distr = Zipf::new((class_end - class_begin) as f32, skew)?;
    let results = (0..num_build)
        .map(|i| (i, StdRng::from_rng(&mut rng)))
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(move |(build_i, mut rng)| {
            let mut network = Classified::new();
            let mut node_classes = HashMap::new();
            let mut num_class_node = vec![0; 64];
            for _ in 0..num_node {
                let node_id = rng.random();
                let class = class_end - class_distr.sample(&mut rng) as u8;
                network.insert_node(node_id, class);
                node_classes.insert(node_id, class);
                num_class_node[class as usize] += 1;
            }
            let mut freqs = vec![0u32; 64];
            for _ in 0..num_find {
                let node_ids = network.find(rng.random(), find_size);
                for node_id in node_ids {
                    freqs[node_classes[&node_id] as usize] += 1
                }
            }
            // if last_report.elapsed() >= Duration::from_secs(1) {
            //     println!("{build_i:03}/{num_build:03} Number of node {num_node}");
            //     last_report = Instant::now()
            // }
            // println!(
            //     "{class_begin} {:?}",
            //     &freqs[class_begin as usize..class_end as usize]
            // );
            eprint!("\r{:80}", format!("{build_i}"));
            (build_i, num_class_node, freqs)
        })
        .collect::<Vec<_>>();
    eprintln!();
    for (build_i, num_class_node, freqs) in results {
        for class in class_begin..class_end {
            writeln!(
                output,
                "{num_node},{num_build},{build_i},{num_find},{find_size},{class_begin},{class_end},{skew},{class},{},{}",
                num_class_node[class as usize], freqs[class as usize]
            )?
        }
    }

    Ok(())
}
