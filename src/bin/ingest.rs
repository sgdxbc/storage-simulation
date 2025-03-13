#![allow(clippy::too_many_arguments)]
use std::{
    env::args,
    fs::{File, create_dir_all},
    io::Write,
    mem::replace,
    time::{Duration, Instant, UNIX_EPOCH},
};

use rand::{Rng, SeedableRng, rng, rngs::StdRng};
use rand_distr::{Distribution, Zipf};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustc_hash::FxHashMap as HashMap;
use storage_simulation::{Classified, Overlay, VanillaBin};

type DataId = u64;
type NodeId = u64;

fn main() -> anyhow::Result<()> {
    let num_sim = 100;
    let num_node: usize = 12_000;
    let num_copy: usize = 3;
    let node_min_capacity = 1 << 10;
    let capacity_multiplier = 1 << 7;

    let mut rng = rng();
    create_dir_all("data/ingest-evict")?;
    create_dir_all("data/ingest-reject")?;
    create_dir_all("data/ingest-capacity")?;

    if args().nth(1).as_deref() == Some("test") {
        return run(
            Sim::Capacity,
            100,
            num_node,
            node_min_capacity,
            capacity_multiplier,
            1.,
            num_copy,
            true,
            true,
            &mut rng,
        );
    }

    run(
        Sim::EvictLoss(10_000_000),
        num_sim,
        num_node,
        node_min_capacity,
        capacity_multiplier,
        1.,
        num_copy,
        false,
        false,
        &mut rng,
    )?;
    // for (classified, two_choices) in [(false, false), (true, true)] {
    //     run(
    //         Sim::Reject(10_000_000),
    //         num_sim,
    //         num_node,
    //         node_min_capacity,
    //         capacity_multiplier,
    //         1.,
    //         num_copy,
    //         classified,
    //         two_choices,
    //         &mut rng,
    //     )?
    // }
    Ok(())
}

#[derive(Debug)]
enum Sim {
    EvictLoss(usize),
    Capacity,
    Reject(usize),
}

fn run(
    program: Sim,
    num_sim: u32,
    num_node: usize,
    node_min_capacity: usize,
    capacity_multiplier: usize,
    capacity_skew: f32,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    mut rng: impl Rng,
) -> anyhow::Result<()> {
    println!(
        "Program {program:?} Capacity Min {node_min_capacity} Multiplier {capacity_multiplier} Skew {capacity_skew} Classified={classified} Two choices={two_choices}"
    );
    let tag = UNIX_EPOCH.elapsed().unwrap().as_secs();
    let header = "num_node,node_min_capacity,capacity_multiplier,capacity_skew,num_copy,strategy";
    let prefix = format!(
        "{num_node},{node_min_capacity},{capacity_multiplier},{capacity_skew},{num_copy},{}",
        match (classified, two_choices) {
            (false, false) => "Vanilla",
            (true, false) => "Classified",
            (false, true) => "TwoChoices",
            (true, true) => "Classified+TwoChoices",
        },
    );

    let node_capacity_variance_distr = Zipf::new(
        ((capacity_multiplier - 1) * node_min_capacity) as f32,
        capacity_skew,
    )?;
    let start = Instant::now();

    let rngs = (0..num_sim)
        .map(|i| {
            (
                move |s: String| {
                    eprint!(
                        "\r[{:10.3?}] [{i:03}/{num_sim:03}] {s:120}",
                        start.elapsed()
                    )
                },
                StdRng::from_rng(&mut rng),
            )
        })
        .collect::<Vec<_>>()
        .into_par_iter();
    match program {
        Sim::EvictLoss(num_attempt) => {
            let results = rngs
                .map(|(report, rng)| {
                    sim_evict_loss(
                        num_node,
                        num_attempt,
                        num_copy,
                        classified,
                        two_choices,
                        node_min_capacity,
                        node_capacity_variance_distr,
                        rng,
                        report,
                    )
                })
                .collect::<Vec<_>>();
            eprintln!();
            let mut output = File::create(format!("data/ingest-evict/{tag}-sys.csv"))?;
            writeln!(&mut output, "{header},supply,num_stored,num_evict,num_loss")?;
            for lines in results {
                for line in lines {
                    writeln!(&mut output, "{prefix},{line}")?
                }
            }
        }
        Sim::Reject(num_attempt) => {
            let results = rngs
                .map(|(report, rng)| {
                    sim_reject(
                        num_node,
                        num_attempt,
                        num_copy,
                        classified,
                        two_choices,
                        node_min_capacity,
                        node_capacity_variance_distr,
                        rng,
                        report,
                    )
                })
                .collect::<Vec<_>>();
            eprintln!();
            let mut output = File::create(format!("data/ingest-reject/{tag}.csv"))?;
            writeln!(
                &mut output,
                "{header},supply,num_attempt,store_rate,num_stored"
            )?;
            for lines in results {
                for line in lines {
                    writeln!(&mut output, "{prefix},{line}")?
                }
            }
        }
        Sim::Capacity => {
            let results = rngs
                .map(|(report, rng)| {
                    sim_capacity(
                        num_node,
                        num_copy,
                        classified,
                        two_choices,
                        node_min_capacity,
                        node_capacity_variance_distr,
                        rng,
                        report,
                    )
                })
                .collect::<Vec<_>>();
            eprintln!();
            let mut evict_output = File::create(format!("data/ingest-capacity/{tag}-evict.csv"))?;
            writeln!(
                &mut evict_output,
                "{header},supply,capacity,available_capacity,num_bin_node"
            )?;
            let mut loss_output = File::create(format!("data/ingest-capacity/{tag}-loss.csv"))?;
            writeln!(
                &mut loss_output,
                "{header},supply,capacity,available_capacity,num_bin_node"
            )?;
            for (evict_lines, loss_lines) in results {
                for line in evict_lines {
                    writeln!(&mut evict_output, "{prefix},{line}")?
                }
                for line in loss_lines {
                    writeln!(&mut loss_output, "{prefix},{line}")?
                }
            }
        }
    }
    Ok(())
}

struct Node {
    capacity: usize,
    data: Vec<DataId>,
}

impl Node {
    fn with_random_capacity(
        node_min_capacity: usize,
        node_capacity_variance_distr: Zipf<f32>,
        rng: &mut impl Rng,
    ) -> Self {
        Self {
            capacity: node_min_capacity + node_capacity_variance_distr.sample(rng) as usize - 1,
            data: Default::default(),
        }
    }

    fn available_capacity(&self) -> usize {
        self.capacity - self.data.len()
    }
}

fn sim_evict_loss(
    num_node: usize,
    num_attempt: usize,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    node_min_capacity: usize,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: StdRng,
    report: impl Fn(String),
) -> Vec<String> {
    let mut network = Network::random(
        num_node,
        node_min_capacity,
        classified,
        node_capacity_variance_distr,
        &mut rng,
    );

    let mut num_evict = 0;
    let mut num_loss = 0;
    let mut copy_counts = HashMap::default();
    let mut lines = Vec::new();
    let mut last_report = Instant::now();
    for num_storing in 0..num_attempt {
        let (data_id, node_ids) = create_workload(
            num_copy,
            two_choices,
            &mut rng,
            &network.overlay,
            &network.nodes,
        );
        let n = ingest_with_eviction(
            &mut rng,
            &mut network.nodes,
            &mut copy_counts,
            data_id,
            node_ids,
        );
        num_evict += n.0;
        num_loss += n.1;
        if last_report.elapsed() >= Duration::from_secs(1) {
            report(format!("Stored {} Loss {num_loss}", num_storing + 1));
            last_report = Instant::now()
        }
        if (num_storing + 1) % (num_attempt / 100) == 0 {
            lines.push(format!(
                "{},{},{num_evict},{num_loss}",
                network.supply,
                num_storing + 1
            ))
        }
    }
    lines
}

fn sim_capacity(
    num_node: usize,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    node_min_capacity: usize,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: StdRng,
    report: impl Fn(String),
) -> (Vec<String>, Vec<String>) {
    let mut network = Network::random(
        num_node,
        node_min_capacity,
        classified,
        node_capacity_variance_distr,
        &mut rng,
    );
    let mut copy_counts = HashMap::default();
    let mut last_report = Instant::now();
    let mut num_stored = 0;
    loop {
        let (data_id, node_ids) = create_workload(
            num_copy,
            two_choices,
            &mut rng,
            &network.overlay,
            &network.nodes,
        );
        let (num_evicted, _) = ingest_with_eviction(
            &mut rng,
            &mut network.nodes,
            &mut copy_counts,
            data_id,
            node_ids,
        );
        if num_evicted != 0 {
            break;
        }
        num_stored += 1;
        if last_report.elapsed() >= Duration::from_secs(1) {
            report(format!("Stored {num_stored}"));
            last_report = Instant::now()
        }
    }
    let evict_lines = bin_capacity(&network);
    loop {
        let (data_id, node_ids) = create_workload(
            num_copy,
            two_choices,
            &mut rng,
            &network.overlay,
            &network.nodes,
        );
        let (_, num_loss) = ingest_with_eviction(
            &mut rng,
            &mut network.nodes,
            &mut copy_counts,
            data_id,
            node_ids,
        );
        if num_loss != 0 {
            break;
        }
        num_stored += 1;
        if last_report.elapsed() >= Duration::from_secs(1) {
            report(format!("Stored {num_stored}"));
            last_report = Instant::now()
        }
    }
    let loss_lines = bin_capacity(&network);
    (evict_lines, loss_lines)
}

fn bin_capacity(network: &Network) -> Vec<String> {
    let mut matrix = HashMap::<_, u32>::default();
    for node in network.nodes.values() {
        let capacity = ((node.capacity as f32 + 1.).log2() * 100.).round() as usize;
        let available_capacity =
            ((node.available_capacity() as f32 + 1.).log2() * 100.).round() as usize;
        *matrix.entry((capacity, available_capacity)).or_default() += 1
    }
    let mut evict_lines = Vec::new();
    for ((capacity, available_capacity), num_node) in matrix {
        evict_lines.push(format!(
            "{},{},{},{num_node}",
            network.supply,
            capacity as f32 / 100.,
            available_capacity as f32 / 100.,
        ))
    }
    evict_lines
}

fn sim_reject(
    num_node: usize,
    num_attempt: usize,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    node_min_capacity: usize,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: StdRng,
    report: impl Fn(String),
) -> Vec<String> {
    let mut network = Network::random(
        num_node,
        node_min_capacity,
        classified,
        node_capacity_variance_distr,
        &mut rng,
    );
    let mut num_stored_diff = 0;
    let mut num_stored = 0;
    let mut lines = Vec::new();
    let mut last_report = Instant::now();
    for n in 0..num_attempt {
        let (data_id, node_ids) = create_workload(
            num_copy,
            two_choices,
            &mut rng,
            &network.overlay,
            &network.nodes,
        );
        if ingest_with_rejection(&mut network.nodes, data_id, &node_ids) {
            num_stored_diff += 1
        }
        if last_report.elapsed() >= Duration::from_secs(1) {
            report(format!(
                "{} attempts, {} stored",
                n + 1,
                num_stored + num_stored_diff
            ));
            last_report = Instant::now()
        }
        if (n + 1) % (num_attempt / 100) == 0 {
            num_stored += num_stored_diff;
            lines.push(format!(
                "{},{},{},{num_stored}",
                network.supply,
                n + 1,
                num_stored_diff as f32 / (num_attempt / 100) as f32
            ));
            num_stored_diff = 0
        }
    }
    lines
}

fn create_workload(
    num_copy: usize,
    two_choices: bool,
    rng: &mut StdRng,
    network: &Overlay,
    nodes: &std::collections::HashMap<u64, Node, rustc_hash::FxBuildHasher>,
) -> (u64, Vec<u64>) {
    let data_id;
    let node_ids;
    if !two_choices {
        data_id = rng.random();
        node_ids = network.find(data_id, num_copy);
    } else {
        let data_id0 = rng.random();
        let node_ids0 = network.find(data_id0, num_copy);
        let score0 = node_ids0
            .iter()
            .map(|id| nodes[id].available_capacity())
            .min();
        let data_id1 = rng.random();
        let node_ids1 = network.find(data_id1, num_copy);
        let score1 = node_ids1
            .iter()
            .map(|id| nodes[id].available_capacity())
            .min();
        (data_id, node_ids) = if score0 > score1 {
            (data_id0, node_ids0)
        } else {
            (data_id1, node_ids1)
        }
    }
    (data_id, node_ids)
}

struct Network {
    overlay: Overlay,
    nodes: HashMap<NodeId, Node>,
    supply: usize,
}

impl Network {
    fn random(
        num_node: usize,
        node_min_capacity: usize,
        classified: bool,
        node_capacity_variance_distr: Zipf<f32>,
        mut rng: impl Rng,
    ) -> Self {
        let mut overlay = if classified {
            Overlay::Classified(Classified::new())
        } else {
            Overlay::Vanilla(VanillaBin::new())
        };
        let mut nodes = HashMap::default();
        // let mut data_placements = HashMap::<_, Vec<_>>::default();
        let mut supply = 0;
        for _ in 0..num_node {
            let node_id = rng.random();
            let node = Node::with_random_capacity(
                node_min_capacity,
                node_capacity_variance_distr,
                &mut rng,
            );
            supply += node.capacity;
            match &mut overlay {
                Overlay::Vanilla(network) => network.insert_node(node_id),
                Overlay::Classified(network) => network.insert_node(
                    node_id,
                    // (node.capacity as f32 / node_min_capacity as f32)
                    //     .log2()
                    //     .round() as _,
                    (node.capacity as f32).log2().round() as _,
                ),
            }
            let replaced = nodes.insert(node_id, node);
            assert!(replaced.is_none(), "duplicated node {node_id:016x}")
        }
        // println!("Total capacity {total_capacity}");
        if let Overlay::Classified(network) = &mut overlay {
            network.optimize()
        }
        Self {
            overlay,
            nodes,
            supply,
        }
    }
}

fn ingest_with_eviction(
    mut rng: impl Rng,
    nodes: &mut HashMap<NodeId, Node>,
    // data_placements: &mut HashMap<u64, Vec<u64>>,
    copy_counts: &mut HashMap<DataId, u8>,
    data_id: DataId,
    node_ids: Vec<NodeId>,
) -> (usize, usize) {
    let mut num_evict = 0;
    let mut num_loss = 0;
    for &node_id in &node_ids {
        let node = nodes.get_mut(&node_id).unwrap();
        if node.data.len() < node.capacity {
            node.data.push(data_id)
        } else {
            num_evict += 1;
            let evicted = replace(&mut node.data[rng.random_range(0..node.capacity)], data_id);
            // let evicted_placement = data_placements.get_mut(&evicted).unwrap();
            // evicted_placement.remove(
            //     evicted_placement
            //         .iter()
            //         .position(|&id| id == node_id)
            //         .unwrap(),
            // );
            // println!("Evicted {node_id:016x} Left {evicted_placement:016x?}");
            // if evicted_placement.is_empty() {
            //     return false;
            // }
            let evicted_count = copy_counts.get_mut(&evicted).unwrap();
            *evicted_count -= 1;
            if *evicted_count == 0 {
                num_loss += 1
            }
        }
    }
    // let replaced = data_placements.insert(data_id, node_ids);
    let replaced = copy_counts.insert(data_id, node_ids.len() as _);
    assert!(replaced.is_none(), "duplicated data {data_id:016x}");
    (num_evict, num_loss)
}

fn ingest_with_rejection(
    nodes: &mut HashMap<NodeId, Node>,
    data_id: DataId,
    node_ids: &[NodeId],
) -> bool {
    if node_ids
        .iter()
        .any(|node_id| nodes[node_id].data.len() == nodes[node_id].capacity)
    {
        return false;
    }
    for &node_id in node_ids {
        nodes.get_mut(&node_id).unwrap().data.push(data_id)
    }
    true
}
