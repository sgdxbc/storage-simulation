use std::{
    fs::{File, create_dir_all},
    io::Write,
    mem::replace,
    time::{Duration, Instant, UNIX_EPOCH},
};

use rand::{Rng, SeedableRng, rng, rngs::StdRng};
use rand_distr::{Distribution, Zipf};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustc_hash::FxHashMap as HashMap;
use storage_simulation::{Classified, Network, VanillaBin};

type DataId = u64;
type NodeId = u64;

fn main() -> anyhow::Result<()> {
    let num_node: usize = 12_000;
    let num_copy: usize = 3;

    let mut rng = rng();
    create_dir_all("data/ingest-loss")?;
    create_dir_all("data/ingest-reject")?;

    // for two_choices in [false, true] {
    //     for node_min_capacity in (6..=12).step_by(2).map(|k| 1 << k) {
    //         run(
    //             100,
    //             num_node,
    //             node_min_capacity,
    //             node_min_capacity * 100,
    //             1.5,
    //             num_copy,
    //             false,
    //             two_choices,
    //             &mut rng,
    //         )?
    //     }
    // }

    // let node_min_capacity = 1 << 10;
    // for classified in [false, true] {
    //     for two_choices in [false, true] {
    //         for skew in (10..=20).map(|n| n as f32 / 10.) {
    //             run(
    //                 Sim::Loss,
    //                 100,
    //                 num_node,
    //                 node_min_capacity,
    //                 node_min_capacity * 100,
    //                 skew,
    //                 num_copy,
    //                 classified,
    //                 two_choices,
    //                 &mut rng,
    //             )?
    //         }
    //     }
    // }

    let node_min_capacity = 1 << 10;
    // for classified in [false, true] {
    for classified in [true] {
        for two_choices in [false, true] {
            for skew in (10..=20).map(|n| n as f32 / 10.) {
                run(
                    Sim::Reject,
                    100,
                    num_node,
                    node_min_capacity,
                    node_min_capacity * 100,
                    skew,
                    num_copy,
                    classified,
                    two_choices,
                    &mut rng,
                )?
            }
        }
    }
    // run(
    //     Sim::Reject,
    //     100,
    //     num_node,
    //     node_min_capacity,
    //     node_min_capacity * 100,
    //     1.,
    //     num_copy,
    //     false,
    //     false,
    //     &mut rng,
    // )?;

    Ok(())
}

#[derive(Debug)]
enum Sim {
    Loss,
    Reject,
}

#[allow(clippy::too_many_arguments)]
fn run(
    program: Sim,
    num_sim: u32,
    num_node: usize,
    node_min_capacity: usize,
    node_max_capacity: usize,
    capacity_skew: f32,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    mut rng: impl Rng,
) -> anyhow::Result<()> {
    println!(
        "Program {program:?} Capacity {node_min_capacity}/{node_max_capacity} Skew {capacity_skew} Classified={classified} Two choices={two_choices}"
    );
    let tag = UNIX_EPOCH.elapsed().unwrap().as_secs();
    let header = "num_node,node_min_capacity,node_max_capacity,capacity_skew,num_copy,strategy";
    let prefix = format!(
        "{num_node},{node_min_capacity},{node_max_capacity},{capacity_skew},{num_copy},{}",
        match (classified, two_choices) {
            (false, false) => "Vanilla",
            (true, false) => "Classified",
            (false, true) => "TwoChoices",
            (true, true) => "Classified+TwoChoices",
        },
    );

    let node_capacity_variance_distr = Zipf::new(
        (node_max_capacity - node_min_capacity) as f32,
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
        Sim::Loss => {
            let results = rngs
                .map(|(report, rng)| {
                    sim_loss(
                        num_node,
                        node_min_capacity,
                        num_copy,
                        classified,
                        two_choices,
                        node_capacity_variance_distr,
                        rng,
                        report,
                    )
                })
                .collect::<Vec<_>>();
            eprintln!();
            let mut sys_output = File::create(format!("data/ingest-loss/{tag}-sys.csv"))?;
            writeln!(
                &mut sys_output,
                "{header},total_capacity,num_stored,num_utilized_node,utilized_capacity,redundancy",
            )?;
            let mut bin_output = File::create(format!("data/ingest-loss/{tag}-bin.csv"))?;
            writeln!(
                &mut bin_output,
                "{header},bin_index,num_bin_node,bin_hit_count,bin_capacity,bin_used_capacity,bin_max_utilization"
            )?;
            for (sys_stats, bin_stats) in results {
                writeln!(&mut sys_output, "{prefix},{sys_stats}")?;
                for line in bin_stats {
                    writeln!(&mut bin_output, "{prefix},{line}")?
                }
            }
        }
        Sim::Reject => {
            let results = rngs
                .map(|(report, rng)| {
                    sim_reject(
                        num_node,
                        node_min_capacity,
                        num_copy,
                        classified,
                        two_choices,
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
                "{header},total_capacity,num_attempt,num_stored"
            )?;
            for lines in results {
                for line in lines {
                    writeln!(&mut output, "{prefix},{line}")?
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
    fn score(&self) -> usize {
        self.capacity - self.data.len()
    }
}

#[allow(clippy::too_many_arguments)]
fn sim_loss(
    num_node: usize,
    node_min_capacity: usize,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: StdRng,
    report: impl Fn(String),
) -> (String, Vec<String>) {
    let (network, mut nodes, total_capacity) = create_network(
        num_node,
        node_min_capacity,
        classified,
        node_capacity_variance_distr,
        &mut rng,
    );

    #[derive(Default, Clone)]
    struct NodeBin {
        num_node: usize,
        freq: usize,
        capacity: usize,
        used_capacity: usize,
        max_utilization: f32,
    }
    let mut num_stored = 0;
    let mut copy_counts = HashMap::default();
    let mut bins = vec![NodeBin::default(); 64];
    let mut last_report = Instant::now();
    loop {
        let (data_id, node_ids) =
            create_workload(num_copy, two_choices, &mut rng, &network, &nodes);
        for node_id in &node_ids {
            let node = &nodes[node_id];
            let bin = &mut bins[((node.capacity as f32 / node_min_capacity as f32)
                .log2()
                .floor()) as usize];
            bin.freq += 1
        }
        if !ingest(&mut rng, &mut nodes, &mut copy_counts, data_id, node_ids) {
            break;
        }
        num_stored += 1;
        if last_report.elapsed() >= Duration::from_secs(1) {
            report(format!("Stored {num_stored}"));
            last_report = Instant::now()
        }
    }
    let num_utilized_node = nodes.values().filter(|node| !node.data.is_empty()).count();
    let utilized_capacity = nodes.values().map(|node| node.data.len()).sum::<usize>();
    report(format!(
        "Stored {num_stored} Utilized Node {:.2} Utilized Capacity {:.2} Average Redundancy {:.2}",
        num_utilized_node as f32 / nodes.len() as f32,
        utilized_capacity as f32 / total_capacity as f32,
        utilized_capacity as f32 / num_stored as f32
    ));
    let sys_stats = format!(
        "{total_capacity},{num_stored},{num_utilized_node},{utilized_capacity},{}",
        utilized_capacity as f32 / num_stored as f32,
    );
    for node in nodes.values() {
        let bin = &mut bins[((node.capacity as f32 / node_min_capacity as f32)
            .log2()
            .floor()) as usize];
        bin.num_node += 1;
        bin.capacity += node.capacity;
        bin.used_capacity += node.data.len();
        bin.max_utilization = bin
            .max_utilization
            .max(node.data.len() as f32 / node.capacity as f32)
    }
    let mut bin_stats = Vec::new();
    for (i, bin) in bins.into_iter().enumerate() {
        if bin.num_node != 0 {
            bin_stats.push(format!(
                "{i},{},{},{},{},{}",
                bin.num_node, bin.freq, bin.capacity, bin.used_capacity, bin.max_utilization
            ))
        }
    }
    (sys_stats, bin_stats)
}

fn sim_reject(
    num_node: usize,
    node_min_capacity: usize,
    num_copy: usize,
    classified: bool,
    two_choices: bool,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: StdRng,
    report: impl Fn(String),
) -> Vec<String> {
    let (network, mut nodes, total_capacity) = create_network(
        num_node,
        node_min_capacity,
        classified,
        node_capacity_variance_distr,
        &mut rng,
    );
    let mut max_num_attempt = 0;
    let mut lines = Vec::new();
    'sim: for num_stored in 1..=total_capacity / num_copy / 5 {
        let mut num_attempt = 0;
        while {
            num_attempt += 1;
            let (data_id, node_ids) =
                create_workload(num_copy, two_choices, &mut rng, &network, &nodes);
            !ingest_with_rejection(&mut nodes, data_id, &node_ids)
        } {
            if num_attempt > 100 {
                break 'sim;
            }
        }
        if num_attempt > max_num_attempt {
            report(format!("{num_attempt}"));
            lines.push(format!("{total_capacity},{num_attempt},{num_stored}"));
            max_num_attempt = num_attempt
        }
    }
    lines
}

fn create_workload(
    num_copy: usize,
    two_choices: bool,
    rng: &mut StdRng,
    network: &Network,
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
        let score0 = node_ids0.iter().map(|id| nodes[id].score()).min();
        let data_id1 = rng.random();
        let node_ids1 = network.find(data_id1, num_copy);
        let score1 = node_ids1.iter().map(|id| nodes[id].score()).min();
        (data_id, node_ids) = if score0 > score1 {
            (data_id0, node_ids0)
        } else {
            (data_id1, node_ids1)
        }
    }
    (data_id, node_ids)
}

fn create_network(
    num_node: usize,
    node_min_capacity: usize,
    classified: bool,
    node_capacity_variance_distr: Zipf<f32>,
    mut rng: impl Rng,
) -> (
    Network,
    std::collections::HashMap<u64, Node, rustc_hash::FxBuildHasher>,
    usize,
) {
    let mut network = if classified {
        Network::Classified(Classified::new())
    } else {
        Network::Vanilla(VanillaBin::new())
    };
    let mut nodes = HashMap::default();
    // let mut data_placements = HashMap::<_, Vec<_>>::default();
    let mut total_capacity = 0;
    for _ in 0..num_node {
        let node_id = rng.random();
        let node = Node {
            capacity: node_min_capacity
                + node_capacity_variance_distr.sample(&mut rng).ceil() as usize
                - 1,
            data: Default::default(),
        };
        total_capacity += node.capacity;
        match &mut network {
            Network::Vanilla(network) => network.insert_node(node_id),
            Network::Classified(network) => network.insert_node(
                node_id,
                (node.capacity as f32 / node_min_capacity as f32)
                    .log2()
                    .round() as _,
            ),
        }
        let replaced = nodes.insert(node_id, node);
        assert!(replaced.is_none(), "duplicated node {node_id:016x}")
    }
    // println!("Total capacity {total_capacity}");
    if let Network::Classified(network) = &mut network {
        network.optimize()
    }
    (network, nodes, total_capacity)
}

fn ingest(
    mut rng: impl Rng,
    nodes: &mut HashMap<NodeId, Node>,
    // data_placements: &mut HashMap<u64, Vec<u64>>,
    copy_counts: &mut HashMap<DataId, u8>,
    data_id: DataId,
    node_ids: Vec<NodeId>,
) -> bool {
    for &node_id in &node_ids {
        let node = nodes.get_mut(&node_id).unwrap();
        if node.data.len() < node.capacity {
            node.data.push(data_id)
        } else {
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
                return false;
            }
        }
    }
    // let replaced = data_placements.insert(data_id, node_ids);
    let replaced = copy_counts.insert(data_id, node_ids.len() as _);
    assert!(replaced.is_none(), "duplicated data {data_id:016x}");
    true
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
