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

type DataId = u64;
type NodeId = u64;

struct Node {
    capacity: usize,
    data: Vec<DataId>,
}

impl Node {
    fn score(&self) -> usize {
        self.capacity - self.data.len()
    }
}

struct Network {
    subnets: Vec<Vec<NodeId>>,
}

const SUBNET_PROXIMITY: u32 = 11;

impl Network {
    fn new() -> Self {
        Self {
            subnets: (0..1 << SUBNET_PROXIMITY)
                .map(|_| Default::default())
                .collect(),
        }
    }

    fn insert_node(&mut self, node_id: NodeId) {
        self.subnets[(node_id >> (NodeId::BITS - SUBNET_PROXIMITY)) as usize].push(node_id)
    }

    fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        let data_subnet = data_id >> (DataId::BITS - SUBNET_PROXIMITY);
        let mut node_ids = Vec::new();
        for diff in 0.. {
            let mut subnet = self.subnets[(data_subnet ^ diff) as usize].clone();
            if subnet.len() <= count - node_ids.len() {
                node_ids.extend(subnet.clone())
            } else {
                subnet.sort_unstable_by_key(|id| id ^ data_id);
                node_ids.extend(subnet.into_iter().take(count - node_ids.len()))
            }
            if node_ids.len() == count {
                break;
            }
        }
        node_ids
    }
}

fn main() -> anyhow::Result<()> {
    let num_node: usize = 12_000;
    let num_copy: usize = 3;

    let mut rng = rng();
    create_dir_all("data/ingest")?;
    File::create("data/.gitignore")?.write_all(b"*")?;
    let mut output = File::create(format!(
        "data/ingest/{}.csv",
        UNIX_EPOCH.elapsed().unwrap().as_secs()
    ))?;
    writeln!(
        &mut output,
        "num_node,node_min_capacity,node_max_capacity,capacity_skew,num_copy,strategy,total_capacity,num_stored,num_utilized_node,utilized_capacity,redundancy"
    )?;

    // let node_capacity: usize = 4 << 10;
    // for two_choices in [false, true] {
    //     for node_min_capacity in [1, 2, 3, 4].into_iter().map(|n| n << 10) {
    //         run(
    //             num_node,
    //             node_min_capacity,
    //             node_capacity,
    //             num_copy,
    //             two_choices,
    //             StdRng::from_rng(&mut rng),
    //             &mut output,
    //         )?
    //     }
    // }

    for two_choices in [false, true] {
        for node_min_capacity in (6..=12).step_by(2).map(|k| 1 << k) {
            run(
                100,
                num_node,
                node_min_capacity,
                node_min_capacity * 100,
                1.5,
                num_copy,
                two_choices,
                &mut rng,
                &mut output,
            )?
        }
    }

    let node_min_capacity = 1 << 12;
    for two_choices in [false, true] {
        let mut skew = 1.;
        while skew <= 2. {
            run(
                100,
                num_node,
                node_min_capacity,
                node_min_capacity * 100,
                skew,
                num_copy,
                two_choices,
                &mut rng,
                &mut output,
            )?;
            skew += 0.1
        }
    }
    Ok(())
}

fn run(
    num_sim: u32,
    num_node: usize,
    node_min_capacity: usize,
    node_max_capacity: usize,
    capacity_skew: f32,
    num_copy: usize,
    two_choices: bool,
    mut rng: impl Rng,
    mut output: impl Write,
) -> anyhow::Result<()> {
    println!("Capacity {node_min_capacity}/{node_max_capacity} Two choices {two_choices}");
    let node_capacity_variance_distr = Zipf::new(
        (node_max_capacity - node_min_capacity) as f32,
        capacity_skew,
    )?;
    let results = (0..num_sim).map(|i| (i, StdRng::from_rng(&mut rng))).collect::<Vec<_>>().into_par_iter().map(|(sim_i, mut rng)| {
        let mut network = Network::new();
        let mut nodes = HashMap::default();
        // let mut data_placements = HashMap::<_, Vec<_>>::default();
        let mut total_capacity = 0;
        for _ in 0..num_node {
            let node_id = rng.random();
            let node = Node {
                capacity: node_min_capacity
                    + node_capacity_variance_distr.sample(&mut rng).ceil() as usize - 1,
                data: Default::default(),
            };
            total_capacity += node.capacity;
            network.insert_node(node_id);
            let replaced = nodes.insert(node_id, node);
            assert!(replaced.is_none(), "duplicated node {node_id:016x}")
        }
        // println!("Total capacity {total_capacity}");
        let mut num_stored = 0;
        let mut copy_counts = HashMap::default();
        let mut last_report = Instant::now();
        loop {
            let data_id;
            let node_ids;
            if !two_choices {
                data_id = rng.random();
                node_ids = network.find(data_id, num_copy);
            } else {
                let data_id0 = rng.random();
                let node_ids0 = network.find(data_id0, num_copy);
                let score0 = node_ids0.iter().map(|id| nodes[id].score()).sum::<usize>();
                let data_id1 = rng.random();
                let node_ids1 = network.find(data_id1, num_copy);
                let score1 = node_ids1.iter().map(|id| nodes[id].score()).sum::<usize>();
                (data_id, node_ids) = if score0 > score1 {
                    (data_id0, node_ids0)
                } else {
                    (data_id1, node_ids1)
                }
            }
            if !ingest(
                &mut rng,
                &mut nodes,
                &mut copy_counts,
                data_id,
                node_ids,
            ) {
                break;
            }
            num_stored += 1;
            if last_report.elapsed() >= Duration::from_secs(1) {
                eprint!(
                    "\r[{sim_i:03}/{num_sim:03}] {:120}",
                    format!("Stored {num_stored}"));
                last_report = Instant::now()
            }
        }
        let num_utilized_node = nodes.values().filter(|node| !node.data.is_empty()).count();
        let utilized_capacity = nodes.values().map(|node| node.data.len()).sum::<usize>();
        eprint!(
            "\r[{sim_i:03}/{num_sim:03}] {:120}",
            format!(
                "Stored {num_stored} Utiliazed Node {:.2} Utilized Capacity {:.2} Average Redundancy {:.2}",
                num_utilized_node as f32 / nodes.len() as f32,
                utilized_capacity as f32 / total_capacity as f32,
                utilized_capacity as f32 / num_stored as f32));
        format!(
            "{num_node},{node_min_capacity},{node_max_capacity},{capacity_skew},{num_copy},{},{total_capacity},{num_stored},{num_utilized_node},{utilized_capacity},{}",
            if two_choices { "TwoChoices" } else { "Vanilla" },
            utilized_capacity as f32 / num_stored as f32
        )
    }).collect::<Vec<_>>();
    eprintln!();
    for line in results {
        writeln!(&mut output, "{line}")?
    }
    Ok(())
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
