use std::{
    collections::HashMap,
    fs::{File, create_dir_all},
    io::Write,
    mem::replace,
    time::{Duration, Instant, UNIX_EPOCH},
};

use rand::{Rng, SeedableRng, rng, rngs::StdRng};
use rand_distr::{Distribution, Exp};

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
    let node_capacity: usize = 4 << 10;
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
        "num_node,node_min_capacity,node_capacity,num_copy,strategy,total_capacity,num_stored,num_utilized_node,utilized_capacity,redundancy"
    )?;
    for two_choices in [false, true] {
        for node_min_capacity in [1, 2, 3, 4].into_iter().map(|n| n << 10) {
            for _ in 0..100 {
                run(
                    num_node,
                    node_min_capacity,
                    node_capacity,
                    num_copy,
                    two_choices,
                    StdRng::from_rng(&mut rng),
                    &mut output,
                )?
            }
        }
    }
    Ok(())
}

fn run(
    num_node: usize,
    node_min_capacity: usize,
    node_capacity: usize,
    num_copy: usize,
    two_choices: bool,
    mut rng: impl Rng,
    mut output: impl Write,
) -> anyhow::Result<()> {
    println!("Min capacity {node_min_capacity} Two choices {two_choices}");
    let mut network = Network::new();
    let mut nodes = HashMap::new();
    let mut data_placements = HashMap::<_, Vec<_>>::new();
    let node_capacity_variance_distr = Exp::new(1. / (node_capacity - node_min_capacity) as f32)?;
    let mut total_capacity = 0;
    for _ in 0..num_node {
        let node_id = rng.random();
        let node = Node {
            capacity: node_min_capacity
                + node_capacity_variance_distr.sample(&mut rng).ceil() as usize,
            data: Default::default(),
        };
        total_capacity += node.capacity;
        network.insert_node(node_id);
        let replaced = nodes.insert(node_id, node);
        assert!(replaced.is_none(), "duplicated node {node_id:016x}")
    }
    println!("Total capacity {total_capacity}");
    let mut num_stored = 0;
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
            &mut data_placements,
            data_id,
            node_ids,
        ) {
            break;
        }
        num_stored += 1;
        if last_report.elapsed() >= Duration::from_secs(1) {
            println!("Stored {num_stored}");
            last_report = Instant::now()
        }
    }
    let num_utilized_node = nodes.values().filter(|node| !node.data.is_empty()).count();
    let utilized_capacity = nodes.values().map(|node| node.data.len()).sum::<usize>();
    println!(
        "Stored {num_stored} Utiliazed Node {:.2} Utilized Capacity {:.2} Average Redundancy {:.2}",
        num_utilized_node as f32 / nodes.len() as f32,
        utilized_capacity as f32 / total_capacity as f32,
        utilized_capacity as f32 / num_stored as f32,
    );
    writeln!(
        output,
        "{num_node},{node_min_capacity},{node_capacity},{num_copy},{},{total_capacity},{num_stored},{num_utilized_node},{utilized_capacity},{}",
        if two_choices { "TwoChoices" } else { "Vanilla" },
        utilized_capacity as f32 / num_stored as f32
    )?;
    Ok(())
}

fn ingest(
    mut rng: impl Rng,
    nodes: &mut HashMap<u64, Node>,
    data_placements: &mut HashMap<u64, Vec<u64>>,
    data_id: u64,
    node_ids: Vec<u64>,
) -> bool {
    for &node_id in &node_ids {
        let node = nodes.get_mut(&node_id).unwrap();
        if node.data.len() < node.capacity {
            node.data.push(data_id)
        } else {
            let evicted = replace(&mut node.data[rng.random_range(0..node.capacity)], data_id);
            let evicted_placement = data_placements.get_mut(&evicted).unwrap();
            evicted_placement.remove(
                evicted_placement
                    .iter()
                    .position(|&id| id == node_id)
                    .unwrap(),
            );
            // println!("Evicted {node_id:016x} Left {evicted_placement:016x?}");
            if evicted_placement.is_empty() {
                return false;
            }
        }
    }
    let replaced = data_placements.insert(data_id, node_ids);
    assert!(replaced.is_none(), "duplicated data {data_id:016x}");
    true
}
