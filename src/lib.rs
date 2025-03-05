#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// this three should always alias to the same type
// just use different names at places for more readable code
pub type DataId = u64;
pub type NodeId = u64;
pub type Distance = u64;

pub enum Network {
    Vanilla(Vanilla),
    Classified(Classified),
}

impl Network {
    pub fn find(&self, target: DataId, count: usize) -> Vec<NodeId> {
        match self {
            Self::Vanilla(network) => network.find(target, count),
            Self::Classified(network) => network.find(target, count),
        }
    }
}

pub struct Vanilla {
    subnets: Vec<Vec<NodeId>>,
}

const SUBNET_BITS: u32 = 11;

impl Default for Vanilla {
    fn default() -> Self {
        Self::new()
    }
}

impl Vanilla {
    pub fn new() -> Self {
        Self {
            subnets: (0..1 << SUBNET_BITS).map(|_| Default::default()).collect(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.subnets[(node_id >> (NodeId::BITS - SUBNET_BITS)) as usize].push(node_id)
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        let local_subnet_index = data_id >> (DataId::BITS - SUBNET_BITS);
        let mut node_ids = Vec::new();
        for diff in 0..1 << SUBNET_BITS {
            let mut subnet = self.subnets[(local_subnet_index ^ diff) as usize].clone();
            if subnet.len() <= count - node_ids.len() {
                node_ids.extend(subnet)
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

pub struct VanillaTrie {
    data: VanillaTrieData,
}

enum VanillaTrieData {
    Empty,
    Node(NodeId),
    SubTrie(Box<VanillaTrie>, Box<VanillaTrie>),
}

impl Default for VanillaTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl VanillaTrie {
    pub fn new() -> Self {
        Self {
            data: VanillaTrieData::Empty,
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.insert_node_level(node_id, 0)
    }

    fn level_bit(node_id: NodeId, level: u32) -> bool {
        (node_id >> (NodeId::BITS - (level + 1))) & 1 == 0
    }

    fn insert_node_level(&mut self, node_id: NodeId, level: u32) {
        match &mut self.data {
            VanillaTrieData::Empty => self.data = VanillaTrieData::Node(node_id),
            VanillaTrieData::Node(other_node_id) => {
                assert_ne!(node_id, *other_node_id);
                let mut trie0 = VanillaTrie::new();
                let mut trie1 = VanillaTrie::new();
                for node_id in [node_id, *other_node_id] {
                    if Self::level_bit(node_id, level) {
                        &mut trie0
                    } else {
                        &mut trie1
                    }
                    .insert_node_level(node_id, level + 1)
                }
                self.data = VanillaTrieData::SubTrie(trie0.into(), trie1.into())
            }
            VanillaTrieData::SubTrie(trie0, trie1) => if Self::level_bit(node_id, level) {
                trie0
            } else {
                trie1
            }
            .insert_node_level(node_id, level + 1),
        }
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        self.find_level(data_id, count, 0)
    }

    fn find_level(&self, data_id: DataId, count: usize, level: u32) -> Vec<NodeId> {
        match &self.data {
            VanillaTrieData::Empty => vec![],
            VanillaTrieData::Node(node_id) => vec![*node_id],
            VanillaTrieData::SubTrie(trie0, trie1) => {
                let (primary_trie, secondary_trie) = if Self::level_bit(data_id, level) {
                    (trie0, trie1)
                } else {
                    (trie1, trie0)
                };
                let mut node_ids = primary_trie.find_level(data_id, count, level + 1);
                if node_ids.len() < count {
                    node_ids.extend(secondary_trie.find_level(
                        data_id,
                        count - node_ids.len(),
                        level + 1,
                    ))
                }
                node_ids
            }
        }
    }
}

pub mod classified {
    pub type NodeId = (super::NodeId, u8);

    pub fn subnet(node_id: super::NodeId, class: u8) -> usize {
        // ((node_id & (!0 >> class))
        //     >> (((super::NodeId::BITS - super::SUBNET_BITS) as u8).max(class) - class)) as _
        (node_id << class >> (super::NodeId::BITS - super::SUBNET_BITS)) as _
    }

    pub fn distance(node_id: super::NodeId, target: super::DataId, class: u8) -> super::Distance {
        (node_id ^ target) & (!0 >> class)
    }
}

#[derive(Debug)]
pub struct Classified {
    // [class -> [subnet prefix -> [node id]]]
    subnets: Vec<Vec<Vec<NodeId>>>,
}

impl Default for Classified {
    fn default() -> Self {
        Self::new()
    }
}

impl Classified {
    pub fn new() -> Self {
        Self {
            // wasting some subnets in high classes
            // probably not affecting correctness and performance
            subnets: (0..NodeId::BITS)
                .map(|_| (0..1 << SUBNET_BITS).map(|_| Default::default()).collect())
                .collect(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId, class: u8) {
        self.subnets[class as usize][classified::subnet(node_id, class)].push(node_id)
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        Default::default()
    }
}

#[cfg(test)]
mod tests;
