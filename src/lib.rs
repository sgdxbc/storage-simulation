#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// this three should always alias to the same type
// just use different names at places for more readable code
pub type DataId = u64;
pub type NodeId = u64;
pub type Distance = u64;

pub enum Network {
    Vanilla(VanillaBin),
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

pub struct VanillaBin {
    subnets: Vec<Vec<NodeId>>,
}

const SUBNET_BITS: u32 = 11;

impl Default for VanillaBin {
    fn default() -> Self {
        Self::new()
    }
}

impl VanillaBin {
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

#[derive(Debug, Clone)]
pub struct VanillaTrie {
    data: VanillaTrieData,
}

#[derive(Debug, Clone)]
enum VanillaTrieData {
    Empty,
    Node(NodeId),
    SubTrie(Box<VanillaSubTrie>),
}

#[derive(Debug, Clone)]
struct VanillaSubTrie {
    zero: VanillaTrie,
    one: VanillaTrie,
    skip: u32,
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

    fn is_empty(&self) -> bool {
        matches!(self.data, VanillaTrieData::Empty)
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.insert_node_level(node_id, NodeId::BITS - 1)
    }

    fn level_bit(node_id: NodeId, level: u32) -> bool {
        (node_id >> level) & 1 == 0
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
                    .insert_node_level(node_id, level - 1)
                }
                self.data = VanillaTrieData::SubTrie(
                    VanillaSubTrie {
                        zero: trie0,
                        one: trie1,
                        skip: 0,
                    }
                    .into(),
                )
            }
            VanillaTrieData::SubTrie(fork) => if Self::level_bit(node_id, level) {
                &mut fork.zero
            } else {
                &mut fork.one
            }
            .insert_node_level(node_id, level - 1),
        }
    }

    pub fn compress(&mut self) {
        let VanillaTrieData::SubTrie(fork) = &mut self.data else {
            return;
        };
        fork.zero.compress();
        fork.one.compress();
        use VanillaTrieData::*;
        let nested_fork = match (&fork.zero.data, &fork.one.data) {
            (Empty, SubTrie(fork)) | (SubTrie(fork), Empty) => fork.clone(),
            (Empty, _) | (_, Empty) => unreachable!(),
            _ => return,
        };
        *fork = nested_fork;
        fork.skip += 1
    }

    #[cfg(test)]
    fn assert_compressed(&self) {
        assert!(!self.is_empty());
        if let VanillaTrieData::SubTrie(fork) = &self.data {
            fork.zero.assert_compressed();
            fork.one.assert_compressed()
        }
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        self.find_level(data_id, count, NodeId::BITS - 1)
    }

    fn find_level(&self, data_id: DataId, count: usize, mut level: u32) -> Vec<NodeId> {
        match &self.data {
            VanillaTrieData::Empty => vec![],
            VanillaTrieData::Node(node_id) => vec![*node_id],
            VanillaTrieData::SubTrie(fork) => {
                level -= fork.skip;
                let (primary_trie, secondary_trie) = if Self::level_bit(data_id, level) {
                    (&fork.zero, &fork.one)
                } else {
                    (&fork.one, &fork.zero)
                };
                let mut node_ids = primary_trie.find_level(data_id, count, level - 1);
                if node_ids.len() < count {
                    node_ids.extend(secondary_trie.find_level(
                        data_id,
                        count - node_ids.len(),
                        level - 1,
                    ))
                }
                node_ids
            }
        }
    }
}

pub type Class = u8;

pub mod classified {
    pub type NodeId = (super::NodeId, super::Class);

    pub fn distance(
        node_id: super::NodeId,
        target: super::DataId,
        class: super::Class,
    ) -> super::Distance {
        (node_id ^ target) & (!0 >> class)
    }
}

#[derive(Debug, Clone)]
pub struct Classified {}

impl Default for Classified {
    fn default() -> Self {
        Self::new()
    }
}
impl Classified {
    pub fn new() -> Self {
        Self {}
    }

    pub fn insert_node(&mut self, node_id: NodeId, class: Class) {}

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests;
