#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// this three should always alias to the same type
// just use different names at places for more readable code
pub type DataId = u64;
pub type NodeId = u64;
pub type Distance = u64;

pub enum Overlay {
    Vanilla(VanillaBin),
    Classified(Classified),
}

impl Overlay {
    pub fn find(&self, target: DataId, count: usize) -> Vec<NodeId> {
        match self {
            Self::Vanilla(overlay) => overlay.find(target, count),
            Self::Classified(overlay) => overlay.find(target, count),
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

    pub fn subnet_index(id: super::NodeId, class: super::Class) -> usize {
        // mostly equivalent to left shift `class` then right right `NodeId::BITS - SUBNET_BITS`
        // but different with large classes
        ((id >> (super::NodeId::BITS - super::SUBNET_BITS - class as u32))
            & ((1 << super::SUBNET_BITS) - 1)) as _
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
        self.insert_classified_node(node_id, 0)
    }

    fn insert_classified_node(&mut self, node_id: NodeId, class: Class) {
        self.subnets[classified::subnet_index(node_id, class)].push(node_id)
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        self.find_classified(data_id, count, 0)
    }

    fn find_classified(&self, data_id: DataId, count: usize, class: Class) -> Vec<NodeId> {
        let local_subnet_index = classified::subnet_index(data_id, class);
        let mut node_ids = Vec::new();
        for diff in 0..1 << SUBNET_BITS {
            let mut subnet = self.subnets[local_subnet_index ^ diff].clone();
            if subnet.len() <= count - node_ids.len() {
                node_ids.extend(subnet)
            } else {
                subnet.sort_unstable_by_key(|id| classified::distance(*id, data_id, class));
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

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.insert_classified_node(node_id, 0)
    }

    fn insert_classified_node(&mut self, node_id: NodeId, class: Class) {
        self.insert_node_level(node_id, NodeId::BITS - class as u32 - 1)
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
        assert!(!matches!(self.data, VanillaTrieData::Empty));
        if let VanillaTrieData::SubTrie(fork) = &self.data {
            fork.zero.assert_compressed();
            fork.one.assert_compressed()
        }
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        self.find_classified(data_id, count, 0)
    }

    fn find_classified(&self, data_id: DataId, count: usize, class: Class) -> Vec<NodeId> {
        self.find_level(data_id, count, NodeId::BITS - class as u32 - 1)
    }

    fn find_level(&self, data_id: DataId, count: usize, mut level: u32) -> Vec<NodeId> {
        match &self.data {
            VanillaTrieData::Empty => vec![],
            VanillaTrieData::Node(node_id) => vec![*node_id],
            VanillaTrieData::SubTrie(fork) => {
                level -= fork.skip;
                let (primary_trie, secondary_trie) = {
                    if Self::level_bit(data_id, level) {
                        (&fork.zero, &fork.one)
                    } else {
                        (&fork.one, &fork.zero)
                    }
                    // let b = Self::level_bit(data_id, level) as usize;
                    // let ts = [&fork.one, &fork.zero];
                    // (ts[b], ts[1 - b])
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

pub struct Classified {
    classes: Vec<ClassOverlay>,
}

pub enum ClassOverlay {
    Naive(Vec<NodeId>),
    Trie(VanillaTrie),
    Bin(VanillaBin),
}

impl Default for Classified {
    fn default() -> Self {
        Self::new()
    }
}
impl Classified {
    pub fn new() -> Self {
        Self {
            classes: Default::default(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId, class: Class) {
        if class as usize >= self.classes.len() {
            self.classes
                .resize_with((class + 1) as _, || ClassOverlay::Naive(Default::default()))
        }
        let ClassOverlay::Naive(node_ids) = &mut self.classes[class as usize] else {
            unimplemented!()
        };
        node_ids.push(node_id)
    }

    pub fn optimize(&mut self) {
        for (class, class_overlay) in self.classes.iter_mut().enumerate() {
            let ClassOverlay::Naive(node_ids) = &class_overlay else {
                unimplemented!()
            };
            if node_ids.len() >= 512 {
                let mut overlay = VanillaBin::new();
                for &node_id in node_ids {
                    overlay.insert_classified_node(node_id, class as _)
                }
                *class_overlay = ClassOverlay::Bin(overlay)
            } else if node_ids.len() >= 16 {
                let mut overlay = VanillaTrie::new();
                for &node_id in node_ids {
                    overlay.insert_classified_node(node_id, class as _)
                }
                overlay.compress();
                *class_overlay = ClassOverlay::Trie(overlay)
            }
        }
    }

    pub fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        let mut node_ids = self
            .classes
            .iter()
            .enumerate()
            .flat_map(|(class, class_overlay)| {
                let class = class as _;
                match class_overlay {
                    ClassOverlay::Naive(node_ids) => {
                        let mut node_ids = node_ids.clone();
                        node_ids
                            .sort_unstable_by_key(|&id| classified::distance(id, data_id, class));
                        node_ids.into_iter().take(count).collect()
                    }
                    ClassOverlay::Trie(overlay) => overlay.find_classified(data_id, count, class),
                    ClassOverlay::Bin(overlay) => overlay.find_classified(data_id, count, class),
                }
                .into_iter()
                .map(move |id| (id, class))
            })
            .collect::<Vec<_>>();
        node_ids.sort_unstable_by_key(|&(id, class)| classified::distance(id, data_id, class));
        node_ids.into_iter().take(count).map(|(id, _)| id).collect()
    }
}

#[cfg(test)]
mod tests;
