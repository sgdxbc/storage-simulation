use std::cell::RefCell;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// this three should always alias to the same type
// just use different names at places for more readable code
pub type NodeId = u64;
pub type Target = u64; // either id of node or data
pub type Distance = u64;

pub fn distance(node_id: NodeId, target: Target) -> Distance {
    node_id ^ target
}

pub fn find(node_ids: &mut [NodeId], target: Target, count: usize) -> Vec<NodeId> {
    node_ids.sort_unstable_by_key(|&id| distance(id, target));
    node_ids.iter().take(count).copied().collect()
}

pub type Class = u8;

pub mod classified {
    pub type NodeId = (super::NodeId, super::Class);

    pub fn distance(
        node_id: super::NodeId,
        target: super::Target,
        class: super::Class,
    ) -> super::Distance {
        (node_id ^ target) & (!0 >> class)
    }

    pub fn find(
        node_ids: &mut [NodeId],
        target: super::Target,
        count: usize,
    ) -> Vec<super::NodeId> {
        node_ids.sort_unstable_by_key(|&(id, class)| distance(id, target, class));
        node_ids
            .iter()
            .take(count)
            .map(|&(node_id, _)| node_id)
            .collect()
    }

    pub fn subnet_index(id: super::NodeId, class: super::Class) -> usize {
        // take the next (up to) SUBNET_BITS bits from the `class`th highest bit
        // when class is large, just make sure to include every bit starting with the `class`th
        // highest bit, the padding bits can be anything at anywhere
        (id << class >> (super::NodeId::BITS - super::SUBNET_BITS)) as _
    }
}

pub enum Overlay {
    Vanilla(BinOverlay),
    Classified(Classified),
}

impl Overlay {
    pub fn find(&self, target: Target, count: usize) -> Vec<NodeId> {
        match self {
            Self::Vanilla(overlay) => overlay.find(target, count),
            Self::Classified(overlay) => overlay.find(target, count),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinOverlay {
    subnets: Vec<Vec<NodeId>>,
}

const SUBNET_BITS: u32 = 11;

impl Default for BinOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl BinOverlay {
    pub fn new() -> Self {
        Self {
            subnets: (0..1 << SUBNET_BITS).map(|_| Default::default()).collect(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.insert_classified_node(node_id, 0)
    }

    fn insert_classified_node(&mut self, target: NodeId, class: Class) {
        self.subnets[classified::subnet_index(target, class)].push(target)
    }

    pub fn find(&self, target: Target, count: usize) -> Vec<NodeId> {
        self.find_classified(target, count, 0)
    }

    fn find_classified(&self, target: Target, count: usize, class: Class) -> Vec<NodeId> {
        let target_subnet_index = classified::subnet_index(target, class);
        let mut node_ids = Vec::new();
        for diff in 0..1 << SUBNET_BITS {
            let mut subnet = self.subnets[target_subnet_index ^ diff].clone();
            if subnet.len() <= count - node_ids.len() {
                node_ids.extend(subnet)
            } else {
                subnet.sort_unstable_by_key(|&id| classified::distance(id, target, class));
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
pub struct TrieOverlay {
    data: TrieData,
}

#[derive(Debug, Clone)]
enum TrieData {
    Empty,
    Node(NodeId),
    Fork(Box<SubTries>),
}

#[derive(Debug, Clone)]
struct SubTries {
    zero: TrieOverlay,
    one: TrieOverlay,
    skip: u32,
}

impl Default for TrieOverlay {
    fn default() -> Self {
        Self::new()
    }
}

impl TrieOverlay {
    pub fn new() -> Self {
        Self {
            data: TrieData::Empty,
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
        self.insert_classified_node(node_id, 0)
    }

    fn insert_classified_node(&mut self, node_id: NodeId, class: Class) {
        self.insert_node_level(node_id, NodeId::BITS - 1 - class as u32)
    }

    fn level_bit(node_id: NodeId, level: u32) -> bool {
        (node_id >> level) & 1 == 0
    }

    fn insert_node_level(&mut self, node_id: NodeId, level: u32) {
        match &mut self.data {
            TrieData::Empty => self.data = TrieData::Node(node_id),
            TrieData::Node(other_node_id) => {
                assert_ne!(node_id, *other_node_id);
                let mut trie0 = TrieOverlay::new();
                let mut trie1 = TrieOverlay::new();
                for node_id in [node_id, *other_node_id] {
                    if Self::level_bit(node_id, level) {
                        &mut trie0
                    } else {
                        &mut trie1
                    }
                    .insert_node_level(node_id, level - 1)
                }
                self.data = TrieData::Fork(
                    SubTries {
                        zero: trie0,
                        one: trie1,
                        skip: 0,
                    }
                    .into(),
                )
            }
            TrieData::Fork(fork) => if Self::level_bit(node_id, level) {
                &mut fork.zero
            } else {
                &mut fork.one
            }
            .insert_node_level(node_id, level - 1),
        }
    }

    pub fn compress(&mut self) {
        let TrieData::Fork(fork) = &mut self.data else {
            return;
        };
        fork.zero.compress();
        fork.one.compress();
        use TrieData::*;
        let nested_fork = match (&fork.zero.data, &fork.one.data) {
            (Empty, Fork(fork)) | (Fork(fork), Empty) => fork.clone(),
            (Empty, _) | (_, Empty) => unreachable!(),
            _ => return,
        };
        *fork = nested_fork;
        fork.skip += 1
    }

    #[cfg(test)]
    fn assert_compressed(&self) {
        assert!(!matches!(self.data, TrieData::Empty));
        if let TrieData::Fork(fork) = &self.data {
            fork.zero.assert_compressed();
            fork.one.assert_compressed()
        }
    }

    pub fn find(&self, target: Target, count: usize) -> Vec<NodeId> {
        self.find_classified(target, count, 0)
    }

    fn find_classified(&self, target: Target, count: usize, class: Class) -> Vec<NodeId> {
        self.find_level(target, count, NodeId::BITS - 1 - class as u32)
    }

    fn find_level(&self, target: Target, count: usize, mut level: u32) -> Vec<NodeId> {
        match &self.data {
            TrieData::Empty => vec![],
            TrieData::Node(node_id) => vec![*node_id],
            TrieData::Fork(fork) => {
                level -= fork.skip;
                let (primary_trie, secondary_trie) = {
                    if Self::level_bit(target, level) {
                        (&fork.zero, &fork.one)
                    } else {
                        (&fork.one, &fork.zero)
                    }
                    // the branchless version did not show performance improvement
                    // probably auto-applied by compiler
                    // let b = Self::level_bit(data_id, level) as usize;
                    // let ts = [&fork.one, &fork.zero];
                    // (ts[b], ts[1 - b])
                };
                let mut node_ids = primary_trie.find_level(target, count, level - 1);
                if node_ids.len() < count {
                    node_ids.extend(secondary_trie.find_level(
                        target,
                        count - node_ids.len(),
                        level - 1,
                    ))
                }
                node_ids
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Classified {
    classes: Vec<ClassOverlay>,
}

#[derive(Debug, Clone)]
pub enum ClassOverlay {
    Naive(RefCell<Vec<NodeId>>), // interior mutability for sorting inside `find`
    Trie(TrieOverlay),
    Bin(BinOverlay),
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
        node_ids.borrow_mut().push(node_id)
    }

    pub fn optimize(&mut self) {
        for (class, class_overlay) in self.classes.iter_mut().enumerate() {
            let ClassOverlay::Naive(node_ids) = &class_overlay else {
                unimplemented!()
            };
            let replace_overlay = {
                let node_ids = &*node_ids.borrow();
                if node_ids.len() >= 512 {
                    let mut overlay = BinOverlay::new();
                    for &node_id in node_ids {
                        overlay.insert_classified_node(node_id, class as _)
                    }
                    ClassOverlay::Bin(overlay)
                } else if node_ids.len() >= 16 {
                    let mut overlay = TrieOverlay::new();
                    for &node_id in node_ids {
                        overlay.insert_classified_node(node_id, class as _)
                    }
                    overlay.compress();
                    ClassOverlay::Trie(overlay)
                } else {
                    continue;
                }
            };
            *class_overlay = replace_overlay
        }
    }

    pub fn find(&self, target: Target, count: usize) -> Vec<NodeId> {
        let mut node_ids = self
            .classes
            .iter()
            .enumerate()
            .flat_map(|(class, class_overlay)| {
                let class = class as _;
                match class_overlay {
                    ClassOverlay::Naive(node_ids) => {
                        node_ids
                            .borrow_mut()
                            .sort_unstable_by_key(|&id| classified::distance(id, target, class));
                        node_ids.borrow().iter().take(count).copied().collect()
                    }
                    ClassOverlay::Trie(overlay) => overlay.find_classified(target, count, class),
                    ClassOverlay::Bin(overlay) => overlay.find_classified(target, count, class),
                }
                .into_iter()
                .map(move |id| (id, class))
            })
            .collect::<Vec<_>>();
        classified::find(&mut node_ids, target, count)
    }
}

#[cfg(test)]
mod tests;
