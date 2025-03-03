#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub type DataId = u64;
pub type NodeId = u64;
pub type Distance = u64;

pub trait Overlay {
    fn find(&self, target: DataId, count: usize) -> Vec<NodeId>;
}

pub struct Vanilla {
    subnets: Vec<Vec<NodeId>>,
}

const SUBNET_PROXIMITY: u32 = 11;

impl Default for Vanilla {
    fn default() -> Self {
        Self::new()
    }
}

impl Vanilla {
    pub fn new() -> Self {
        Self {
            subnets: (0..1 << SUBNET_PROXIMITY)
                .map(|_| Default::default())
                .collect(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId) {
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

impl Overlay for Vanilla {
    fn find(&self, target: DataId, count: usize) -> Vec<NodeId> {
        Vanilla::find(self, target, count)
    }
}

pub mod classified {
    pub type NodeId = (super::NodeId, u8);

    pub fn subnet(node_id: super::NodeId, class: u8) -> usize {
        if class >= super::SUBNET_PROXIMITY as u8 {
            0
        } else {
            (node_id >> (super::NodeId::BITS - super::SUBNET_PROXIMITY + class as u32)) as _
        }
    }

    pub fn distance(node_id: super::NodeId, target: super::DataId, class: u8) -> super::Distance {
        (node_id ^ target) & (!0 >> class)
    }
}

#[derive(Debug)]
pub struct Classified {
    // class 0..SUBNET_PROXIMITY
    // [class -> [subnet prefix -> [node id]]]
    subnets: Vec<Vec<Vec<NodeId>>>,
    // class SUBNET_PROXIMITY and above
    unclassified: Vec<classified::NodeId>,
}

impl Default for Classified {
    fn default() -> Self {
        Self::new()
    }
}

impl Classified {
    pub fn new() -> Self {
        Self {
            subnets: (0..SUBNET_PROXIMITY)
                .map(|class| {
                    (0..1 << (SUBNET_PROXIMITY - class))
                        .map(|_| Default::default())
                        .collect()
                })
                .collect(),
            unclassified: Default::default(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId, class: u8) {
        if class >= SUBNET_PROXIMITY as u8 {
            self.unclassified.push((node_id, class))
        } else {
            self.subnets[class as usize][classified::subnet(node_id, class)].push(node_id)
        }
    }

    fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        let original_subnet_index = (data_id >> (DataId::BITS - SUBNET_PROXIMITY)) as usize;
        let mut node_ids = self.unclassified.clone();
        let mut mask = (1 << SUBNET_PROXIMITY) - 1;
        for class in 0..SUBNET_PROXIMITY {
            node_ids.extend(
                self.subnets[class as usize][original_subnet_index & mask]
                    .iter()
                    .map(|&id| (id, class as _)),
            );
            mask >>= 1
        }
        let mut diff = 1;
        while node_ids.len() < count && diff < (1 << SUBNET_PROXIMITY) {
            let subnet_index = original_subnet_index ^ diff;
            let class_mask = diff ^ (diff - 1);
            let mut mask = (1 << SUBNET_PROXIMITY) - 1;
            for class in 0..SUBNET_PROXIMITY {
                if (class_mask & (1 << class)) != 0 {
                    node_ids.extend(
                        self.subnets[class as usize][subnet_index & mask]
                            .iter()
                            .map(|&id| (id, class as _)),
                    )
                }
                mask >>= 1
            }
            diff += 1
        }
        debug_assert!(
            node_ids
                .iter()
                .enumerate()
                .all(|(i, id)| node_ids.iter().skip(i + 1).all(|other_id| id != other_id)),
            "{node_ids:016x?}"
        );
        node_ids.sort_unstable_by_key(|&(id, class)| classified::distance(id, data_id, class));
        node_ids.into_iter().take(count).map(|(id, _)| id).collect()
    }
}

impl Overlay for Classified {
    fn find(&self, target: DataId, count: usize) -> Vec<NodeId> {
        Classified::find(self, target, count)
    }
}

#[cfg(test)]
mod tests;
