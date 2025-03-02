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
        if class == 0 {
            0
        } else {
            (node_id >> (super::NodeId::BITS - class as u32)) as _
        }
    }

    pub fn distance(node_id: super::NodeId, target: super::DataId, class: u8) -> super::Distance {
        (node_id ^ target) & !(!0 >> class)
    }
}

pub struct Classified {
    // class SUBNET_PROXIMITY and above
    subnets: Vec<Vec<classified::NodeId>>,
    // class 0..SUBNET_PROXIMITY
    // [class -> [subnet prefix -> [node id]]]
    classified_subnets: Vec<Vec<Vec<NodeId>>>,
}

impl Default for Classified {
    fn default() -> Self {
        Self::new()
    }
}

impl Classified {
    pub fn new() -> Self {
        Self {
            subnets: (0..1 << SUBNET_PROXIMITY)
                .map(|_| Default::default())
                .collect(),
            classified_subnets: (0..SUBNET_PROXIMITY)
                .map(|class| (0..1 << class).map(|_| Default::default()).collect())
                .collect(),
        }
    }

    pub fn insert_node(&mut self, node_id: NodeId, class: u8) {
        if class >= SUBNET_PROXIMITY as u8 {
            self.subnets[(node_id >> (NodeId::BITS - SUBNET_PROXIMITY)) as usize]
                .push((node_id, class))
        } else {
            self.classified_subnets[class as usize][classified::subnet(node_id, class)]
                .push(node_id)
        }
    }

    fn find(&self, data_id: DataId, count: usize) -> Vec<NodeId> {
        let data_subnet = data_id >> (DataId::BITS - SUBNET_PROXIMITY);
        let mut node_ids = Vec::new();
        for diff in 0.. {
            let subnet_index = (data_subnet ^ diff) as usize;
            for class in 0..SUBNET_PROXIMITY {
                let mut subnet = self.classified_subnets[class as usize]
                    [subnet_index >> (SUBNET_PROXIMITY - class)]
                    .clone();
                if subnet.len() <= count - node_ids.len() {
                    node_ids.extend(subnet);
                } else {
                    subnet
                        .sort_unstable_by_key(|&id| classified::distance(id, data_id, class as _));
                    node_ids.extend(subnet.into_iter().take(count - node_ids.len()));
                    break;
                }
            }
            let mut subnet = self.subnets[subnet_index].clone();
            if subnet.len() <= count - node_ids.len() {
                node_ids.extend(subnet.into_iter().map(|(id, _)| id))
            } else if node_ids.len() < count {
                subnet
                    .sort_unstable_by_key(|&(id, class)| classified::distance(id, data_id, class));
                node_ids.extend(
                    subnet
                        .into_iter()
                        .map(|(id, _)| id)
                        .take(count - node_ids.len()),
                )
            }
            if node_ids.len() == count {
                break;
            }
        }
        node_ids
    }
}

impl Overlay for Classified {
    fn find(&self, target: DataId, count: usize) -> Vec<NodeId> {
        Classified::find(self, target, count)
    }
}
