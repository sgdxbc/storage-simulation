use proptest::{prelude::*, sample::SizeRange, test_runner::FileFailurePersistence};

use crate::{Classified, DataId, NodeId, classified};

fn common_config() -> ProptestConfig {
    ProptestConfig::with_failure_persistence(FileFailurePersistence::WithSource("regressions"))
}

prop_compose! {
    fn classified_node_id()(node_id: NodeId, class in 0..NodeId::BITS) -> classified::NodeId {
        (node_id, class as u8)
    }
}

fn classified_node_ids() -> impl Strategy<Value = Vec<classified::NodeId>> {
    prop::collection::vec(classified_node_id(), SizeRange::default())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1 << 12, // ~0.6s
        ..common_config()
    })]
    #[test]
    fn classified_find_node_works(node_ids in classified_node_ids(), data_id: DataId) {
        let mut network = Classified::new();
        for (node_id, class) in node_ids {
            network.insert_node(node_id, class)
        }
        network.find(data_id, 3);
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1 << 13, // ~0.9s
        ..common_config()
    })]
    #[test]
    fn classified_find_node_self(node_ids: Vec<NodeId>, find_node_id in classified_node_id()) {
        let mut network = Classified::new();
        for node_id in node_ids {
            network.insert_node(node_id, 0)
        }
        let (node_id, class) = find_node_id;
        network.insert_node(node_id, class);
        let result_node_ids = network.find(node_id, 1);
        assert_eq!(result_node_ids, vec![node_id])
    }
}
