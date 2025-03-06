use std::collections::HashSet;

use proptest::{prelude::*, sample::SizeRange, test_runner::FileFailurePersistence};

use crate::{Classified, DataId, NodeId, VanillaBin, VanillaTrie, classified};

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

proptest! {
    #[test]
    fn vanilla_find_node_closest(node_ids: HashSet<NodeId>, data_id: DataId) {
        let mut network = VanillaBin::new();
        let mut sorted_node_ids = node_ids.iter().cloned().collect::<Vec<_>>();
        sorted_node_ids.sort_unstable_by_key(|id| id ^ data_id);
        for node_id in node_ids {
            network.insert_node(node_id)
        }
        for i in 1..sorted_node_ids.len() {
            let node_ids = network.find(data_id, i);
            assert_eq!(node_ids.len(), i);
            assert!(sorted_node_ids[..i].iter().all(|id| node_ids.contains(id)))
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1 << 10, // ~1.02s
        ..common_config()
    })]
    #[test]
    fn vanilla_trie_find_node_closest(node_ids: HashSet<NodeId>, data_id: DataId) {
        let mut network = VanillaTrie::new();
        let mut sorted_node_ids = node_ids.iter().cloned().collect::<Vec<_>>();
        sorted_node_ids.sort_unstable_by_key(|id| id ^ data_id);
        for node_id in node_ids {
            network.insert_node(node_id)
        }
        for i in 1..sorted_node_ids.len() {
            let node_ids = network.find(data_id, i);
            // println!("{data_id:016x} {:016x?} {node_ids:016x?}", sorted_node_ids);
            assert_eq!(node_ids.len(), i);
            assert!(sorted_node_ids[..i].iter().all(|id| node_ids.contains(id)))
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1 << 10, // ~0.8s
        ..common_config()
    })]
    #[test]
    fn vanilla_trie_compress(node_ids: HashSet<NodeId>, data_id: DataId) {
        if node_ids.is_empty() {
            return Ok(())
        }
        let mut network = VanillaTrie::new();
        let mut sorted_node_ids = node_ids.iter().cloned().collect::<Vec<_>>();
        sorted_node_ids.sort_unstable_by_key(|id| id ^ data_id);
        for node_id in node_ids {
            network.insert_node(node_id)
        }
        network.compress();
        network.assert_compressed();
        for i in 1..sorted_node_ids.len() {
            let node_ids = network.find(data_id, i);
            // println!("{data_id:016x} {:016x?} {node_ids:016x?}", sorted_node_ids);
            assert_eq!(node_ids.len(), i);
            assert!(sorted_node_ids[..i].iter().all(|id| node_ids.contains(id)))
        }
    }
}
