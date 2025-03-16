use std::collections::{HashMap, HashSet};

use proptest::{prelude::*, sample::SizeRange, test_runner::FileFailurePersistence};

use crate::{BinOverlay, Classified, NodeId, Target, TrieOverlay, classified, find};

fn common_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        ..ProptestConfig::with_failure_persistence(FileFailurePersistence::WithSource(
            "regressions",
        ))
    }
}

prop_compose! {
    fn classified_node_id()(node_id: NodeId, class in 0..NodeId::BITS) -> classified::NodeId {
        (node_id, class as u8)
    }
}

fn classified_node_ids() -> impl Strategy<Value = HashSet<classified::NodeId>> {
    prop::collection::hash_set(classified_node_id(), SizeRange::default())
}

proptest! {
    #![proptest_config(common_config(1 << 10))]
    #[test]
    fn overlay_find(node_ids: HashSet<NodeId>, target: Target) {
        let mut trie = TrieOverlay::new();
        let mut bin = BinOverlay::new();
        for &node_id in &node_ids {
            trie.insert_node(node_id);
            bin.insert_node(node_id)
        }
        let mut compressed_trie = trie.clone();
        compressed_trie.compress();
        let mut node_ids = node_ids.into_iter().collect::<Vec<_>>();
        for count in 1..node_ids.len() {
            let ground_truth = find(&mut node_ids, target, count);
            let results = trie.find(target, count);
            assert_eq!(results.len(), count);
            assert!(ground_truth.iter().all(|id| results.contains(id)));
            let results = compressed_trie.find(target, count);
            assert_eq!(results.len(), count);
            assert!(ground_truth.iter().all(|id| results.contains(id)));
            let results = bin.find(target, count);
            assert_eq!(results.len(), count);
            assert!(ground_truth.iter().all(|id| results.contains(id)))
        }
    }
}

proptest! {
    #![proptest_config(common_config(1 << 10))]
    #[test]
    fn classified_overlay_find(node_ids in classified_node_ids(), target: Target) {
        let mut overlay = Classified::new();
        let mut distances = Vec::new();
        let mut node_classes = HashMap::new();
        for (node_id, class) in node_ids {
            node_classes.insert(node_id, class);
            overlay.insert_node(node_id, class);
            distances.push(classified::distance(node_id, target, class))
        }
        let mut optimized_overlay = overlay.clone();
        optimized_overlay.optimize();
        distances.sort_unstable();
        for count in 1..distances.len() {
            let results = overlay.find(target, count);
            assert_eq!(results.len(), count);
            assert!(results.into_iter().all(|id| classified::distance(id, target, node_classes[&id]) <= distances[count - 1]));
            let results = optimized_overlay.find(target, count);
            assert_eq!(results.len(), count);
            assert!(results.into_iter().all(|id| classified::distance(id, target, node_classes[&id]) <= distances[count - 1]))
        }
    }
}

proptest! {
    #![proptest_config(common_config(1 << 10))]
    #[test]
    fn trie_compress(node_ids: HashSet<NodeId>) {
        prop_assume!(!node_ids.is_empty());
        let mut overlay = TrieOverlay::new();
        for node_id in node_ids {
            overlay.insert_node(node_id)
        }
        overlay.compress();
        overlay.assert_compressed()
    }
}
