use super::MAX_CACHE_SIZE;
use super::page::BTreePage;
use std::collections::{HashMap, VecDeque};

pub(super) struct PageCache {
    pub(super) pages: HashMap<u64, BTreePage>,
    pub(super) access_order: VecDeque<u64>,
    hits: u64,
    misses: u64,
}

impl PageCache {
    pub(super) fn new() -> Self {
        PageCache {
            pages: HashMap::new(),
            access_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub(super) fn get_cloned(&mut self, page_id: u64) -> Option<BTreePage> {
        let Some(page) = self.pages.get(&page_id).cloned() else {
            self.misses += 1;
            return None;
        };

        self.hits += 1;
        self.mark_recent(page_id);
        Some(page)
    }

    pub(super) fn insert(&mut self, page_id: u64, page: BTreePage) {
        let replaced_existing = self.pages.insert(page_id, page).is_some();

        if !replaced_existing {
            while self.pages.len() > MAX_CACHE_SIZE {
                let Some(oldest_id) = self.access_order.pop_front() else {
                    break;
                };
                if oldest_id != page_id {
                    self.pages.remove(&oldest_id);
                }
            }
        }

        self.mark_recent(page_id);
    }

    fn mark_recent(&mut self, page_id: u64) {
        if self.access_order.back().copied() == Some(page_id) {
            return;
        }

        self.access_order.retain(|&id| id != page_id);
        self.access_order.push_back(page_id);
    }

    pub(super) fn clear(&mut self) {
        self.pages.clear();
        self.access_order.clear();
    }

    pub(super) fn stats(&self) -> (u64, u64, usize) {
        (self.hits, self.misses, self.pages.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::page::PageKind;

    #[test]
    fn get_cloned_tracks_hits_and_refreshes_recency() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));
        cache.insert(2, BTreePage::new(2, PageKind::Leaf));

        let page = cache.get_cloned(1).expect("page should be cached");

        assert_eq!(page.header.page_id, 1);
        assert_eq!(cache.stats(), (1, 0, 2));
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn get_cloned_keeps_current_most_recent_page_in_place() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));
        cache.insert(2, BTreePage::new(2, PageKind::Leaf));

        let page = cache.get_cloned(2).expect("page should be cached");

        assert_eq!(page.header.page_id, 2);
        assert_eq!(cache.stats(), (1, 0, 2));
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn get_cloned_tracks_misses_without_changing_cache_contents() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));

        assert!(cache.get_cloned(2).is_none());

        assert_eq!(cache.stats(), (0, 1, 1));
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn insert_replaces_cached_page_and_refreshes_recency() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));
        cache.insert(2, BTreePage::new(2, PageKind::Leaf));
        cache.insert(1, BTreePage::new(1, PageKind::Internal));

        let page = cache.get_cloned(1).expect("replacement should be cached");

        assert_eq!(page.header.page_id, 1);
        assert_eq!(page.header.kind, PageKind::Internal);
        assert_eq!(cache.stats(), (1, 0, 2));
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn insert_keeps_current_most_recent_replacement_in_place() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));
        cache.insert(2, BTreePage::new(2, PageKind::Leaf));

        cache.insert(2, BTreePage::new(2, PageKind::Internal));

        let page = cache.pages.get(&2).expect("replacement should be cached");
        assert_eq!(page.header.kind, PageKind::Internal);
        assert_eq!(cache.stats(), (0, 0, 2));
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn insert_evicts_oldest_page_after_new_write() {
        let mut cache = PageCache::new();
        for page_id in 0..MAX_CACHE_SIZE as u64 {
            cache.insert(page_id, BTreePage::new(page_id, PageKind::Leaf));
        }

        cache.get_cloned(0).expect("page should be cached");
        cache.insert(
            MAX_CACHE_SIZE as u64,
            BTreePage::new(MAX_CACHE_SIZE as u64, PageKind::Leaf),
        );

        assert!(cache.pages.contains_key(&0));
        assert!(!cache.pages.contains_key(&1));
        assert!(cache.pages.contains_key(&(MAX_CACHE_SIZE as u64)));
        assert_eq!(cache.stats(), (1, 0, MAX_CACHE_SIZE));
    }
}
