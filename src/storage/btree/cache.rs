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

    pub(super) fn get(&mut self, page_id: &u64) -> Option<&BTreePage> {
        if let Some(page) = self.pages.get(page_id) {
            self.hits += 1;
            self.access_order.retain(|&id| id != *page_id);
            self.access_order.push_back(*page_id);
            Some(page)
        } else {
            self.misses += 1;
            None
        }
    }

    pub(super) fn insert(&mut self, page_id: u64, page: BTreePage) {
        if self.pages.contains_key(&page_id) {
            self.pages.insert(page_id, page);
            self.access_order.retain(|&id| id != page_id);
            self.access_order.push_back(page_id);
        } else {
            while self.pages.len() >= MAX_CACHE_SIZE {
                if let Some(oldest_id) = self.access_order.pop_front() {
                    self.pages.remove(&oldest_id);
                } else {
                    break;
                }
            }
            self.pages.insert(page_id, page);
            self.access_order.push_back(page_id);
        }
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
    use super::super::page::PageKind;
    use super::*;

    #[test]
    fn cache_hit_refreshes_lru_order_without_changing_size() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));
        cache.insert(2, BTreePage::new(2, PageKind::Leaf));

        let page = cache.get(&1).expect("page should be cached");

        assert_eq!(page.header.page_id, 1);
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(cache.stats(), (1, 0, 2));
    }

    #[test]
    fn cache_miss_updates_stats_without_lru_change() {
        let mut cache = PageCache::new();
        cache.insert(1, BTreePage::new(1, PageKind::Leaf));

        assert!(cache.get(&2).is_none());
        assert_eq!(
            cache.access_order.iter().copied().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(cache.stats(), (0, 1, 1));
    }
}
