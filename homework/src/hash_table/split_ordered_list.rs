//! Split-ordered linked list.

use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

//TODO remove where

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> where V: std::fmt::Debug {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> where V: std::fmt::Debug {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> where V: std::fmt::Debug {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;
    const HI_MASK: usize = 0x8000000000000000usize;
    const MASK: usize    = 0x0000FFFFFFFFFFFFusize;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        let reversed_key = index.reverse_bits();
        let bucket_store = self.buckets.get(reversed_key, guard);
        let bucket = bucket_store.load(Ordering::Acquire, guard);

        if !bucket.is_null() {
            return unsafe {
                Cursor::from_raw(
                    bucket_store,
                    bucket.as_raw()
                )
            };
        }

        // Initialize Bucket
        let parent = {
            let mut parent = self.size.load(Ordering::Acquire);
            while {
                parent = parent >> 1;
                parent > index
            } {};
            index - parent
        };
        let mut parent_cursor =
            if parent == 0
            { self.list.head(guard) } else
            { self.lookup_bucket(parent, guard) };

        let mut sentinel_node = Owned::new(
            Node::new(reversed_key, None)
        );

        let inserted_cursor = loop {
            let (found, mut my_cursor) = loop {
                let mut my_cursor = parent_cursor.clone();

                match my_cursor.find_harris(&reversed_key, guard) {
                    Ok(found) => break (found, my_cursor),
                    Err(_) => ()
                }
            };

            if found {
                drop(sentinel_node);
                break my_cursor;
            }

            match my_cursor.insert(sentinel_node, guard) {
                Ok(_) => break my_cursor,
                Err(e) => { sentinel_node = e; }
            };
        };

        match bucket_store.compare_and_set(
            Shared::null(),
            inserted_cursor.curr(),
            Ordering::Release,
            guard
        ) {
            Err(e) => drop(e.new),
            _ => ()
        };

        inserted_cursor
    }

    fn make_content_key(key: &usize) -> usize { (
        key | SplitOrderedList::<V>::HI_MASK

        /*SplitOrderedList::<V>::MASK
            & key
            | SplitOrderedList::<V>::HI_MASK*/
    ).reverse_bits() }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let size = self.size.load(Ordering::Acquire);
        let bucket_key = (key % size);
        let cursor = self.lookup_bucket(bucket_key, guard);

        let content_key = SplitOrderedList::<V>::make_content_key(key);
        loop {
            let mut my_cursor = cursor.clone();
            match my_cursor.find_harris(&content_key, guard) {
                Ok(found) => break (size, found, my_cursor),
                Err(_) => ()
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert_ne!(key.leading_zeros(), 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> where V: std::fmt::Debug {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);

        if found {
            cursor.lookup().unwrap().as_ref()
        } else {
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);

        let content_key = SplitOrderedList::<V>::make_content_key(key);
        let mut node = Owned::new(
            Node::new(content_key, Some(value))
        );

        let size = loop {
            let (size, found, mut cursor) = self.find(key, guard);
            if found {
                let inner = *node.into_box();
                return Err(inner.into_value().unwrap());
            }

            match cursor.insert(node, guard) {
                Ok(_) => break size,
                Err(val) => node = val
            }
        };

        let count = self.count.fetch_add(1, Ordering::Relaxed);
        if count > size * SplitOrderedList::<V>::LOAD_FACTOR {
            self.size.compare_and_swap(size, size * 2, Ordering::Relaxed);
        }

        Ok(())
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_, found, mut cursor) = self.find(key, guard);
        if !found {
            return Err(())
        }

        match cursor.delete(guard) {
            Ok(v) => {
                self.count.fetch_sub(1, Ordering::Relaxed);
                Ok(v.as_ref().unwrap())
            },
            Err(_) => Err(())
        }
    }
}
