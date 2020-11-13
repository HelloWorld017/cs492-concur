#![allow(clippy::mutex_atomic)]
use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        let mut mutex_guard = & self.0;
        if (*mutex_guard).is_null() {
            return false
        }

        let mut node = unsafe { & *(*(*mutex_guard)) };

        if key <= &node.data {
            return key == &node.data
        }

        loop {
            let next_guard = node.next.lock().unwrap();
            self.0 = next_guard;

            if (*self.0).is_null() {
                return false
            }

            let next_node = unsafe { & *(*self.0) };
            if key <= &next_node.data {
                return key == &next_node.data
            }

            node = next_node;
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let mut cursor = Cursor(self.head.lock().unwrap());
        let result = cursor.find(key);

        (result, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let (result, cursor) = self.find(&key);
        result
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let (result, cursor) = self.find(&key);
        if result {
            return Err(key)
        }

        let mut current_guard = cursor.0;

        *current_guard = Node::new(key, *current_guard);
        Ok(())
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let (result, cursor) = self.find(&key);
        if !result {
            return Err(())
        }

        let mut removed_guard = cursor.0;
        let removed_node = unsafe { Box::from_raw(*removed_guard) };

        let next_guard = (*removed_node).next.lock().unwrap();
        let next_node = *next_guard;
        *removed_guard = next_node;

        Ok(removed_node.data)
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>, bool);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()), true)
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        let mutex_guard = match &self.0 {
            None => return None,
            Some(mutex_guard) => {
                mutex_guard
            }
        };

        if (*mutex_guard).is_null() {
            return None
        }

        let node = unsafe { & *(*(*mutex_guard)) };

        // I know this solution is kinda *hacky*,
        // but there was no other option.
        if self.1 {
            self.1 = false;
            return Some(& node.data);
        }

        let next_guard = node.next.lock().unwrap();

        if (*next_guard).is_null() {
            self.0 = None;
            None
        } else {
            let next_node = unsafe { & *(*next_guard) };
            self.0 = Some(next_guard);

            Some(& next_node.data)
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut next_ptr = self.head.get_mut().unwrap();
        let mut node;
        loop {
            if (*next_ptr).is_null() {
                break
            }

            node = unsafe { Box::from_raw(*next_ptr) };
            next_ptr = (*node).next.get_mut().unwrap();
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
