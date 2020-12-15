//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};
use rand::seq::index::IndexVec::USize;

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        let max_key = (1usize << SEGMENT_LOGSIZE) - 1;
        let mut stack = vec![];
        let guard = unsafe { unprotected() };
        let root =
            unsafe { self.root.load(Ordering::Relaxed, guard).into_owned() };

        let root_height  = root.tag();
        stack.push((root, root_height));

        while !stack.is_empty() {
            let (mut node, height) = stack.pop().unwrap();

            if height == 1 {
                drop(node);
                continue;
            }

            for x in 0..max_key {
                let ptr = unsafe {
                    let ptr = mem::take(&mut node.inner[x]);
                    Shared::from_usize(ptr.into_inner())
                };

                if !ptr.is_null() {
                    stack.push((unsafe { ptr.into_owned() }, height - 1));
                }
            }

            drop(node);
        }
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

// usize::BITS is nightly-only API
const USIZE_SIZE: usize = mem::size_of::<usize>() * 8;

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        // Ensure tree height
        let (root, root_height) = loop {
            let root = self.root.load(Ordering::Acquire, guard);
            let root_height = root.tag();
            let max_key =
                if root_height > 0
                {
                    let sr_count = std::cmp::min(
                        USIZE_SIZE, SEGMENT_LOGSIZE * root_height
                    );
                    usize::MAX >> USIZE_SIZE - sr_count
                } else
                { 0 };

            if index < max_key {
                break (root, root_height);
            }

            let mut new_node = Segment::new();
            new_node.inner[0] = AtomicUsize::new(root.into_usize());

            let owned_ptr = Owned::new(new_node);

            match self.root.compare_and_set(
                root,
                owned_ptr.with_tag(root_height + 1),
                Ordering::Release,
                guard
            ) {
                Err(err) => {
                    drop(err.new);
                }
                _ => ()
            };

            continue;
        };

        // Find node
        let mut current_height = root_height;
        let mut node: Atomic<Segment> = Atomic::from(root);
        let mask = (1 << SEGMENT_LOGSIZE) - 1;

        loop {
            let current_index = (index >> ((current_height - 1) * SEGMENT_LOGSIZE)) & mask;
            let next_node = unsafe {
                node.load(Ordering::Acquire, guard).deref().get_unchecked(current_index)
            };

            let next_usize = next_node.load(Ordering::Acquire);
            let next_ptr = unsafe { Shared::from_usize(next_usize) };

            if current_height == 1 {
                return unsafe { &*(next_node as *const _ as *const Atomic<T>) };
            }

            if next_ptr.is_null() {
                let mut new_node = Segment::new();
                let owned_ptr = Owned::new(new_node);
                let new_usize = owned_ptr.into_usize();

                if next_node.compare_and_swap(
                    next_usize,
                    new_usize,
                    Ordering::Release
                ) == next_usize {
                    current_height -= 1;
                    node = unsafe { Atomic::from(Shared::from_usize(new_usize)) };
                } else {
                    let owned: Owned<Segment> = unsafe { Owned::from_usize(new_usize) };
                    drop(owned);
                }

                continue;
            }

            current_height -= 1;
            node = Atomic::from(next_ptr);
        }
    }
}
