//! CategoricalArray
//!
//! In llamas, provides backing for
//! - binary
//! - categorical
//! - string
//! datatype columns.
//!
//! A CategoricalArray stores distinct values of
//! bytestrings/strings in a monolithic array. Indexing
//! is provided by indexes to offsets.
//!
//! See Readme for thoughts on using a monolithic array
//! for backing, as opposed to a HashMap or Rope
//!
//! Does not implement a bitmask, and also implements insert/remove
//! where the logical llamas dtypes do not (because of bitmask). Part
//! of the reason is because nulls are particular to tables/dataframes,
//! and if this crate is separated out, I don't want to have the overhead
//! of an unused bitmask. Also, this is would also be consistent with other
//! columns implementing a mask over another data structure (like a vec
//! or an ndarray).
//!

extern crate rayon;

use rayon::prelude::*;
use std::ops::Index;

#[derive(Debug)]
pub struct CategoricalArray {
    indices: Vec<usize>,
    offsets: Vec<usize>,
    data: Vec<u8>,
}

impl CategoricalArray {
    pub fn new() -> Self {
        CategoricalArray {
            indices: Vec::new(),
            offsets: vec![0],
            data: Vec::new(),
        }
    }

    /// Takes a reference to a string because:
    /// - if string already exists in array, don't need
    ///   to copy.
    /// - Array should stay contiguous, so need to make
    ///   a copy of arg into array anyways.
    pub fn push(&mut self, bytes: &[u8]) {
        let indices_len = self.indices.len();
        self.insert(indices_len, bytes);
    }

    // insert should only insert into indices,
    // but append to data
    // What is the use case for this?
    // TODO double-check how push and insert are implemented in Vec
    /// Should panic if out of bounds
    pub fn insert(&mut self, index: usize, bytes: &[u8]) {
        // push shouldn't happen that often, except
        // when initially building column.

        // First check if bytes already exists in data.
        if let Some(ptr_to_offset) = self.offset_position(bytes) {
            // only has to add a reference to the offset
            self.indices.insert(index, ptr_to_offset);
            return;
        }

        // Now we know that `bytes` doesn't already exist
        // in data, so need to add to data and update
        // indices, etc. accordingly

        // New offset and data append only if s doesn't
        // already exist in data.
        self.offsets.push(self.data.len() + bytes.len());
        // Note: indices will point to the next-to-last
        // offset AFTER offsets are updated.
        self.indices.insert(index, self.offsets.len() - 2);
        self.data.extend_from_slice(bytes);
    }

    /// Looks for str slices in data that match bytes.
    /// Of course, matches at the offsets, not on arbitrary
    /// slices in self.data
    fn offset_position(&self, bytes: &[u8]) -> Option<usize> {
        for (i, offset_range) in self.offsets.windows(2).enumerate() {
            if *bytes == self.data[offset_range[0]..offset_range[1]] {
                return Some(i);
            }
        }
        None
    }

    pub fn contains(&self, bytes: &[u8]) -> bool {
        match self.offset_position(bytes) {
            Some(_) => true,
            _ => false,
        }
    }

    pub fn get(&self, i: usize) -> Option<&[u8]> {
        if i < self.indices.len() {
            let offset_ptr = self.indices[i];
            let offset_range = self.offsets[offset_ptr]..self.offsets[offset_ptr + 1];

            // unwrap here because we put in correct utf8,
            // this must also output correct utf8
            Some(&self.data[offset_range])
        } else {
            None
        }
    }

    /// Should panic if out of bounds, just like Vec::remove()
    pub fn remove(&mut self, index: usize) -> Vec<u8> {
        // Do I need to reference count to collect
        // garbage? offset would hold the rc
        // No, removal of a single row should be relatively
        // rare, so just check all indices to see if
        // they are also referencing the same offset.
        // In this vein, it's fine to just compact the
        // data vec immediately to prevent floating
        // data.
        let offset_ptr = self.indices[index];

        self.indices.remove(index);

        let offset_start = self.offsets[offset_ptr];
        let offset_end = self.offsets[offset_ptr + 1];
        let offset_range = offset_start..offset_end;

        // since there's no more references to that offset,
        // we should delete the data in self.data
        if !self.indices.contains(&offset_ptr) {
            let offset_len = offset_end - offset_start;

            let res_bytes = self.data.drain(offset_range);

            // need to fix all the offsets.
            // Just need to remove offset at offset_ptr + 1
            self.offsets.remove(offset_ptr + 1);
            self.offsets[offset_ptr + 1..]
                .par_iter_mut()
                .for_each(|x| *x -= offset_len);

            // and then dont forget that some of the offset_ptr now all
            // need to be moved one to the left
            // note that -= 1 is ok, because offset_ptr will always be > 0
            // in the below calculation
            self.indices
                .par_iter_mut()
                .for_each(|p| if *p > offset_ptr { *p -= 1});

            res_bytes.collect::<Vec<u8>>()

        } else {
            // We don't need to do anything if there's still an
            // offset_ptr, except return str.
            self.data[offset_range].to_vec()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    //pub fn split_off(&mut self, at: usize) -> Self {
    //}

    // retain (filter)?
    // pop?
    // append?
    // clear?
    // len!
    //
}

// don't implement Index.
// Can only use Get
// The problem is that [] dereferences
// the &str to str.
impl Index<usize> for CategoricalArray {
    type Output = [u8];

    fn index(&self, i: usize) -> &[u8] {
        let ptr_to_offset = self.indices[i];
        let offset_range = self.offsets[ptr_to_offset]..self.offsets[ptr_to_offset + 1];

        // unwrap here because we put in correct utf8,
        // this must also output correct utf8
        &self.data[offset_range]
    }
}

//impl Iterator
//impl Display

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_and_insert() {
        let mut sa = CategoricalArray::new();
        sa.push(b"one");
        sa.push(b"two");
        sa.push(b"three");
        sa.push(b"one");
        assert_eq!(&sa[0], &b"one"[..]);
        assert_eq!(&sa[3], &b"one"[..]);
        assert_eq!(&sa[1], &b"two"[..]);
        assert_eq!(&sa[2], &b"three"[..]);
        assert_eq!(sa.get(0), Some(&b"one"[..]));
        assert_eq!(sa.get(3), Some(&b"one"[..]));
        assert_eq!(sa.get(1), Some(&b"two"[..]));
        assert_eq!(sa.get(2), Some(&b"three"[..]));
        assert_eq!(sa.get(4), None);
        assert_eq!(sa.len(), 4);

        // insert middle
        sa.insert(1, b"ten");
        // insert end
        sa.insert(5, b"twenty");
        assert_eq!(sa.get(1), Some(&b"ten"[..]));
        assert_eq!(sa.get(5), Some(&b"twenty"[..]));
        assert_eq!(sa.get(6), None);

        // insert front
        sa.insert(0, b"test");
        assert_eq!(sa.get(0), Some(&b"test"[..]));
        assert_eq!(sa.get(1), Some(&b"one"[..]));
        assert_eq!(sa.get(2), Some(&b"ten"[..]));
        assert_eq!(sa.get(3), Some(&b"two"[..]));
        assert_eq!(sa.get(4), Some(&b"three"[..]));
        assert_eq!(sa.get(5), Some(&b"one"[..]));
        assert_eq!(sa.get(6), Some(&b"twenty"[..]));
        assert_eq!(sa.get(7), None);
        assert_eq!(sa.len(), 7);
    }

    #[test]
    #[should_panic]
    fn insert_panic() {
        let mut sa = CategoricalArray::new();
        sa.push(b"one");
        sa.insert(5, b"twenty");
    }

    #[test]
    fn remove() {
        let mut sa = CategoricalArray::new();
        sa.push(b"one");
        sa.push(b"two");
        sa.push(b"three");
        sa.push(b"one");
        sa.push(b"five");

        // removing the last of a value
        let removed = sa.remove(1);
        assert_eq!(removed, b"two".to_vec());
        assert_eq!(sa.get(0), Some(&b"one"[..]));
        assert_eq!(sa.get(1), Some(&b"three"[..]));
        assert_eq!(sa.get(2), Some(&b"one"[..]));
        assert_eq!(sa.get(3), Some(&b"five"[..]));

        // removing a value which still exists
        // at another index
        let removed = sa.remove(2);
        assert_eq!(removed, b"one".to_vec());
        assert_eq!(sa.get(0), Some(&b"one"[..]));
        assert_eq!(sa.get(1), Some(&b"three"[..]));
        assert_eq!(sa.get(2), Some(&b"five"[..]));

        // removing first
        let removed = sa.remove(0);
        assert_eq!(removed, b"one".to_vec());
        assert_eq!(sa.get(0), Some(&b"three"[..]));
        assert_eq!(sa.get(1), Some(&b"five"[..]));

        // removing last
        let removed = sa.remove(1);
        assert_eq!(removed, b"five".to_vec());
        assert_eq!(sa.get(0), Some(&b"three"[..]));

        // removing very last
        let removed = sa.remove(0);
        assert_eq!(removed, b"three".to_vec());
        assert!(sa.is_empty());
        assert!(sa.indices.is_empty());
        assert!(sa.offsets.len() == 1);
        assert!(sa.data.is_empty());
    }
}
