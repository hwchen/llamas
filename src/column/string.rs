//! String dtype column
//!
//! Backed by llamas categorical, an array of categorical objects
//! (represented as strings). As can be seen by the name of the crate
//! the StringColumn type is expected to work well with information
//! of low cardinality (where there are fewer options for values).
//!
//! This works well with how strings are often used in dataframes: either
//! as a categorical type, or an almost-categorical type (string to be split
//! into categorical types).
//!
//! Would this work ok with streaming from disk? (since it's not just a
//! straightforward list of elements).

use std::convert::From;
use std::ops::Index;
use std::str;
use std::string::String;

use super::{Column, NumericColumn};

#[derive(Debug)]
pub struct StringArray {
    indices: Vec<usize>,
    offsets: Vec<usize>,
    data: Vec<u8>,
    mask: BitVec,
}

impl StringArray {
    pub fn new() -> Self {
        StringArray {
            indices: Vec::new(),
            offsets: vec![0],
            data: Vec::new(),
            mask: BitVec::new(),
        }
    }

    /// Takes a reference to a string because:
    /// - if string already exists in array, don't need
    ///   to copy.
    /// - Array should stay contiguous, so need to make
    ///   a copy of arg into array anyways.
    pub fn push(&mut self, s: &str) {
        let indices_len = self.indices.len();
        self.insert(indices_len, s);
    }

    pub fn push_null(&mut self) {
        self.indices.push(0);
        self.mask.push(false);
    }

    // insert should only insert into indices,
    // but append to data
    // What is the use case for this?
    // TODO double-check how push and insert are implemented in Vec
    /// Should panic if out of bounds
    pub fn insert(&mut self, index: usize, s: &str) {
        // push shouldn't happen that often, except
        // when initially building column.

        // No matter whether s already exists in data,
        // mask gets one more true value
        // TODO: bug! mask should not just be a push here.
        self.mask.push(true);

        let bytes = s.as_bytes();

        // First check if s already exists in data.
        if let Some(ptr_to_offset) = self.contains_str_bytes(bytes) {
            // only has to add a reference to the offset
            self.indices.insert(index, ptr_to_offset);
            return;
        }

        // Now we know that s doesn't already exist
        // in data, so need to add to data and update
        // indices, mask, etc. accordingly

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
    /// This method is private, public one matches again &str
    fn contains_str_bytes(&self, bytes: &[u8]) -> Option<usize> {
        // doesn't use iterator, because
        // iterator is over &str, and
        // this is over bytes
        let slice_indices = self.offsets.windows(2);

        for (i, range) in slice_indices.enumerate() {
            if *bytes == self.data[range[0]..range[1]] {
                return Some(i);
            }
        }
        None
    }

    pub fn contains(&self, s: &str) -> bool {
        // Don't want to use String::contains
        // because we only want to check
        // at each offset, not entire string array.
        match self.contains_str_bytes(s.as_bytes()) {
            Some(_) => true,
            _ => false,
        }
    }

    pub fn get(&self, i: usize) -> Option<&str> {
        if i < self.indices.len() {
            let offset_ptr = self.indices[i];
            let offset_range = self.offsets[offset_ptr]..self.offsets[offset_ptr + 1];

            // unwrap here because we put in correct utf8,
            // this must also output correct utf8
            Some(str::from_utf8(&self.data[offset_range]).unwrap())
        } else {
            None
        }
    }

    /// Should panic if out of bounds, just like Vec::remove()
    pub fn remove(&mut self, index: usize) -> String {
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
        // TODO fix mask insert/remove
        //self.mask.remove(index);

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

            String::from_utf8(res_bytes.collect::<Vec<u8>>()).unwrap()

        } else {
            // We don't need to do anything if there's still an
            // offset_ptr, except return str.
            String::from_utf8(self.data[offset_range].to_vec()).unwrap()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    //pub fn split_off(&mut self, at: usize) -> Self {
    //}

    // retain (filter)?
    // pop?
    // append?
    // clear?
    // len!
    // is_empty
    //
}

// don't implement Index.
// Can only use Get
// The problem is that [] dereferences
// the &str to str.
impl Index<usize> for StringArray {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        let ptr_to_offset = self.indices[i];
        let offset_range = self.offsets[ptr_to_offset]..self.offsets[ptr_to_offset + 1];

        // unwrap here because we put in correct utf8,
        // this must also output correct utf8
        str::from_utf8(&self.data[offset_range]).unwrap()
    }
}

//impl Iterator
//impl Display

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_and_insert() {
        let mut sa = StringArray::new();
        sa.push("one");
        sa.push("two");
        sa.push("three");
        sa.push("one");
        assert_eq!(&sa[0], "one");
        assert_eq!(&sa[3], "one");
        assert_eq!(&sa[1], "two");
        assert_eq!(&sa[2], "three");
        assert_eq!(sa.get(0), Some("one"));
        assert_eq!(sa.get(3), Some("one"));
        assert_eq!(sa.get(1), Some("two"));
        assert_eq!(sa.get(2), Some("three"));
        assert_eq!(sa.get(4), None);

        // insert middle
        sa.insert(1, "ten");
        // insert end
        sa.insert(5, "twenty");
        assert_eq!(sa.get(1), Some("ten"));
        assert_eq!(sa.get(5), Some("twenty"));
        assert_eq!(sa.get(6), None);

        // insert front
        sa.insert(0, "test");
        assert_eq!(sa.get(0), Some("test"));
        assert_eq!(sa.get(1), Some("one"));
        assert_eq!(sa.get(2), Some("ten"));
        assert_eq!(sa.get(3), Some("two"));
        assert_eq!(sa.get(4), Some("three"));
        assert_eq!(sa.get(5), Some("one"));
        assert_eq!(sa.get(6), Some("twenty"));
        assert_eq!(sa.get(7), None);
    }

    #[test]
    #[should_panic]
    fn insert_panic() {
        let mut sa = StringArray::new();
        sa.push("one");
        sa.insert(5, "twenty");
    }

    #[test]
    fn remove() {
        let mut sa = StringArray::new();
        sa.push("one");
        sa.push("two");
        sa.push("three");
        sa.push("one");
        sa.push("five");

        // removing the last of a value
        let removed = sa.remove(1);
        assert_eq!(removed, "two".to_owned());
        assert_eq!(sa.get(0), Some("one"));
        assert_eq!(sa.get(1), Some("three"));
        assert_eq!(sa.get(2), Some("one"));
        assert_eq!(sa.get(3), Some("five"));

        // removing a value which still exists
        // at another index
        let removed = sa.remove(2);
        assert_eq!(removed, "one".to_owned());
        assert_eq!(sa.get(0), Some("one"));
        assert_eq!(sa.get(1), Some("three"));
        assert_eq!(sa.get(2), Some("five"));

        // removing first
        let removed = sa.remove(0);
        assert_eq!(removed, "one".to_owned());
        assert_eq!(sa.get(0), Some("three"));
        assert_eq!(sa.get(1), Some("five"));

        // removing last
        let removed = sa.remove(1);
        assert_eq!(removed, "five".to_owned());
        assert_eq!(sa.get(0), Some("three"));

        // removing very last
        let removed = sa.remove(0);
        assert_eq!(removed, "three".to_owned());
        assert!(sa.is_empty());
        assert!(sa.indices.is_empty());
        assert!(sa.offsets.len() == 1);
        assert!(sa.data.is_empty());
        // TODO fix mask problems
        //assert!(sa.mask.is_empty());
    }
}
