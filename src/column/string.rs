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

use bit_vec::BitVec;
use llamas_categorical::CategoricalVec;
use std::convert::From;
use std::ops::Index;
use std::str;
use std::string::String;

use super::Column;

#[derive(Debug)]
pub struct StringColumn {
    values: CategoricalVec,
    mask: BitVec,
}

impl StringColumn {
    pub fn new() -> Self {
        StringColumn {
            values: CategoricalVec::new(),
            mask: BitVec::new(),
        }
    }

    /// Takes a reference to a string because:
    /// - if string already exists in array, don't need
    ///   to copy.
    /// - Array should stay contiguous, so need to make
    ///   a copy of arg into array anyways.
    pub fn push(&mut self, s: &str) {
        self.values.push(s.as_bytes());
        self.mask.push(true);
    }

    pub fn push_null(&mut self) {
        self.values.push(b"");
        self.mask.push(false);
    }

    pub fn contains(&self, s: &str) -> bool {
        self.values.contains(s.as_bytes())
    }

    pub fn get(&self, i: usize) -> Option<&str> {
        self.values
            .get(i)
            .map(|bytes| str::from_utf8(bytes).unwrap())
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
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
    // pop?
}

// don't implement Index.
// Can only use Get
// The problem is that [] dereferences
// the &str to str.
impl Index<usize> for StringColumn {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        str::from_utf8(&self.values[i]).unwrap()
    }
}

//impl Iterator
//impl Display

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_and_insert() {
        let mut sa = StringColumn::new();
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
    }

    #[test]
    fn pop() {
        let mut sa = StringColumn::new();
        sa.push("one");
        sa.push("two");
        sa.push("three");
        sa.push("one");
        sa.push("five");
    }
}
