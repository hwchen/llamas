//! Dataframe!
//!
//! Rust implementation of dataframes,
//! based on https://pandas-dev.github.io/pandas2/

// First add sum to float
// then add from for float
// then add integer with bitmask
// then add string column

// Should I back with ndarray or not? Maybe I don't really need this.
extern crate bit_vec;
//extern crate ndarray;
extern crate rayon;

use bit_vec::BitVec;
use rayon::prelude::*;
use std::convert::From;

pub struct Table<C> where C: Column {
    column_name: Vec<usize>, //keep name and index synced?
    columns: Vec<Box<C>>,
}

/// A Column. It's the logical interface to
/// to an array(1D collection, column) of dtypes.
pub trait Column {
    type BaseType;
    //new, new_from, etc...
    // public interface for things like sum
    fn apply<F>(&mut self, f: F) where
        Self: Sized,
        F: Fn(Self::BaseType) -> Self::BaseType + std::marker::Sync;
    // private interface for things that table manipulation requires?
}

pub trait NumericColumn: Column {
    fn sum(&self) -> Self::BaseType;
}


/// Dtype describes the logical type, as well
/// as the physical type backing it.
/// All these Dtypes implement the Column trait,
/// to have a consistent interface to the Table.
//enum Dtype {
//    //Float16,
//    Float32(Vec<f32>),
//    Float64(Vec<f64>),
//    Int8,
//    Int8,
//    Int16,
//    Int32,
//    Int64,
//    Boolean,
//    Categorical,
//    String,
//    Binary,
//    Timestamp(Unit),
//    Timedelta(Unit),
//    Period(Unit),
//    Interval(Unit),
//}

#[derive(Debug)]
pub struct Float32(Vec<f32>);

impl Float32 {
    pub fn new(v: Vec<f32>) -> Self {
        Float32(v)
    }
}

impl Column for Float32 {
    type BaseType = f32;

    fn apply<F: Fn(f32) -> f32>(&mut self, f: F)
        where F: std::marker::Sync {
        self.0.par_iter_mut().for_each(|x| *x = f(*x));
    }
}

impl NumericColumn for Float32 {
    fn sum(&self) -> f32 {
        self.0.par_iter().cloned().sum()
    }
}

#[derive(Debug)]
pub struct Int8 {
    values: Vec<u8>,
    // Mask uses a bitvec overlaid onto values to know which indices hold
    // a null value. false in the bitvec maps to null in values.
    mask: BitVec,
}

impl Int8 {
    fn new(values: Vec<u8>, mask: BitVec) -> Self {
        // Where should the check for consistency btwn nulls
        // and values be?
        // Should they always be constructed from something
        // else?
        assert_eq!(values.len(), mask.len());
        Int8 {
            values: values,
            mask: mask,
        }
    }
}

impl Column for Int8 {
    type BaseType = u8;

    fn apply<F>(&mut self, f: F)
        where F: Fn(Self::BaseType) -> Self::BaseType + std::marker::Sync
    {
        // TODO best way to apply mask? zip values, or refer to mask by index?
        let mask = &self.mask;
        self.values
            .par_iter_mut()
            .enumerate()
            .filter(|&(i,_)| mask[i] )
            .for_each(|(_, x)| *x = f(*x));
    }
}

impl NumericColumn for Int8 {
    fn sum(&self) -> Self::BaseType {
        let mask = &self.mask;
        self.values
            .par_iter()
            .enumerate()
            .filter(|&(i,_)| mask[i])
            .map(|(_, x)| x)
            .cloned()
            .sum()
    }
}


#[cfg(test)]
mod tests {
    use std::{f32};
    use super::*;

    fn float_nearly_equal(a: f32, b: f32) -> bool {
        let abs_a = a.abs();
        let abs_b = b.abs();
        let diff = (a - b).abs();

        if a == b { // Handle infinities.
            true
        } else if a == 0.0 || b == 0.0 || diff < f32::MIN_POSITIVE {
            //One of a or b is zero (or both are
            //extremely close to it,) use absolute
            //error.
            diff < (f32::EPSILON * f32::MIN_POSITIVE)
        } else { // Use relative error.
            (diff / f32::min(abs_a + abs_b, f32::MAX)) < f32::EPSILON
        }
    }

    fn compare_float_vec(xs: Vec<f32>, ys: Vec<f32>) -> bool {
        xs.iter().zip(ys.iter()).all(|(&a,&b)| float_nearly_equal(a,b))
    }

    #[test]
    fn impl_column_for_float() {
        let mut col = Float32::new(vec![1.0,2.,3.,4.,5.,6.]);
        col.apply(|x| x*x);
        let res = vec![1.0,4.,9.,16.,25.,36.];
        assert_eq!(col.0, res);
    }

    #[test]
    fn impl_numeric_column_for_float() {
        let col = Float32::new(vec![1.0,2.,3.,4.,5.,6.]);
        let sum = col.sum();
        assert!(float_nearly_equal(sum, 21.0));
    }

    #[test]
    fn impl_column_for_int8() {
        let mut col = Int8::new(vec![1,2,3,4,5,6], BitVec::from_elem(6, true));
        col.apply(|x| x*x);
        let res = vec![1,4,9,16,25,36];
        assert_eq!(col.values, res);
    }

    #[test]
    fn impl_numeric_column_for_int8() {
        let col = Int8::new(vec![1,2,3,4,5,6], BitVec::from_elem(6, true));
        let sum = col.sum();
        assert_eq!(sum, 21);
    }

    #[test]
    fn int8_column_null_test() {
        let mut mask = BitVec::from_elem(6, true);
        mask.set(2, false);
        mask.set(4, false);
        let col = Int8::new(vec![1,2,3,4,5,6], mask);
        let sum = col.sum();
        assert_eq!(sum, 13);
    }
}
