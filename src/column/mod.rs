//! Module for different types of columns:
//!
//! In something like pandas, each column has a dtype.
//! In pandas 2.0, the dtype describes only the logical
//! type (the semantics), where the physical type (the backing
//! types, the implementation) are kept separate.
//!
//! In Llamas, I try to keep the same separation of logical
//! and physical types as in the pandas 2.0 design doc.

// So, maybe that means that I don't do any abstraction or dynamic
// anything. Column is just a bare trait object, everything is
// written specific to struct.
//
// Now I've hacked this... Column as a bare trait. But I have another
// trait DataType for implementing the actual logic (instead of for
// dynamic dispatch). What this means is that there aren't meaningful
// constraints on Column right now. But maybe that's for the best,
// since I may need some flexibility for Columns anyways.
//
// I'll just use the traits more for organizing logic and reducing
// boilerplate, rather than for placing constraints (esp. on user)

//#[derive(Debug, Clone, PartialEq)]
//pub enum Dtype {
//    //Float16,
//    Float32(Vec<f32>),
//    Float64(Vec<f64>),
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

//mod float;
mod int;
//mod string;

use std::ops::Add;
use std::iter::Sum;

//pub use self::float::{Float32Column};
pub use self::int::{Int8Column};
//pub use self::string::{StringColumn};

/// A Column. It's the logical interface to
/// to an array(1D collection, column, logical store) of dtypes.
pub trait Column {}

// This trait should be everything that has to work
// directly with the backing data;
//
// Other stuff, that can use an iterator of Option<> to
// represent null doesn't have to use this interface.
pub trait DataType {
    type Item;

    fn values(&self) -> Series<Self::Item>
        where Self::Item: Clone;

    fn get(&self, index: usize) -> Option<Option<&Self::Item>>;
}

pub trait Numeric: DataType {
    fn sum(&self) -> Self::Item
        where Self::Item : Sum + Clone
    {
        self.values()
            .filter_map(|x| x)
            .cloned()
            .sum()
    }
}

/// For DataType methods that use &mut, which means that they
/// can't be implemented on &Column types, only Column and &mut
/// Column
pub trait DataTypeMut: DataType {
    fn apply<F>(&mut self, f: F) where
        Self: Sized,
        F: Fn(Self::Item) -> Self::Item + ::std::marker::Sync;
}

// TODO make sure Series works for other data types.
/// Iterator for column types.
pub struct Series<'a, T: 'a + Clone> {
    values: &'a DataType<Item=T>,
    index: usize,
}

impl<'a, T> Series<'a, T>
    where T: Clone
{
    pub fn new(values: &'a DataType<Item=T>) -> Self {
        Series {
            values: values,
            index: 0,
        }
    }
}

impl<'a, T> Iterator for Series<'a, T>
    where T: Clone
{
    type Item = Option<&'a T>;

    fn next(&mut self) -> Option<Option<&'a T>> {
        let res = self.values.get(self.index);
        self.index += 1;
        res
    }
}
