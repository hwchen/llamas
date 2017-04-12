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

//pub use self::float::{Float32Column};
pub use self::int::{Int8Column};
//pub use self::string::{StringColumn};

/// A Column. It's the logical interface to
/// to an array(1D collection, column, logical store) of dtypes.
/// TODO Can I have trait DType that extends Column, where
/// Column is just a bare trait for use as a object trait?
pub trait Column {}

pub trait DataType: Column {
    type Item;

    fn apply<F>(&mut self, f: F) where
        Self: Sized,
        F: Fn(Self::Item) -> Self::Item + ::std::marker::Sync;
}

pub trait Numeric: DataType {
  fn sum(&self) -> Self::Item;
}

pub trait Time: Column {
}

