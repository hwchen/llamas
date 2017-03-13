//! Module for different types of columns:
//!
//! In something like pandas, each column has a dtype.
//! In pandas 2.0, the dtype describes only the logical
//! type (the semantics), where the physical type (the backing
//! types, the implementation) are kept separate.
//!
//! In Llamas, I try to keep the same separation of logical
//! and physical types as in the pandas 2.0 design doc.


// [x] First add sum to float
// [x] then add from for int8
// [x] then add integer with bitmask
// [ ] add macro?
// [ ] then add string column

//enum Dtype {
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

mod float;
mod int;

pub use self::float::{Float32};
pub use self::int::{Int8};

/// A Column. It's the logical interface to
/// to an array(1D collection, column, logical store) of dtypes.
pub trait Column {
    type BaseType;

    fn dtype() -> String;

    fn apply<F>(&mut self, f: F) where
        Self: Sized,
        F: Fn(Self::BaseType) -> Self::BaseType + ::std::marker::Sync;
}

pub trait NumericColumn: Column {
    fn sum(&self) -> Self::BaseType;
}

