//! Dataframe!
//!
//! Rust implementation of dataframes,
//! based on https://pandas-dev.github.io/pandas2/


// Should I back with ndarray or not? Maybe I don't really need this.
//extern crate ndarray;
//extern crate bit_vec;

//use bit_vec::BitVec;

pub struct Table<C> where C: Column {
    column_index: Vec<usize>,
    columns: Vec<Box<C>>,
}

/// A Column. It's the logical interface to
/// to an array(1D collection, column) of dtypes.
pub trait Column{
    type BaseType;
    //new, new_from, etc...
    // public interface for things like sum
    fn apply(&mut self, Box<Fn(Self::BaseType) -> Self::BaseType>);
    // private interface for things that table manipulation requires?
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
    fn apply(&mut self, f: Box<Fn(f32) -> f32>) {
        //self.0.iter_mut().map(|x| x = f(x));
        for i in 0..self.0.len() {
            let x = self.0[i];
            self.0[i] = f(x);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_test() {
        let mut col = Float32::new(vec![1.0,2.,3.,4.,5.,6.]);
        col.apply(Box::new(|x| x*x));
        println!("{:?}", col);
        assert!(false);
    }
}
