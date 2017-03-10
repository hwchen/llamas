//! Dataframe!
//!
//! Rust implementation of dataframes,
//! based on https://pandas-dev.github.io/pandas2/


// Should I back with ndarray or not? Maybe I don't really need this.
//extern crate ndarray;
extern crate bit_vec;

use bit_vec::BitVec;

pub struct Table {
    column_index = Vec<usize>,
    columns: Vec<LmArray>,
}

/// A Llama Array. It's the physical type backing
/// the logical types e.g.
enum Column {
    //Float16(Vec<f16>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8 {
        nulls: BitVec,
        values:Vec<u8>,
    },
    Int8 {
        nulls: BitVec,
        values:Vec<u8>,
    },
    Int16 {
        nulls: BitVec,
        values:Vec<u16>,
    },
    Int32 {
        nulls: BitVec,
        values:Vec<u32>,
    },
    Int64 {
        nulls: BitVec,
        values:Vec<u64>,
    },
    Boolean {
        nulls: BitVec,
        values:Vec<bool>,
    },
    Categorical{
    },
    String {
    },
    Binary {
    },
    Timestamp <Unit>{
        unit: Unit,
        values: Vec<u64>,
    },
    Timedelta <Unit>{
        unit: Unit,
        values: Vec<u64>,
    },
    Period <Unit>{
        unit: Unit,
        values: Vec<u64>,
    },
    Interval <Unit>{
        unit: Unit,
        values: Vec<u64>,
    },

}

#[test]
fn it_works() {
}
