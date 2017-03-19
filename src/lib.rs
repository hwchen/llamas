//! Dataframe!
//!
//! Rust implementation of dataframes,
//! based on https://pandas-dev.github.io/pandas2/

// Should I back with ndarray or not? Maybe I don't really need this.
extern crate bit_vec;
extern crate llamas_categorical;
//extern crate ndarray;
extern crate rayon;

pub mod column;

use column::Column;

pub struct Table<C> where C: Column {
    column_name: Vec<usize>, //keep name and index synced?
    columns: Vec<Box<C>>,
}

