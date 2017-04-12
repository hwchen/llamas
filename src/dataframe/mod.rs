use bit_vec::BitVec;
use std::any::Any;
use super::column::{Column, DataType};

pub struct DataFrame {
    column_names: Vec<String>, //keep name and index synced?
    columns: Vec<Box<Column>>,
}

impl DataFrame {
    pub fn new() -> Self {
        DataFrame {
            column_names: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column: Box<Column>) {
        self.columns.push(column);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn dataframe_init() {
        let mut df = DataFrame::new();
        df.add_column(Box::new(::column::Int8Column::new(Vec::new(), BitVec::new())));
    }
}
