use bit_vec::BitVec;
use rayon::prelude::*;
use std::convert::From;

use super::{Column, DataType, Numeric, Series};

#[derive(Debug, Clone, PartialEq)]
pub struct Int8Column {
    values: Vec<i8>,
    // Mask uses a bitvec overlaid onto values to know which indices hold
    // a null value. false in the bitvec maps to null in values.
    mask: BitVec,
}

impl Column for Int8Column{}

impl Int8Column {
    pub fn new(values: Vec<i8>, mask: BitVec) -> Self {
        // Where should the check for consistency btwn nulls
        // and values be?
        // Should they always be constructed from something
        // else?
        assert_eq!(values.len(), mask.len());
        Int8Column {
            values: values,
            mask: mask,
        }
    }
}


impl DataType for Int8Column {
    type Item = i8;

    fn get(&self, index: usize) -> Option<Option<&i8>> {
        if let Some(mask) = self.mask.get(index) {
            if !mask {
                return Some(None);
            }
        } else {
            return None;
        }
        Some(self.values.get(index))
    }

    fn values(&self) -> Series<Self::Item> {
        Series::new(self)
    }

}

impl<'a> DataType for &'a Int8Column {
    type Item = i8;

    fn get(&self, index: usize) -> Option<Option<&i8>> {
        if let Some(mask) = self.mask.get(index) {
            if !mask {
                return Some(None);
            }
        } else {
            return None;
        }
        Some(self.values.get(index))
    }

    fn values(&self) -> Series<Self::Item> {
        Series::new(self)
    }
}

impl DataTypeMut for Int8Column {
    fn apply<F>(&mut self, f: F)
        where F: Fn(i8) -> i8 + ::std::marker::Sync
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

impl<'a> Numeric for &'a Int8Column {
}

impl<'a> ::std::iter::Sum<i8> for &'a Int8Column {
    fn sum<I>(iter: I) -> Self
        where I: Iterator<Item=i8>
    {
        iter.sum()
    }
}


impl From<Vec<i8>> for Int8Column {
    fn from(v: Vec<i8>) -> Self {
        let length = v.len();
        Int8Column::new(v, BitVec::from_elem(length, true))
    }
}

impl From<Vec<Option<i8>>> for Int8Column {
    fn from(v: Vec<Option<i8>>) -> Self {
        let mask = BitVec::from_fn(v.len(), |i| {
            match v[i] {
                Some(_) => true,
                _ => false,
            }
        });
        let values = v.into_iter().map(|x| {
            match x {
                Some(x) => x,
                _ => 0,
            }
        }).collect();
        Int8Column::new(values, mask)
    }
}

impl<'a> IntoIterator for &'a Int8Column {
    type Item = Option<&'a i8>;
    type IntoIter = Series<'a, i8>;

    fn into_iter(self) -> Self::IntoIter {
        Series::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impl_column_for_int8() {
        let mut col = Int8Column::new(vec![1,2,3,4,5,6], BitVec::from_elem(6, true));
        col.apply(|x| x*x);
        let res = vec![1,4,9,16,25,36];
        assert_eq!(col.values, res);
    }

    #[test]
    fn impl_numeric_column_for_int8() {
        let col = &Int8Column::new(vec![1,2,3,4,5,6], BitVec::from_elem(6, true));
        let sum = col.sum();
        assert_eq!(sum, 21);
    }

    #[test]
    fn int8_column_null_test() {
        let mut mask = BitVec::from_elem(6, true);
        mask.set(2, false);
        mask.set(4, false);
        let col = &Int8Column::new(vec![1,2,3,4,5,6], mask);
        let sum = col.sum();
        assert_eq!(sum, 13);
    }

    #[test]
    fn from_into_int8_column() {
        let from_vec_i8 = Int8Column::from(vec![1,3,5,7,9]);
        assert_eq!(from_vec_i8, Int8Column::new(vec![1,3,5,7,9], BitVec::from_elem(5, true)));
        let from_vec_option_i8 = Int8Column::from(vec![Some(1),None,Some(5),None,None]);
        let res_values = vec![1,0,5,0,0];
        let mut res_mask = BitVec::from_elem(5, false);
        res_mask.set(0, true);
        res_mask.set(2, true);
        assert_eq!(from_vec_option_i8, Int8Column::new(res_values, res_mask));
    }
}
