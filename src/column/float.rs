use rayon::prelude::*;
use std::convert::From;

use super::{Column, Numeric};

#[derive(Debug)]
pub struct Float32Column(Vec<f32>);

impl Float32Column {
    pub fn new(v: Vec<f32>) -> Self {
        Float32Column(v)
    }
}

impl Column for Float32Column {
    type BaseType = f32;

    fn dtype() -> String { "float32".to_owned() }

    fn apply<F: Fn(f32) -> f32>(&mut self, f: F)
        where F: ::std::marker::Sync {
        self.0.par_iter_mut().for_each(|x| *x = f(*x));
    }
}

impl Numeric for Float32Column {
    fn sum(&self) -> f32 {
        self.0.par_iter().cloned().sum()
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
        let mut col = Float32Column::new(vec![1.0,2.,3.,4.,5.,6.]);
        col.apply(|x| x*x);
        let res = vec![1.0,4.,9.,16.,25.,36.];
        assert_eq!(col.0, res);
    }

    #[test]
    fn impl_numeric_column_for_float() {
        let col = Float32Column::new(vec![1.0,2.,3.,4.,5.,6.]);
        let sum = col.sum();
        assert!(float_nearly_equal(sum, 21.0));
    }
}
