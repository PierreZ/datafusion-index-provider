//! Query optimization utilities for index operations.
//!
//! This module provides functions to optimize expressions for index lookups,
//! such as combining multiple range conditions into a single BETWEEN expression.

use datafusion::{
    logical_expr::{Between, Operator},
    prelude::Expr,
};
use datafusion_common::{Column, ScalarValue, Spans};

/// Helper function to try combining multiple expressions on a column into a BETWEEN expression.
/// This is useful for optimizing multiple range conditions on the same column into a single
/// BETWEEN expression that can be handled by an index scan.
///
/// # Arguments
/// * `exprs` - A slice of expressions to try combining
/// * `column_name` - The name of the column to look for in the expressions
///
/// # Returns
/// * `Some(Vec<Expr>)` - If expressions were successfully combined, returns a new list of expressions
/// * `None` - If expressions could not be combined (e.g., incompatible operators or values)
///
/// # Example
/// ```ignore
/// let exprs = vec![col("age").gt(lit(20)), col("age").lt(lit(30))];
/// let combined = try_combine_exprs_to_between(&exprs, "age");
/// // Returns a BETWEEN expression: age BETWEEN 21 AND 29
/// ```
pub fn try_combine_exprs_to_between(exprs: &[&Expr], column_name: &str) -> Option<Vec<Expr>> {
    let mut lower_bound = None;
    let mut upper_bound = None;
    let mut other_exprs = Vec::new();
    let mut relation_name = None;

    for expr in exprs {
        if let Expr::BinaryExpr(binary) = expr {
            if let (Expr::Column(col), _) = (&*binary.left, &*binary.right) {
                if col.name == column_name {
                    relation_name = col.relation.clone();
                    break;
                }
            }
        }
    }

    for expr in exprs {
        if let Expr::BinaryExpr(binary) = expr {
            if let (Expr::Column(col), Expr::Literal(value)) = (&*binary.left, &*binary.right) {
                if col.name == column_name {
                    match binary.op {
                        Operator::GtEq | Operator::Gt => {
                            let mut val = value.clone();
                            if binary.op == Operator::Gt {
                                // For Gt, we need to increment the value
                                match &val {
                                    ScalarValue::Int8(Some(v)) => {
                                        val = ScalarValue::Int8(Some(v + 1))
                                    }
                                    ScalarValue::Int16(Some(v)) => {
                                        val = ScalarValue::Int16(Some(v + 1))
                                    }
                                    ScalarValue::Int32(Some(v)) => {
                                        val = ScalarValue::Int32(Some(v + 1))
                                    }
                                    ScalarValue::Int64(Some(v)) => {
                                        val = ScalarValue::Int64(Some(v + 1))
                                    }
                                    ScalarValue::UInt8(Some(v)) => {
                                        val = ScalarValue::UInt8(Some(v + 1))
                                    }
                                    ScalarValue::UInt16(Some(v)) => {
                                        val = ScalarValue::UInt16(Some(v + 1))
                                    }
                                    ScalarValue::UInt32(Some(v)) => {
                                        val = ScalarValue::UInt32(Some(v + 1))
                                    }
                                    ScalarValue::UInt64(Some(v)) => {
                                        val = ScalarValue::UInt64(Some(v + 1))
                                    }
                                    _ => return None,
                                }
                            }
                            match lower_bound {
                                None => lower_bound = Some(val),
                                Some(ref current) => {
                                    if val.gt(current) {
                                        lower_bound = Some(val);
                                    }
                                }
                            }
                        }
                        Operator::LtEq | Operator::Lt => {
                            let mut val = value.clone();
                            if binary.op == Operator::Lt {
                                match &val {
                                    ScalarValue::Int8(Some(v)) => {
                                        val = ScalarValue::Int8(Some(v - 1))
                                    }
                                    ScalarValue::Int16(Some(v)) => {
                                        val = ScalarValue::Int16(Some(v - 1))
                                    }
                                    ScalarValue::Int32(Some(v)) => {
                                        val = ScalarValue::Int32(Some(v - 1))
                                    }
                                    ScalarValue::Int64(Some(v)) => {
                                        val = ScalarValue::Int64(Some(v - 1))
                                    }
                                    ScalarValue::UInt8(Some(v)) => {
                                        val = ScalarValue::UInt8(Some(v.saturating_sub(1)))
                                    }
                                    ScalarValue::UInt16(Some(v)) => {
                                        val = ScalarValue::UInt16(Some(v.saturating_sub(1)))
                                    }
                                    ScalarValue::UInt32(Some(v)) => {
                                        val = ScalarValue::UInt32(Some(v.saturating_sub(1)))
                                    }
                                    ScalarValue::UInt64(Some(v)) => {
                                        val = ScalarValue::UInt64(Some(v.saturating_sub(1)))
                                    }
                                    _ => return None,
                                }
                            }
                            match upper_bound {
                                None => upper_bound = Some(val),
                                Some(ref current) => {
                                    if val.lt(current) {
                                        upper_bound = Some(val);
                                    }
                                }
                            }
                        }
                        _ => other_exprs.push((*expr).clone()),
                    }
                }
            }
        }
    }

    if let (Some(low), Some(high)) = (lower_bound, upper_bound) {
        let mut optimized = other_exprs;

        optimized.push(Expr::Between(Between {
            negated: false,
            expr: Box::new(Expr::Column(Column {
                relation: relation_name.clone(),
                name: column_name.to_string(),
                spans: Spans::new(),
            })),
            low: Box::new(Expr::Literal(low)),
            high: Box::new(Expr::Literal(high)),
        }));

        log::debug!("Optimized expressions: {:?}", optimized);

        Some(optimized)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::*;

    #[test]
    fn test_try_combine_exprs_to_between_int32() {
        let col_name = "age";
        let gt_expr = col(col_name).gt(lit(10));
        let lt_expr = col(col_name).lt(lit(20));
        let exprs = vec![&gt_expr, &lt_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Expr::Between(_)));

        let gte_expr = col(col_name).gt_eq(lit(10));
        let lte_expr = col(col_name).lt_eq(lit(20));
        let exprs = vec![&gte_expr, &lte_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Expr::Between(_)));
    }

    #[test]
    fn test_try_combine_exprs_to_between_uint8() {
        let col_name = "small_num";
        let gte_expr = col(col_name).gt_eq(lit(ScalarValue::UInt8(Some(5))));
        let lte_expr = col(col_name).lt_eq(lit(ScalarValue::UInt8(Some(10))));
        let exprs = vec![&gte_expr, &lte_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Expr::Between(_)));
    }

    #[test]
    fn test_try_combine_exprs_to_between_edge_cases() {
        let col_name = "value";
        let eq_expr = col(col_name).eq(lit(10));
        let exprs = vec![&eq_expr];
        assert!(try_combine_exprs_to_between(&exprs, col_name).is_none());

        // Test with empty exprs
        let exprs = vec![];
        assert!(try_combine_exprs_to_between(&exprs, col_name).is_none());

        // Test with wrong column name
        let gt_expr = col(col_name).gt(lit(10));
        let lt_expr = col(col_name).lt(lit(20));
        let exprs = vec![&gt_expr, &lt_expr];
        assert!(try_combine_exprs_to_between(&exprs, "wrong_column").is_none());
    }

    #[test]
    fn test_try_combine_exprs_to_between_mixed_operators() {
        let col_name = "value";

        // Test GT and LTE
        let gt_expr = col(col_name).gt(lit(10));
        let lte_expr = col(col_name).lt_eq(lit(20));
        let exprs = vec![&gt_expr, &lte_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Expr::Between(_)));

        // Test GTE and LT
        let gte_expr = col(col_name).gt_eq(lit(10));
        let lt_expr = col(col_name).lt(lit(20));
        let exprs = vec![&gte_expr, &lt_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Expr::Between(_)));
    }

    #[test]
    fn test_try_combine_exprs_to_between_multiple_bounds() {
        let col_name = "value";

        // Test multiple lower bounds
        let gt_expr1 = col(col_name).gt(lit(10));
        let gt_expr2 = col(col_name).gt(lit(15));
        let lt_expr = col(col_name).lt(lit(20));
        let exprs = vec![&gt_expr1, &gt_expr2, &lt_expr];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        if let Expr::Between(between) = &result[0] {
            if let Expr::Literal(ScalarValue::Int32(Some(val))) = *between.low {
                assert_eq!(val, 16); // Should take the highest lower bound (15) + 1 for GT
            } else {
                panic!("Expected Int32 literal");
            }
        }

        // Test multiple upper bounds
        let gt_expr = col(col_name).gt(lit(10));
        let lt_expr1 = col(col_name).lt(lit(20));
        let lt_expr2 = col(col_name).lt(lit(15));
        let exprs = vec![&gt_expr, &lt_expr1, &lt_expr2];
        let result = try_combine_exprs_to_between(&exprs, col_name).unwrap();
        assert_eq!(result.len(), 1);
        if let Expr::Between(between) = &result[0] {
            if let Expr::Literal(ScalarValue::Int32(Some(val))) = *between.high {
                assert_eq!(val, 14); // Should take the lowest upper bound (15) - 1 for LT
            } else {
                panic!("Expected Int32 literal");
            }
        }
    }
}
