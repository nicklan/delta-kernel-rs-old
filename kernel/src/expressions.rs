use arrow_array::{
    array::PrimitiveArray, types::{Int32Type, Int64Type}, Array, BooleanArray, Datum, RecordBatch, StructArray,
};
use arrow_ord::cmp::{gt, gt_eq, lt, lt_eq};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    sync::Arc,
};

use arrow_schema::ArrowError;

use self::scalars::Scalar;

pub mod scalars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    // Logical
    And,
    Or,
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    // Comparison
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::LessThan => write!(f, "<"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThan => write!(f, ">"),
            Self::GreaterThanOrEqual => write!(f, ">="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    IsNull,
}

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name.
    Column(String),
    BinaryOperation {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOperation {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

pub type MetadataFilterResult = Result<BooleanArray, ArrowError>;
pub type MetadataFilterResultFnOnce = Box<dyn FnOnce() -> MetadataFilterResult>;

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column(name) => write!(f, "Column({})", name),
            Self::BinaryOperation { op, left, right } => {
                match op {
                    // OR requires parentheses
                    BinaryOperator::Or => write!(f, "({} OR {})", left, right),
                    _ => write!(f, "{} {} {}", left, op, right),
                }
            }
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {}", expr),
                UnaryOperator::IsNull => write!(f, "{} IS NULL", expr),
            },
        }
    }
}


impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&str> {
        let mut set = HashSet::new();

        for expr in self.walk() {
            if let Self::Column(name) = expr {
                set.insert(name.as_str());
            }
        }

        set
    }

    /// Create an new expression for a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    fn binary_op_impl(self, other: Self, op: BinaryOperator) -> Self {
        Self::BinaryOperation {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Equal)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::NotEqual)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThan)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThan)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThanOrEqual)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThanOrEqual)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::And)
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Or)
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::BinaryOperation { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::UnaryOperation { expr, .. } => {
                    stack.push(expr);
                }
            }
            Some(expr)
        })
    }

    fn cast_column<'a, ArrayType: 'static>(name: &str, column: Option<&'a Arc<dyn Array>>)
                                           -> Result<&'a ArrayType, ArrowError> {
        column
            .ok_or(ArrowError::SchemaError(format!("No such column: {}", name)))?
            .as_any()
            .downcast_ref::<ArrayType>()
            .ok_or(ArrowError::SchemaError(format!("{} is not {}", name, std::any::type_name::<ArrayType>())))
    }

    /// Converts this predicate to a lazy data skipping predicate over the given record batch.
    ///
    /// The ultimate result is a The laziness is implemented using closures: For each node type, we
    /// return a MetadataFilterResultFnOnce, which when invoked will evaluate the data skipping
    /// expression to produce a BooleanArray. The evaluation is recursive, invoking the closures of
    /// non-leaf children. Leaf children (column and literal) are extracted directly by the
    /// operators that expect them. Because the evaluation could fail, it actually produces a
    /// Result. Because a given expression may not be convertible to a data skipping predicate, this
    /// method returns an Option.
    // TODO: Given that this is anyway returning closures, rework the closures to take the stats
    // record batch as an argument so we can extract filters only once and apply them to every batch
    // in turn. We also need to decide which error conditions should surface as query errors,
    // vs. merely invalidating the skipping predicate. However, this may first require figuring out
    // why the closures are currently FnOnce instead of Fn (unless it's because of the reference to
    // columns from the stats record batch, in which case the problem might sort itself out).
    pub fn extract_metadata_filters(
        &self,
        stats: &RecordBatch,
    ) -> Option<MetadataFilterResultFnOnce> {
        let get_stats_col = |stat_name: &str, nested_names: &[&str], col_name: &str|
                                                              -> Result<Arc<dyn Array>, ArrowError> {
            let mut col = Self::cast_column::<StructArray>(&stat_name, stats.column_by_name(&stat_name));
            for col_name in nested_names {
                col = Self::cast_column::<StructArray>(&col_name, col?.column_by_name(&col_name));
            }
            col?.column_by_name(&col_name)
                .ok_or(ArrowError::SchemaError(format!("No such column: {}", col_name)))
                .map(|col| col.clone())
        };

        let extract_column = |stat_name: &str, expr: &Expression| -> Option<Result<Arc<dyn Array>, ArrowError>> {
            match expr {
                // TODO: split names like a.b.c into [a, b], c below
                Expression::Column(name) => Some(get_stats_col(&stat_name, &[], name)),
                _ => None,
            }
        };
        let extract_literal = |expr: &Expression| -> Option<Box<dyn Datum>> {
            match expr {
                Expression::Literal(Scalar::Long(v)) =>
                    Some(Box::new(PrimitiveArray::<Int64Type>::new_scalar(*v))),
                Expression::Literal(Scalar::Integer(v)) =>
                    Some(Box::new(PrimitiveArray::<Int32Type>::new_scalar(*v))),
                _ => None
            }
        };
        match self {
            // <expr> AND <expr>
            Expression::BinaryOperation { op: BinaryOperator::And, left, right } => {
                let left = left.extract_metadata_filters(&stats);
                let right = right.extract_metadata_filters(&stats);
                // If one leg of the AND is missing, it just degenerates to the other leg.
                match (left, right) {
                    (Some(left), Some(right)) => {
                        let f = move || -> MetadataFilterResult {
                            arrow_arith::boolean::and(&left()?, &right()?)
                        };
                        Some(Box::new(f))
                    }
                    (left, right) => left.or(right),
                }
            }

            // <expr> OR <expr>
            Expression::BinaryOperation { op: BinaryOperator::Or, left, right } => {
                let left = left.extract_metadata_filters(&stats);
                let right = right.extract_metadata_filters(&stats);
                // OR is valid only if both legs are valid.
                left.zip(right).map(|(left, right)| -> MetadataFilterResultFnOnce {
                    let f = || -> MetadataFilterResult {
                        arrow_arith::boolean::or(&left()?, &right()?)
                    };
                    Box::new(f)
                })
            }

            // col <compare> value
            Expression::BinaryOperation { op, left, right } => {
                let min_column = extract_column("minValues", left.as_ref());
                let max_column = extract_column("maxValues", left.as_ref());
                let literal = extract_literal(right.as_ref());
                let (op, column): (fn(&dyn Datum, &dyn Datum) -> MetadataFilterResult, _) = match op {
                    BinaryOperator::LessThan => (lt, min_column),
                    BinaryOperator::LessThanOrEqual => (lt_eq, min_column),
                    BinaryOperator::GreaterThan => (gt, max_column),
                    BinaryOperator::GreaterThanOrEqual => (gt_eq, max_column),
                    BinaryOperator::Equal => {
                        // Equality filter compares the literal against both min and max stat columns
                        println!("Got an equality filter");
                        return min_column.zip(max_column).zip(literal)
                            .map(|((min_col, max_col), literal)| -> MetadataFilterResultFnOnce {
                                // TODO: Why does this move min_col and max_col out of their
                                // environment, and why does that force the resulting function to be
                                // FnOnce? And what does Box::new have to do with any of this?
                                let f = move || -> MetadataFilterResult {
                                    arrow_arith::boolean::and(
                                        &lt_eq(&min_col?, literal.as_ref())?,
                                        &lt_eq(literal.as_ref(), &max_col?)?)
                                };
                                Box::new(f)
                            });
                    }
                    _ => return None, // Incompatible operator
                };
                column.zip(literal).map(|(col, lit)| ->MetadataFilterResultFnOnce {
                    let f = move || -> MetadataFilterResult { op(&col?, lit.as_ref()) };
                    Box::new(f)
                })
            }
            _ => None,
        }
    }
}

// impl Expression {
//     fn to_arrow(&self, stats: &StructArray) -> Result<BooleanArray, ArrowError> {
//         match self {
//             Expression::LessThan(left, right) => {
//                 lt_scalar(left.to_arrow(stats), right.to_arrow(stats))
//             }
//             Expression::Column(c) => todo!(),
//             Expression::Literal(l) => todo!(),
//         }
//     }
// }

// transform data predicate into metadata predicate
// WHERE x < 10 -> min(x) < 10
// fn construct_metadata_filters(e: Expression) -> Expression {
//     match e {
//         // col < value
//         Expression::LessThan(left, right) => {
//             match (*left, *right.clone()) {
//                 (Expression::Column(name), Expression::Literal(_)) => {
//                     // column_min < value
//                     Expression::LessThan(Box::new(min_stat_col(name)), right)
//                 }
//                 _ => todo!(),
//             }
//         }
//         _ => todo!(),
//     }
// }

// fn min_stat_col(col_name: Vec<String>) -> Expression {
//     stat_col("min", col_name)
// }
//
// fn stat_col(stat: &str, name: Vec<String>) -> Expression {
//     let mut v = vec![stat.to_owned()];
//     v.extend(name);
//     Expression::Column(v)
// }

impl std::ops::Add<Expression> for Expression {
    type Output = Self;

    fn add(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Plus)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = Self;

    fn sub(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Minus)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = Self;

    fn mul(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Multiply)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = Self;

    fn div(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Divide)
    }
}

#[cfg(test)]
mod tests {
    use super::Expression as Expr;

    #[test]
    fn test_expression_format() {
        let col_ref = Expr::column("x");
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (col_ref.clone().eq(Expr::literal(2)), "Column(x) = 2"),
            (
                col_ref
                    .clone()
                    .gt_eq(Expr::literal(2))
                    .and(col_ref.clone().lt_eq(Expr::literal(10))),
                "Column(x) >= 2 AND Column(x) <= 10",
            ),
            (
                col_ref
                    .clone()
                    .gt(Expr::literal(2))
                    .or(col_ref.clone().lt(Expr::literal(10))),
                "(Column(x) > 2 OR Column(x) < 10)",
            ),
            (
                (col_ref.clone() - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                (col_ref.clone() + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (col_ref.eq(Expr::literal("foo")), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
