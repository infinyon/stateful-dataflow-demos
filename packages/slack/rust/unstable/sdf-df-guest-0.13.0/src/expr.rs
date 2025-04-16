use std::ops::Deref;

use anyhow::{Result, Context};

use crate::{
    bindings::sdf::df::lazy::{ColumnSchema, RowValue},
    wit::types::{
        BinaryExp as WitBinaryExp, Column as WitColumn, ColumnNames, Expr as WitExpr,
        Expressions as WitExpressions, Index as WitIndex, Lit as WitLit, Operation as WitOperation,
        Operator as WitOperator, SortOptions as WitSortOptions,
    },
};
pub use crate::bindings::sdf::df::lazy::DfValue;

/// rust expression for arbitrary exp, this should be converted to wit expression
pub enum Expr {
    Binary(BinaryExp),
    Not(Box<Expr>),
    Column(WitColumn),
    Lit(WitLit),
}

impl Expr {
    pub fn lt_eq(self, right: impl Into<Box<Expr>>) -> Expr {
        binary_expr(self, WitOperator::LtEq, right)
    }

    pub fn eq(self, right: impl Into<Box<Expr>>) -> Expr {
        binary_expr(self, WitOperator::Eq, right)
    }

    pub fn gt(self, right: impl Into<Box<Expr>>) -> Expr {
        binary_expr(self, WitOperator::Gt, right)
    }

    /// convert this into wit expression
    /// this will convert into tree with with index representation so it can be converted to wit
    /// for example, col("a").lt_eq(col("b"))
    /// contains tree with root as binary expression
    /// binary
    ///   operator: lf_eq
    ///   left: expr1
    ///   right: expr2
    /// this will be converted to wit expression
    ///
    /// nodes:  binary_exp, left, right (this is the order of traversal)
    /// then we have tree with index
    /// expr
    ///  binary_exp
    ///    operator: lt_eq
    ///    left: 2,
    ///    right: 2
    pub fn as_wit_expressions(self) -> WitExpressions {
        let mut nodes = NodeList::new();

        self.add_to_nodes(&mut nodes);

        nodes.inner()
    }

    pub fn as_wit_operation(self) -> WitOperation {
        WitOperation::Filter(self.as_wit_expressions())
    }

    pub(crate) fn add_to_nodes(self, nodes: &mut NodeList) -> WitIndex {
        match self {
            Expr::Binary(exp) => exp.add_to_nodes(nodes),
            Expr::Column(col) => nodes.add(WitExpr::Col(col)),
            Expr::Not(expr) => expr.add_to_nodes(nodes),
            Expr::Lit(lit) => nodes.add(WitExpr::Lit(lit)),
        }
    }
}

pub struct BinaryExp {
    left: Box<Expr>,
    right: Box<Expr>,
    op: WitOperator,
}

impl BinaryExp {
    /// convert this into wit expressio and return nodex index
    pub(crate) fn add_to_nodes(self, nodes: &mut NodeList) -> WitIndex {
        let left = self.left.add_to_nodes(nodes);
        let right = self.right.add_to_nodes(nodes);

        nodes.add(WitExpr::Binary(WitBinaryExp {
            left,
            right,
            operator: self.op,
        }))
    }
}

/// maintains list of nodes and current index
pub(crate) struct NodeList(Vec<WitExpr>);

impl NodeList {
    pub(crate) fn new() -> Self {
        Self(vec![])
    }

    /// add node to list and return index of the node
    pub(crate) fn add(&mut self, node: WitExpr) -> WitIndex {
        let index = self.0.len() as WitIndex;
        self.0.push(node);
        index
    }

    pub(crate) fn inner(self) -> WitExpressions {
        self.0
    }
}

// --- helper functions ---
pub fn col(name: impl Into<String>) -> Expr {
    Expr::Column(WitColumn { name: name.into() })
}

pub fn binary_expr(
    left: impl Into<Box<Expr>>,
    op: impl Into<WitOperator>,
    right: impl Into<Box<Expr>>,
) -> Expr {
    Expr::Binary(BinaryExp {
        left: left.into(),
        right: right.into(),
        op: op.into(),
    })
}

pub fn lit(value: impl Into<WitLit>) -> Expr {
    Expr::Lit(value.into())
}

impl From<String> for WitLit {
    fn from(value: String) -> Self {
        WitLit::String(value)
    }
}

impl From<&str> for WitLit {
    fn from(value: &str) -> Self {
        WitLit::String(value.to_owned())
    }
}

impl From<f64> for WitLit {
    fn from(value: f64) -> Self {
        WitLit::Float64(value)
    }
}

impl From<i64> for WitLit {
    fn from(value: i64) -> Self {
        WitLit::I64(value)
    }
}

impl From<bool> for WitLit {
    fn from(value: bool) -> Self {
        WitLit::Bool(value)
    }
}

impl From<i32> for WitLit {
    fn from(value: i32) -> Self {
        WitLit::I32(value)
    }
}

impl From<u32> for WitLit {
    fn from(value: u32) -> Self {
        WitLit::U32(value)
    }
}

impl From<u64> for WitLit {
    fn from(value: u64) -> Self {
        WitLit::U64(value)
    }
}

impl From<i16> for WitLit {
    fn from(value: i16) -> Self {
        WitLit::I16(value)
    }
}

impl From<u16> for WitLit {
    fn from(value: u16) -> Self {
        WitLit::U16(value)
    }
}

impl From<i8> for WitLit {
    fn from(value: i8) -> Self {
        WitLit::I8(value)
    }
}

impl From<u8> for WitLit {
    fn from(value: u8) -> Self {
        WitLit::U8(value)
    }
}

impl WitLit {
    pub fn str(self) -> Result<String> {
        match self {
            WitLit::String(s) => Ok(s),
            _ => Err(anyhow::anyhow!("not a string")),
        }
    }

    pub fn f64(&self) -> Result<f64> {
        match self {
            WitLit::Float64(f) => Ok(*f),
            _ => Err(anyhow::anyhow!("not a f64")),
        }
    }

    pub fn i64(&self) -> Result<i64> {
        match self {
            WitLit::I64(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a i64")),
        }
    }

    pub fn u64(&self) -> Result<u64> {
        match self {
            WitLit::U64(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a u64")),
        }
    }

    pub fn i32(&self) -> Result<i32> {
        match self {
            WitLit::I32(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a i32")),
        }
    }

    pub fn u32(&self) -> Result<u32> {
        match self {
            WitLit::U32(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a u32")),
        }
    }

    pub fn f32(&self) -> Result<f32> {
        match self {
            WitLit::Float32(f) => Ok(*f),
            _ => Err(anyhow::anyhow!("not a f32")),
        }
    }

    pub fn i16(&self) -> Result<i16> {
        match self {
            WitLit::I16(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a i16")),
        }
    }

    pub fn u16(&self) -> Result<u16> {
        match self {
            WitLit::U16(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a u16")),
        }
    }

    pub fn i8(&self) -> Result<i8> {
        match self {
            WitLit::I8(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a i8")),
        }
    }

    pub fn u8(&self) -> Result<u8> {
        match self {
            WitLit::U8(i) => Ok(*i),
            _ => Err(anyhow::anyhow!("not a u8")),
        }
    }

    pub fn bool(&self) -> Result<bool> {
        match self {
            WitLit::Bool(b) => Ok(*b),
            _ => Err(anyhow::anyhow!("not a bool")),
        }
    }
}

fn map_str(e: String) -> anyhow::Error {
    anyhow::anyhow!("{}", e)
}

#[derive(Debug)]
pub struct LazyDf(DfValue);

impl LazyDf {
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        let wit_expr = expr.as_wit_operation();
        let df = self.0.run(&wit_expr).map_err(map_str)?;
        Ok(Self(df))
    }

    pub fn sort(&self, columns: impl Into<ColumnNames>, options: WitSortOptions) -> Result<Self> {
        let df = self.0.sort(&columns.into(), &options).map_err(map_str)?;
        Ok(Self(df))
    }

    pub fn select(&self, columns: impl Into<ColumnNames>) -> Result<Self> {
        let df = self.0.select(&columns.into()).map_err(map_str)?;
        Ok(Self(df))
    }

    pub fn schema(&self, columns: impl Into<ColumnNames>) -> Result<Vec<ColumnSchema>> {
        let columns = columns.into();
        let schema = self.0.schema(&columns).map_err(map_str)?;
        Ok(schema)
    }

    pub fn col(&self, name: &str) -> Result<ColumnSchema> {
        let columns = ColumnNames {
            names: vec![name.to_owned()],
        };
        let mut columns = self.0.schema(&columns).map_err(map_str)?;
        if columns.is_empty() {
            Err(anyhow::anyhow!("column: {} not found", name))
        } else {
            Ok(columns.remove(0))
        }
    }

    pub fn key(&self) -> Result<ColumnSchema> {
        self.col("_key")
    }

    /// return row object
    pub fn rows(&self) -> Result<LazyRow> {
        let row = self.0.rows().map_err(map_str)?;
        Ok(LazyRow { row })
    }

    pub fn sql(&self, query: &str) -> Result<Self> {
        let df = self.0.sql(query).map_err(map_str)?;
        Ok(Self(df))
    }
}

impl From<DfValue> for LazyDf {
    fn from(df: DfValue) -> Self {
        Self(df)
    }
}

impl Deref for LazyDf {
    type Target = DfValue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct LazyRow {
    row: RowValue,
}

impl Deref for LazyRow {
    type Target = RowValue;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl LazyRow {
    pub fn str(&self, col: &ColumnSchema) -> Result<String> {
        self.row.value(col.index).context("no column")?.str()
    }

    pub fn f64(&self, col: &ColumnSchema) -> Result<f64> {
        self.row.value(col.index).context("no column")?.f64()
    }

    pub fn i64(&self, col: &ColumnSchema) -> Result<i64> {
        self.row.value(col.index).context("no column")?.i64()
    }

    pub fn u64(&self, col: &ColumnSchema) -> Result<u64> {
        self.row.value(col.index).context("no column")?.u64()
    }

    pub fn i32(&self, col: &ColumnSchema) -> Result<i32> {
        self.row.value(col.index).context("no column")?.i32()
    }

    pub fn u32(&self, col: &ColumnSchema) -> Result<u32> {
        self.row.value(col.index).context("no column")?.u32()
    }

    pub fn f32(&self, col: &ColumnSchema) -> Result<f32> {
        self.row.value(col.index).context("no column")?.f32()
    }

    pub fn i16(&self, col: &ColumnSchema) -> Result<i16> {
        self.row.value(col.index).context("no column")?.i16()
    }

    pub fn u16(&self, col: &ColumnSchema) -> Result<u16> {
        self.row.value(col.index).context("no column")?.u16()
    }

    pub fn i8(&self, col: &ColumnSchema) -> Result<i8> {
        self.row.value(col.index).context("no column")?.i8()
    }

    pub fn u8(&self, col: &ColumnSchema) -> Result<u8> {
        self.row.value(col.index).context("no column")?.u8()
    }

    pub fn bool(&self, col: &ColumnSchema) -> Result<bool> {
        self.row.value(col.index).context("no column")?.bool()
    }
}

#[allow(clippy::derivable_impls)]
impl Default for WitSortOptions {
    fn default() -> Self {
        Self {
            descending: vec![],
            maintain_order: false,
        }
    }
}

impl WitSortOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_order_descending(mut self, descending: bool) -> Self {
        self.descending = vec![descending];
        self
    }

    pub fn with_maintain_order(mut self, enabled: bool) -> Self {
        self.maintain_order = enabled;
        self
    }
}

impl From<Vec<String>> for ColumnNames {
    fn from(columns: Vec<String>) -> Self {
        Self { names: columns }
    }
}

impl From<Vec<&str>> for ColumnNames {
    fn from(columns: Vec<&str>) -> Self {
        let mut names = vec![];
        for col in columns.iter() {
            names.push(col.to_string());
        }
        Self { names }
    }
}

type OneString = [&'static str; 1];
type TwoString = [&'static str; 2];
type ThreeString = [&'static str; 3];
type FourString = [&'static str; 4];
type FiveString = [&'static str; 5];
type SixString = [&'static str; 6];
type SevenString = [&'static str; 7];
type EightString = [&'static str; 8];
type NineString = [&'static str; 9];
type TenString = [&'static str; 10];
type ElevenString = [&'static str; 11];
type TwelveString = [&'static str; 12];

impl From<OneString> for ColumnNames {
    fn from(columns: OneString) -> Self {
        Self {
            names: vec![columns[0].to_owned()],
        }
    }
}

impl From<TwoString> for ColumnNames {
    fn from(columns: TwoString) -> Self {
        Self {
            names: vec![columns[0].to_owned(), columns[1].to_owned()],
        }
    }
}

impl From<ThreeString> for ColumnNames {
    fn from(columns: ThreeString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
            ],
        }
    }
}

impl From<FourString> for ColumnNames {
    fn from(columns: FourString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
            ],
        }
    }
}

impl From<FiveString> for ColumnNames {
    fn from(columns: FiveString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
            ],
        }
    }
}

impl From<SixString> for ColumnNames {
    fn from(columns: SixString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
            ],
        }
    }
}

impl From<SevenString> for ColumnNames {
    fn from(columns: SevenString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
            ],
        }
    }
}

impl From<EightString> for ColumnNames {
    fn from(columns: EightString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
                columns[7].to_owned(),
            ],
        }
    }
}

impl From<NineString> for ColumnNames {
    fn from(columns: NineString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
                columns[7].to_owned(),
                columns[8].to_owned(),
            ],
        }
    }
}

impl From<TenString> for ColumnNames {
    fn from(columns: TenString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
                columns[7].to_owned(),
                columns[8].to_owned(),
                columns[9].to_owned(),
            ],
        }
    }
}

impl From<ElevenString> for ColumnNames {
    fn from(columns: ElevenString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
                columns[7].to_owned(),
                columns[8].to_owned(),
                columns[9].to_owned(),
                columns[10].to_owned(),
            ],
        }
    }
}

impl From<TwelveString> for ColumnNames {
    fn from(columns: TwelveString) -> Self {
        Self {
            names: vec![
                columns[0].to_owned(),
                columns[1].to_owned(),
                columns[2].to_owned(),
                columns[3].to_owned(),
                columns[4].to_owned(),
                columns[5].to_owned(),
                columns[6].to_owned(),
                columns[7].to_owned(),
                columns[8].to_owned(),
                columns[9].to_owned(),
                columns[10].to_owned(),
                columns[11].to_owned(),
            ],
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    impl WitExpr {
        pub(crate) fn binary(&self) -> Option<&WitBinaryExp> {
            match self {
                Self::Binary(exp) => Some(exp),
                _ => None,
            }
        }

        pub(crate) fn col(&self) -> Option<&WitColumn> {
            match self {
                Self::Col(col) => Some(col),
                _ => None,
            }
        }
    }

    #[test]
    fn test_select() {
        let filter_expr = col("a").lt_eq(col("b"));

        let wit_exprs = filter_expr.as_wit_expressions();
        println!("wit expr: {:#?}", wit_exprs);

        assert_eq!(wit_exprs.len(), 3);
        // first node should be col a
        let first = wit_exprs[0].col().expect("first");
        assert_eq!(first.name, "a");
        let second = wit_exprs[1].col().expect("second");
        assert_eq!(second.name, "b");
        // compound node depends on other nodes so it comes last
        let third = wit_exprs[2].binary().expect("third");
        assert_eq!(third.operator, WitOperator::LtEq);
        assert_eq!(third.left, 0);
        assert_eq!(third.right, 1);

        //println!("first: {:#?}", first);
        //  forget(wit_expr);
        //forget(df_value);
    }
}
