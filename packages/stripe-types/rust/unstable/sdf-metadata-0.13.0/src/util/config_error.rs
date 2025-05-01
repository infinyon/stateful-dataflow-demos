pub(crate) const INDENT: &str = "    ";

pub trait ConfigError {
    fn readable(&self, indents: usize) -> String;
}
