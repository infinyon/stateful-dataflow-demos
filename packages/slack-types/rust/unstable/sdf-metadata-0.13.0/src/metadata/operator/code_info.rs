use crate::wit::operator::{CodeInfo, CodeLang};

#[allow(clippy::derivable_impls)]
impl Default for CodeInfo {
    fn default() -> Self {
        CodeInfo {
            code: None,
            lang: CodeLang::Rust,
            extra_deps: Vec::new(),
        }
    }
}
