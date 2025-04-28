use anyhow::Result;

use sdf_parser_core::config::transform::code::Code;

use crate::{
    util::sdf_function_parser::SDFFunctionParser,
    wit::operator::{CodeInfo as CodeInfoWit, StepInvocation as StepInvocationWit},
};

impl TryFrom<Code> for StepInvocationWit {
    type Error = anyhow::Error;

    fn try_from(code: Code) -> Result<Self, Self::Error> {
        let lang = code.lang.into();
        let (uses, inputs, output) = SDFFunctionParser::parse(&lang, &code.run)?;

        Ok(Self {
            uses,
            inputs,
            output,
            states: code.state_imports.into_iter().map(|s| s.into()).collect(),
            imported_function_metadata: None,
            code_info: CodeInfoWit {
                lang,
                code: Some(code.run),
                extra_deps: code.dependencies.into_iter().map(|d| d.into()).collect(),
            },
            ..Default::default()
        })
    }
}
