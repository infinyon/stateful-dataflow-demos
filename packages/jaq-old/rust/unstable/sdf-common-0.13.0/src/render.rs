use std::{fs::File, io::Write, path::Path, sync::OnceLock};

use anyhow::Result;
use check_keyword::CheckKeyword;
use convert_case::{Boundary, Case, Casing};
use regex::Regex;

static RE: OnceLock<Regex> = OnceLock::new();

fn dash_or_underscore_digit_regex() -> &'static Regex {
    RE.get_or_init(|| Regex::new(r"[-_](\d+)").unwrap())
}

pub fn rust_type_case(value: &str) -> String {
    if ["u8", "u16", "u32", "u64", "f32", "f64", "bool"].contains(&value) {
        return value.to_owned();
    }

    if ["s8", "s16", "s32", "s64"].contains(&value) {
        return value.replace('s', "i");
    }

    if ["f32", "f64"].contains(&value) {
        return value.replace("float", "f");
    }

    value.to_case(Case::Pascal)
}

pub fn rust_name_case(value: &str) -> String {
    let case = if value.is_case(Case::Kebab) {
        Case::Kebab
    } else if value.is_case(Case::UpperKebab) {
        Case::UpperKebab
    } else if value.is_case(Case::Train) {
        Case::Train
    } else if value.is_case(Case::Ada) {
        Case::Ada
    } else if value.is_case(Case::UpperSnake) {
        Case::UpperSnake
    } else if value.is_case(Case::UpperCamel) {
        Case::UpperCamel
    } else if value.is_case(Case::Pascal) {
        Case::Pascal
    } else if value.is_case(Case::Camel) {
        Case::Camel
    } else if value.is_case(Case::Snake) {
        Case::Snake
    } else {
        Case::Kebab
    };

    let value = value
        .from_case(case)
        .without_boundaries(&Boundary::letter_digit())
        .to_case(Case::Snake);

    let value = dash_or_underscore_digit_regex()
        .replace_all(&value, "$1")
        .to_string();

    if value.is_keyword() {
        format!("{}_", value)
    } else {
        value
    }
}

pub fn upper_snake(value: &str) -> String {
    value.to_case(Case::UpperSnake)
}

pub fn wit_name_case(name: &str) -> String {
    let case = if name.is_case(Case::UpperKebab) {
        Case::UpperKebab
    } else if name.is_case(Case::Train) {
        Case::Train
    } else if name.is_case(Case::Ada) {
        Case::Ada
    } else if name.is_case(Case::UpperSnake) {
        Case::UpperSnake
    } else if name.is_case(Case::UpperCamel) {
        Case::UpperCamel
    } else if name.is_case(Case::Pascal) {
        Case::Pascal
    } else if name.is_case(Case::Camel) {
        Case::Camel
    } else if name.is_case(Case::Kebab) {
        Case::Kebab
    } else {
        Case::Snake
    };

    let kebab_case_name = name
        .from_case(case)
        .without_boundaries(&Boundary::letter_digit())
        .to_case(Case::Kebab);

    dash_or_underscore_digit_regex()
        .replace_all(&kebab_case_name, "$1")
        .to_string()
}

pub fn upper_camel_case(value: &str) -> String {
    value.to_case(Case::UpperCamel)
}

pub fn is_wit_type_or_keyword(value: &str) -> bool {
    [
        "bool", "string", "s8", "s16", "s32", "s64", "u8", "u16", "u32", "u64", "f32", "f64",
        "char", "list", "option", "result", "tuple",
    ]
    .contains(&value)
        || is_wit_keyword(value)
}

fn is_wit_keyword(value: &str) -> bool {
    [
        "record",
        "variant",
        "enum",
        "resource",
        "type",
        "world",
        "interface",
        "use",
        "package",
    ]
    .contains(&value)
}

pub fn map_wit_keyword(value: &str) -> String {
    if is_wit_keyword(value) {
        format!("%{}", value)
    } else {
        value.to_owned()
    }
}

pub fn create_sdf_gitignore(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let gitignore = path.join(".gitignore");
    if !gitignore.exists() {
        let mut file = File::create(&gitignore)?;
        file.write_all(b"*\n")?;
        file.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_rust_type_case() {
        assert_eq!(rust_type_case("string"), "String");
        assert_eq!(rust_type_case("bool"), "bool");
        assert_eq!(rust_type_case("s8"), "i8");
        assert_eq!(rust_type_case("s16"), "i16");
        assert_eq!(rust_type_case("s32"), "i32");
        assert_eq!(rust_type_case("s64"), "i64");
        assert_eq!(rust_type_case("u8"), "u8");
        assert_eq!(rust_type_case("u16"), "u16");
        assert_eq!(rust_type_case("u32"), "u32");
        assert_eq!(rust_type_case("u64"), "u64");
        assert_eq!(rust_type_case("f32"), "f32");
        assert_eq!(rust_type_case("f64"), "f64");
        assert_eq!(rust_type_case("my-type"), "MyType");
        assert_eq!(rust_type_case("my-type-2"), "MyType2");
    }

    #[test]
    fn test_rust_case_name() {
        assert_eq!(rust_name_case("MyType"), "my_type");
        assert_eq!(rust_name_case("MyType2"), "my_type2");
        assert_eq!(rust_name_case("my-type"), "my_type");
        assert_eq!(rust_name_case("My_Type"), "my_type");
        assert_eq!(rust_name_case("myType"), "my_type");
        assert_eq!(rust_name_case("type"), "type_");
        assert_eq!(wit_name_case("line0"), "line0");
    }

    #[test]
    fn test_wit_name_case() {
        assert_eq!(wit_name_case("MyType0"), "my-type0");
        assert_eq!(wit_name_case("MyType"), "my-type");
        assert_eq!(wit_name_case("myType0"), "my-type0");
        assert_eq!(wit_name_case("myType"), "my-type");
        assert_eq!(wit_name_case("My-Type"), "my-type");
        assert_eq!(wit_name_case("My_Type"), "my-type");
        assert_eq!(wit_name_case("my_type"), "my-type");
        assert_eq!(wit_name_case("my-type"), "my-type");
        assert_eq!(wit_name_case("my-type0"), "my-type0");
        assert_eq!(wit_name_case("line0"), "line0");
        assert_eq!(wit_name_case("line-0"), "line0");
    }

    #[test]
    fn test_upper_snake() {
        assert_eq!(upper_snake("my_type"), "MY_TYPE");
        assert_eq!(upper_snake("my-type"), "MY_TYPE");
        assert_eq!(upper_snake("my-type0"), "MY_TYPE_0");
    }
}
