//! Validated and quoted database identifiers.

use std::iter::Peekable;
use std::str::Chars;

use crate::error::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Table {
    pub database: String,
    pub name: String,
}

impl Table {
    pub(crate) fn parse(text: &str, database: &str) -> Result<Self> {
        let mut chars = text.trim().chars().peekable();
        let first = part(&mut chars)?;
        whitespace(&mut chars);
        let table = match chars.next() {
            None => Self {
                database: database.to_owned(),
                name: first,
            },
            Some('.') => Self {
                database: first,
                name: part(&mut chars)?,
            },
            Some(_) => return Err("table must be an identifier, not a SQL expression".into()),
        };
        whitespace(&mut chars);
        if chars.next().is_some() {
            return Err("table accepts at most database.table".into());
        }
        validate(&table.database)?;
        Ok(table)
    }

    pub(crate) fn sql(&self) -> String {
        format!("{}.{}", quote(&self.database), quote(&self.name))
    }
}

pub(crate) fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(crate) fn validate(name: &str) -> Result<()> {
    if name.is_empty() || name.chars().count() > 64 || name.contains('\0') {
        return Err("identifiers require 1..64 characters without NUL".into());
    }
    Ok(())
}

fn part(chars: &mut Peekable<Chars<'_>>) -> Result<String> {
    whitespace(chars);
    if chars.peek() == Some(&'`') {
        _ = chars.next();
        return quoted(chars);
    }
    let mut name = String::new();
    while let Some(&character) = chars.peek() {
        if !(character.is_alphanumeric() || matches!(character, '_' | '$')) {
            break;
        }
        name.push(character);
        _ = chars.next();
    }
    validate(&name)?;
    Ok(name)
}

fn quoted(chars: &mut Peekable<Chars<'_>>) -> Result<String> {
    let mut name = String::new();
    while let Some(character) = chars.next() {
        if character == '`' {
            if chars.peek() != Some(&'`') {
                validate(&name)?;
                return Ok(name);
            }
            _ = chars.next();
        }
        name.push(character);
    }
    Err("unclosed quoted identifier".into())
}

fn whitespace(chars: &mut Peekable<Chars<'_>>) {
    while chars
        .peek()
        .is_some_and(|character| character.is_whitespace())
    {
        _ = chars.next();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_names_without_interpreting_sql() {
        assert_eq!(
            Table::parse("`a.b` . `c``d`", "unused").unwrap().sql(),
            "`a.b`.`c``d`"
        );
        assert_eq!(
            Table::parse("данные", "база").unwrap().sql(),
            "`база`.`данные`"
        );
        assert!(Table::parse(&"x".repeat(65), "db").is_err());
    }
}
