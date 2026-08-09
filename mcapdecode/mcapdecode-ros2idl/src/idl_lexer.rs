//! Tokenization for the ROS 2 IDL grammar.
//!
//! This module owns source positions, whitespace, comments, literal scanning, and
//! operators. Keeping it separate lets `parser` focus solely on grammar productions.

use mcapdecode_ros2_common::Ros2Error;

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub(crate) text: String,
    pub(crate) line: usize,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl Token {
    pub(crate) fn is(&self, text: &str) -> bool {
        self.text == text
    }
}

/// Tokenize an IDL source string, preserving the line and byte span of every token.
pub(crate) fn lex(source: &str) -> Result<Vec<Token>, Ros2Error> {
    let mut cursor = Cursor::new(source);
    let mut tokens = Vec::new();
    while cursor.skip_ignored()? {
        tokens.push(cursor.next_token()?);
    }
    Ok(tokens)
}

struct Cursor<'a> {
    source: &'a str,
    bytes: &'a [u8],
    index: usize,
    line: usize,
}

impl<'a> Cursor<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            index: 0,
            line: 1,
        }
    }

    /// Skip whitespace and comments. Returns whether another token remains.
    fn skip_ignored(&mut self) -> Result<bool, Ros2Error> {
        loop {
            self.skip_whitespace();
            if self.at_end() {
                return Ok(false);
            }
            if self.starts_with(b"//") {
                self.skip_line_comment();
            } else if self.starts_with(b"/*") {
                self.skip_block_comment()?;
            } else {
                return Ok(true);
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, Ros2Error> {
        let start = self.index;
        let line = self.line;
        if !self.current().is_ascii() {
            return Err(format!(
                "parse error at line {line}: non-ASCII character outside a string or comment"
            )
            .into());
        }
        match self.current() {
            b'"' | b'L' if self.current() == b'"' || self.peek() == Some(b'"') => {
                self.scan_string()?;
            }
            byte if is_identifier_start(byte) => self.scan_identifier(),
            byte if byte.is_ascii_digit() => self.scan_number(),
            _ => self.scan_operator(),
        }
        Ok(Token {
            text: self.source[start..self.index].to_string(),
            line,
            start,
            end: self.index,
        })
    }

    fn skip_whitespace(&mut self) {
        while !self.at_end() && self.current().is_ascii_whitespace() {
            self.advance();
        }
    }

    fn skip_line_comment(&mut self) {
        while !self.at_end() && self.current() != b'\n' {
            self.advance();
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), Ros2Error> {
        let start_line = self.line;
        self.index += 2;
        while !self.at_end() && !self.starts_with(b"*/") {
            self.advance();
        }
        if self.at_end() {
            return Err(format!("parse error at line {start_line}: unclosed block comment").into());
        }
        self.index += 2;
        Ok(())
    }

    fn scan_string(&mut self) -> Result<(), Ros2Error> {
        let start_line = self.line;
        if self.current() == b'L' {
            self.advance();
        }
        self.advance(); // opening quote
        let mut escaped = false;
        while !self.at_end() {
            let byte = self.current();
            self.advance();
            if !escaped && byte == b'"' {
                return Ok(());
            }
            escaped = !escaped && byte == b'\\';
        }
        Err(format!("parse error at line {start_line}: unclosed string literal").into())
    }

    fn scan_identifier(&mut self) {
        self.advance();
        while !self.at_end() && is_identifier_continue(self.current()) {
            self.advance();
        }
    }

    fn scan_number(&mut self) {
        self.advance();
        while !self.at_end() && (self.current().is_ascii_alphanumeric() || self.current() == b'.') {
            self.advance();
        }
    }

    fn scan_operator(&mut self) {
        // Keep `>>` as two `>` tokens. It is a shift operator in a constant
        // expression, but it is also two nested template terminators; that syntax
        // cannot be distinguished during lexing. Const values are rebuilt from
        // tokens, preserving adjacent token spelling such as `>>` in the AST.
        const TWO_CHARACTER_OPERATORS: [&[u8]; 6] = [b"::", b"<<", b"<=", b">=", b"==", b"!="];
        if TWO_CHARACTER_OPERATORS
            .iter()
            .any(|operator| self.starts_with(operator))
        {
            self.index += 2;
        } else {
            self.advance();
        }
    }

    fn current(&self) -> u8 {
        self.bytes[self.index]
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.index + 1).copied()
    }

    fn starts_with(&self, prefix: &[u8]) -> bool {
        self.bytes[self.index..].starts_with(prefix)
    }

    fn at_end(&self) -> bool {
        self.index == self.bytes.len()
    }

    fn advance(&mut self) {
        if self.current() == b'\n' {
            self.line += 1;
        }
        self.index += 1;
    }
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}
