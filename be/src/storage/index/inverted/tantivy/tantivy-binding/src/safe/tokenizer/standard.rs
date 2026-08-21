// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Grammar-based tokenizer compatible with StarRocks' CLucene StandardAnalyzer.
//!
//! CLucene's standard tokenizer recognizes dotted acronyms, hosts, e-mail
//! addresses, company names, apostrophes, numbers, and contiguous CJK text.
//! Tantivy does not ship an equivalent tokenizer, so the grammar is reproduced
//! here instead of aliasing `standard` to Tantivy's `SimpleTokenizer`.

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

const MAX_TOKEN_CHARS: usize = 255;

#[derive(Clone, Default)]
pub(super) struct StandardTokenizer;

pub(super) struct StandardTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for StandardTokenStream {
    fn advance(&mut self) -> bool {
        if self.index >= self.tokens.len() {
            return false;
        }
        self.index += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Tokenizer for StandardTokenizer {
    type TokenStream<'a> = StandardTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        StandardTokenStream {
            tokens: Scanner::new(text).scan(),
            index: 0,
        }
    }
}

struct Scanner<'a> {
    text: &'a str,
    chars: Vec<(usize, char)>,
    cursor: usize,
    tokens: Vec<Token>,
}

impl<'a> Scanner<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            chars: text.char_indices().collect(),
            cursor: 0,
            tokens: Vec::new(),
        }
    }

    fn scan(mut self) -> Vec<Token> {
        while self.cursor < self.chars.len() {
            let c = self.chars[self.cursor].1;
            if is_alpha_or_underscore(c) {
                self.scan_word();
            } else if c.is_ascii_digit() || c == '-' || c == '.' {
                if !self.scan_number() {
                    self.cursor += 1;
                }
            } else {
                self.cursor += 1;
            }
        }
        self.tokens
    }

    fn scan_word(&mut self) {
        let start = self.cursor;
        let mut token_chars = 0;
        while self.cursor < self.chars.len() && is_word_char(self.chars[self.cursor].1) {
            if token_chars == MAX_TOKEN_CHARS {
                // CLucene consumes (and drops) the character that crosses its
                // fixed token buffer before returning the truncated token.
                self.cursor += 1;
                break;
            }
            self.cursor += 1;
            token_chars += 1;
        }
        let base_end = start + token_chars;
        if token_chars == MAX_TOKEN_CHARS {
            self.emit_slice(start, base_end, base_end);
            return;
        }

        if self.cursor >= self.chars.len() {
            self.emit_slice(start, base_end, base_end);
            return;
        }

        match self.chars[self.cursor].1 {
            '\'' => self.scan_apostrophe(start, base_end),
            '&' => self.scan_company(start, base_end),
            '@' => self.scan_email(start),
            '.' => self.scan_dotted(start),
            _ => self.emit_slice(start, base_end, base_end),
        }
    }

    fn scan_apostrophe(&mut self, start: usize, base_end: usize) {
        self.cursor += 1; // apostrophe
        let suffix_start = self.cursor;
        while self.cursor < self.chars.len()
            && self.chars[self.cursor].1.is_alphabetic()
            && self.cursor - start < MAX_TOKEN_CHARS
        {
            self.cursor += 1;
        }

        if self.cursor == suffix_start {
            // A trailing apostrophe is not part of the token.
            self.emit_slice(start, base_end, base_end);
            return;
        }

        let raw_end = self.cursor;
        let raw = self.slice(start, raw_end);
        if ends_with_possessive(raw) {
            let mut normalized = raw.to_owned();
            normalized.truncate(normalized.len() - 2);
            self.emit_text(start, raw_end, normalized);
        } else {
            self.emit_slice(start, raw_end, raw_end);
        }
    }

    fn scan_company(&mut self, start: usize, base_end: usize) {
        self.cursor += 1; // ampersand
        let suffix_start = self.cursor;
        while self.cursor < self.chars.len()
            && is_word_char(self.chars[self.cursor].1)
            && self.cursor - start < MAX_TOKEN_CHARS
        {
            self.cursor += 1;
        }

        if self.cursor == suffix_start {
            self.emit_slice(start, base_end, base_end);
        } else {
            self.emit_slice(start, self.cursor, self.cursor);
        }
    }

    fn scan_email(&mut self, start: usize) {
        self.cursor += 1; // at sign
        self.scan_dotted_tail();
        let end = self.trim_trailing_separator(start, self.cursor);
        self.emit_slice(start, end, end);
    }

    fn scan_dotted(&mut self, start: usize) {
        self.cursor += 1; // first dot
        self.scan_dotted_tail();

        // CLucene permits an at sign after a dotted local part.
        if self.cursor < self.chars.len() && self.chars[self.cursor].1 == '@' {
            self.cursor += 1;
            self.scan_dotted_tail();
            let end = self.trim_trailing_separator(start, self.cursor);
            self.emit_slice(start, end, end);
            return;
        }

        let raw_end = self.cursor;
        let raw = self.slice(start, raw_end);
        if is_acronym(raw) {
            self.emit_text(start, raw_end, raw.chars().filter(|&c| c != '.').collect());
        } else {
            let end = self.trim_trailing_separator(start, raw_end);
            self.emit_slice(start, end, end);
        }
    }

    fn scan_dotted_tail(&mut self) {
        let mut previous_separator = self
            .cursor
            .checked_sub(1)
            .and_then(|index| self.chars.get(index))
            .is_some_and(|(_, c)| is_dot_or_dash(*c));

        while self.cursor < self.chars.len() {
            let c = self.chars[self.cursor].1;
            if !is_word_char(c) && !is_dot_or_dash(c) {
                break;
            }
            let separator = is_dot_or_dash(c);
            if separator && previous_separator {
                break;
            }
            self.cursor += 1;
            previous_separator = separator;
        }
    }

    fn scan_number(&mut self) -> bool {
        let start = self.cursor;
        let first = self.chars[self.cursor].1;

        if first == '-' || first == '.' {
            if self.cursor + 1 >= self.chars.len()
                || !self.chars[self.cursor + 1].1.is_ascii_digit()
            {
                return false;
            }
            self.cursor += 1;
        }

        let mut has_digit = false;
        while self.cursor < self.chars.len()
            && self.chars[self.cursor].1.is_ascii_digit()
            && self.cursor - start < MAX_TOKEN_CHARS
        {
            self.cursor += 1;
            has_digit = true;
        }
        if !has_digit {
            self.cursor = start;
            return false;
        }

        while self.cursor + 1 < self.chars.len()
            && self.chars[self.cursor].1 == '.'
            && self.chars[self.cursor + 1].1.is_ascii_digit()
            && self.cursor - start < MAX_TOKEN_CHARS
        {
            self.cursor += 1; // dot
            while self.cursor < self.chars.len()
                && self.chars[self.cursor].1.is_ascii_digit()
                && self.cursor - start < MAX_TOKEN_CHARS
            {
                self.cursor += 1;
            }
        }

        self.emit_slice(start, self.cursor, self.cursor);
        true
    }

    fn trim_trailing_separator(&self, start: usize, mut end: usize) -> usize {
        while end > start && is_dot_or_dash(self.chars[end - 1].1) {
            end -= 1;
        }
        end
    }

    fn slice(&self, start: usize, end: usize) -> &'a str {
        &self.text[self.byte_offset(start)..self.byte_offset(end)]
    }

    fn byte_offset(&self, index: usize) -> usize {
        self.chars
            .get(index)
            .map(|(offset, _)| *offset)
            .unwrap_or(self.text.len())
    }

    fn emit_slice(&mut self, start: usize, term_end: usize, offset_end: usize) {
        self.emit_text(start, offset_end, self.slice(start, term_end).to_owned());
    }

    fn emit_text(&mut self, start: usize, offset_end: usize, text: String) {
        if text.is_empty() {
            return;
        }
        self.tokens.push(Token {
            offset_from: self.byte_offset(start),
            offset_to: self.byte_offset(offset_end),
            position: self.tokens.len(),
            text,
            position_length: 1,
        });
    }
}

#[inline]
fn is_alpha_or_underscore(c: char) -> bool {
    c.is_alphabetic() || c == '_' || is_cjk_char(c)
}

#[inline]
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[inline]
fn is_dot_or_dash(c: char) -> bool {
    c == '.' || c == '-'
}

fn is_cjk_char(c: char) -> bool {
    matches!(c,
        '\u{3040}'..='\u{318F}'
        | '\u{3300}'..='\u{337F}'
        | '\u{3400}'..='\u{3D2D}'
        | '\u{4E00}'..='\u{9FFF}'
        | '\u{F900}'..='\u{FAFF}'
        | '\u{AC00}'..='\u{D7AF}'
    )
}

fn ends_with_possessive(text: &str) -> bool {
    text.len() >= 2
        && text.as_bytes()[text.len() - 2] == b'\''
        && matches!(text.as_bytes()[text.len() - 1], b's' | b'S')
}

fn is_acronym(text: &str) -> bool {
    let chars: Vec<char> = text.chars().collect();
    chars.len() >= 2
        && chars.len() % 2 == 0
        && chars.iter().enumerate().all(|(index, c)| {
            if index % 2 == 0 {
                c.is_alphabetic()
            } else {
                *c == '.'
            }
        })
}
