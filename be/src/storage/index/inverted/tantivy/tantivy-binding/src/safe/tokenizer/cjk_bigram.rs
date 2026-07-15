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

//! CJK bigram tokenizer (Lucene-style overlapping pairs).
//!
//! Streaming design: reuses a single `Token` buffer across the entire
//! token stream, eliminating per-token heap allocations. ASCII tokens
//! are lowercased inline so the `LowerCaser` filter is not needed.
//!
//! Constructed only via the `tokenizer::build` factory; visibility is
//! `pub(super)` so callers cannot bypass the factory.

use std::iter::Peekable;
use std::str::CharIndices;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

fn is_cjk_char(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'   // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}' // CJK Extension A
        | '\u{20000}'..='\u{2A6DF}' // CJK Extension B
        | '\u{F900}'..='\u{FAFF}' // CJK Compatibility Ideographs
        | '\u{2F800}'..='\u{2FA1F}' // CJK Compatibility Supplement
        | '\u{3040}'..='\u{309F}' // Hiragana
        | '\u{30A0}'..='\u{30FF}' // Katakana
        | '\u{AC00}'..='\u{D7AF}' // Hangul Syllables
        | '\u{1100}'..='\u{11FF}' // Hangul Jamo
    )
}

#[inline]
fn is_word_char(c: char) -> bool {
    !is_cjk_char(c) && (c.is_alphanumeric() || c == '_' || c == '+' || c == '#')
}

#[derive(Clone)]
pub(super) struct CjkBigramTokenizer {
    token: Token,
}

impl Default for CjkBigramTokenizer {
    fn default() -> Self {
        Self {
            token: Token::default(),
        }
    }
}

pub(super) struct CjkBigramTokenStream<'a> {
    text: &'a str,
    chars: Peekable<CharIndices<'a>>,
    token: &'a mut Token,
    position: usize,
    /// Previous CJK char waiting to form the next bigram.
    prev_cjk: Option<(usize, char)>,
    /// Whether `prev_cjk` has already been emitted as part of a bigram.
    prev_emitted: bool,
}

impl<'a> CjkBigramTokenStream<'a> {
    /// Byte offset of the next character in the iterator, or `text.len()`
    /// if exhausted. Used to compute `offset_to` for bigram tokens.
    #[inline]
    fn next_byte_offset(&mut self) -> usize {
        self.chars
            .peek()
            .map(|&(off, _)| off)
            .unwrap_or(self.text.len())
    }

    /// Emit a token whose text is copied from `self.text[from..to]` with
    /// no transformation (used for CJK tokens that need no lowercasing).
    #[inline]
    fn emit_raw(&mut self, from: usize, to: usize) {
        self.token.text.clear();
        self.token.text.push_str(&self.text[from..to]);
        self.token.offset_from = from;
        self.token.offset_to = to;
        self.token.position = self.position;
        self.token.position_length = 1;
        self.position += 1;
    }

    /// Emit an ASCII/Latin word token with inline lowercasing.
    #[inline]
    fn emit_word_lowered(&mut self, from: usize, to: usize) {
        self.token.text.clear();
        for c in self.text[from..to].chars() {
            if c.is_ascii() {
                self.token.text.push(c.to_ascii_lowercase());
            } else {
                // Non-ASCII, non-CJK alphanumeric (e.g. accented Latin):
                // use full Unicode lowercase for correctness.
                for lc in c.to_lowercase() {
                    self.token.text.push(lc);
                }
            }
        }
        self.token.offset_from = from;
        self.token.offset_to = to;
        self.token.position = self.position;
        self.token.position_length = 1;
        self.position += 1;
    }
}

impl TokenStream for CjkBigramTokenStream<'_> {
    fn advance(&mut self) -> bool {
        // --- Phase 1: continue a CJK run from the previous advance() call ---
        if let Some((prev_start, prev_c)) = self.prev_cjk {
            if let Some(&(next_start, next_c)) = self.chars.peek() {
                if is_cjk_char(next_c) {
                    // Consume the next CJK char and emit bigram(prev, next).
                    self.chars.next();
                    let end = self.next_byte_offset();
                    self.emit_raw(prev_start, end);
                    self.prev_cjk = Some((next_start, next_c));
                    self.prev_emitted = true;
                    return true;
                }
            }
            // CJK run ended. Emit prev as a single char only if it was
            // never part of a bigram (run length == 1).
            if !self.prev_emitted {
                let end = prev_start + prev_c.len_utf8();
                self.emit_raw(prev_start, end);
                self.prev_cjk = None;
                self.prev_emitted = false;
                return true;
            }
            // prev was already emitted in a bigram; drop it and scan on.
            self.prev_cjk = None;
            self.prev_emitted = false;
        }

        // --- Phase 2: scan for the next token ---
        while let Some((byte_offset, c)) = self.chars.next() {
            if is_cjk_char(c) {
                // Check if the next char is also CJK → bigram.
                if let Some(&(next_start, next_c)) = self.chars.peek() {
                    if is_cjk_char(next_c) {
                        self.chars.next();
                        let end = self.next_byte_offset();
                        self.emit_raw(byte_offset, end);
                        self.prev_cjk = Some((next_start, next_c));
                        self.prev_emitted = true;
                        return true;
                    }
                }
                // Single CJK char (run of length 1).
                let end = byte_offset + c.len_utf8();
                self.emit_raw(byte_offset, end);
                return true;
            } else if is_word_char(c) {
                // Collect a contiguous ASCII/Latin word.
                let word_start = byte_offset;
                while let Some(&(_next_start, next_c)) = self.chars.peek() {
                    if is_word_char(next_c) {
                        self.chars.next();
                    } else {
                        break;
                    }
                }
                let word_end = self.next_byte_offset();
                self.emit_word_lowered(word_start, word_end);
                return true;
            }
            // Punctuation, whitespace, etc.: skip.
        }
        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

impl Tokenizer for CjkBigramTokenizer {
    type TokenStream<'a> = CjkBigramTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        CjkBigramTokenStream {
            text,
            chars: text.char_indices().peekable(),
            token: &mut self.token,
            position: 0,
            prev_cjk: None,
            prev_emitted: false,
        }
    }
}
