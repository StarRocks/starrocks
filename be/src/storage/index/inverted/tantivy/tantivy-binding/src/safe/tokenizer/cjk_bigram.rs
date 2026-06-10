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
//! Constructed only via the `tokenizer::build` factory; visibility is
//! `pub(super)` so callers cannot bypass the LowerCaser filter applied
//! at factory time.

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

#[derive(Clone, Default)]
pub(super) struct CjkBigramTokenizer;

pub(super) struct CjkBigramTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl CjkBigramTokenStream {
    fn build(text: &str) -> Self {
        let mut tokens = Vec::new();
        let mut position = 0usize;

        let chars: Vec<(usize, char)> = text.char_indices().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            let (byte_offset, c) = chars[i];

            if is_cjk_char(c) {
                // CJK bigram: overlapping pairs
                let cjk_start = i;
                // Collect contiguous CJK run
                let mut cjk_end = i + 1;
                while cjk_end < len && is_cjk_char(chars[cjk_end].1) {
                    cjk_end += 1;
                }

                let run_len = cjk_end - cjk_start;
                if run_len == 1 {
                    // Single CJK char: emit as-is
                    let byte_start = chars[cjk_start].0;
                    let byte_end = if cjk_start + 1 < len {
                        chars[cjk_start + 1].0
                    } else {
                        text.len()
                    };
                    tokens.push(Token {
                        offset_from: byte_start,
                        offset_to: byte_end,
                        position,
                        text: text[byte_start..byte_end].to_owned(),
                        position_length: 1,
                    });
                    position += 1;
                } else {
                    // Emit overlapping bigrams
                    for j in cjk_start..cjk_end - 1 {
                        let byte_start = chars[j].0;
                        let byte_end = if j + 2 < len {
                            chars[j + 2].0
                        } else {
                            text.len()
                        };
                        tokens.push(Token {
                            offset_from: byte_start,
                            offset_to: byte_end,
                            position,
                            text: text[byte_start..byte_end].to_owned(),
                            position_length: 1,
                        });
                        position += 1;
                    }
                }
                i = cjk_end;
            } else if c.is_alphanumeric() || c == '_' || c == '+' || c == '#' {
                // ASCII/Latin word: collect contiguous alphanumeric (but not CJK)
                let word_start = byte_offset;
                i += 1;
                while i < len {
                    let ch = chars[i].1;
                    if !is_cjk_char(ch) && (ch.is_alphanumeric() || ch == '_' || ch == '+' || ch == '#') {
                        i += 1;
                    } else {
                        break;
                    }
                }
                let word_end = if i < len { chars[i].0 } else { text.len() };
                tokens.push(Token {
                    offset_from: word_start,
                    offset_to: word_end,
                    position,
                    text: text[word_start..word_end].to_owned(),
                    position_length: 1,
                });
                position += 1;
            } else {
                // Punctuation, whitespace, other: skip
                i += 1;
            }
        }

        CjkBigramTokenStream { tokens, index: 0 }
    }
}

impl TokenStream for CjkBigramTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Tokenizer for CjkBigramTokenizer {
    type TokenStream<'a> = CjkBigramTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CjkBigramTokenStream::build(text)
    }
}
