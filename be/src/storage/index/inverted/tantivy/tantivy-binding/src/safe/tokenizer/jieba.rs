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

//! Jieba dictionary-based Chinese segmentation.
//!
//! Streaming design: reuses a single `Token` buffer across the entire
//! token stream, eliminating per-token `String` allocations. Case conversion
//! is deliberately left to the analyzer pipeline's explicit lowercase filter.
//!
//! Constructed only via the `tokenizer::build` factory; visibility is
//! `pub(super)` so callers cannot bypass the factory.

use std::sync::Arc;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use super::spec::JiebaMode;

pub(super) struct JiebaTokenizer {
    jieba: Arc<jieba_rs::Jieba>,
    token: Token,
    mode: JiebaMode,
    hmm: bool,
}

impl Clone for JiebaTokenizer {
    fn clone(&self) -> Self {
        Self {
            jieba: self.jieba.clone(),
            token: Token::default(),
            mode: self.mode,
            hmm: self.hmm,
        }
    }
}

impl Default for JiebaTokenizer {
    fn default() -> Self {
        Self {
            jieba: Arc::new(jieba_rs::Jieba::new()),
            token: Token::default(),
            mode: JiebaMode::Search,
            hmm: true,
        }
    }
}

impl JiebaTokenizer {
    pub(super) fn new(mode: JiebaMode, hmm: bool) -> Self {
        Self {
            jieba: Arc::new(jieba_rs::Jieba::new()),
            token: Token::default(),
            mode,
            hmm,
        }
    }
}

pub(super) struct JiebaTokenStream<'a> {
    /// Raw jieba output — borrows word slices from the input text.
    words: Vec<jieba_rs::Token<'a>>,
    text: &'a str,
    index: usize,
    position: usize,
    token: &'a mut Token,
}

impl TokenStream for JiebaTokenStream<'_> {
    fn advance(&mut self) -> bool {
        while self.index < self.words.len() {
            let w = &self.words[self.index];
            self.index += 1;

            if w.word.trim().is_empty() {
                continue;
            }

            let byte_start = w.word.as_ptr() as usize - self.text.as_ptr() as usize;
            let byte_end = byte_start + w.word.len();

            // Reuse the token buffer.
            self.token.text.clear();
            self.token.text.push_str(w.word);
            self.token.offset_from = byte_start;
            self.token.offset_to = byte_end;
            self.token.position = self.position;
            self.token.position_length = 1;
            self.position += 1;
            return true;
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

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mode = match self.mode {
            JiebaMode::Search => jieba_rs::TokenizeMode::Search,
            JiebaMode::Default => jieba_rs::TokenizeMode::Default,
        };
        let words = self.jieba.tokenize(text, mode, self.hmm);
        self.token.reset();
        JiebaTokenStream {
            words,
            text,
            index: 0,
            position: 0,
            token: &mut self.token,
        }
    }
}
