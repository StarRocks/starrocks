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
//! Constructed only via the `tokenizer::build` factory; visibility is
//! `pub(super)` so callers cannot bypass the LowerCaser filter applied
//! at factory time.

use std::sync::Arc;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

pub(super) struct JiebaTokenizer {
    jieba: Arc<jieba_rs::Jieba>,
}

impl Clone for JiebaTokenizer {
    fn clone(&self) -> Self {
        Self {
            jieba: self.jieba.clone(),
        }
    }
}

impl Default for JiebaTokenizer {
    fn default() -> Self {
        Self {
            jieba: Arc::new(jieba_rs::Jieba::new()),
        }
    }
}

pub(super) struct JiebaTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for JiebaTokenStream {
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

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let words = self.jieba.tokenize(text, jieba_rs::TokenizeMode::Search, true);
        let mut tokens = Vec::with_capacity(words.len());
        let mut position = 0usize;
        for w in &words {
            if w.word.trim().is_empty() {
                continue;
            }
            let byte_start = w.word.as_ptr() as usize - text.as_ptr() as usize;
            let byte_end = byte_start + w.word.len();
            tokens.push(Token {
                offset_from: byte_start,
                offset_to: byte_end,
                position,
                text: w.word.to_owned(),
                position_length: 1,
            });
            position += 1;
        }
        JiebaTokenStream { tokens, index: 0 }
    }
}
