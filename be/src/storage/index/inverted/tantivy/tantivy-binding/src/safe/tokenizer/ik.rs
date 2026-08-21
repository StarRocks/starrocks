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

//! IK dictionary-based Chinese segmentation.
//!
//! `tantivy-ik` 0.7 implements `tantivy-tokenizer-api` 0.2, while the
//! in-tree Tantivy uses a newer tokenizer API. This module is a small adapter:
//! it delegates segmentation to `tantivy_ik::IkTokenizer`, then converts the
//! returned tokens into the in-tree Tantivy token type.

use ik_rs::core::ik_segmenter::TokenMode;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};
use tantivy_ik::IkTokenizer as TantivyIkTokenizer;
use tantivy_tokenizer_api_v02::{
    TokenStream as TantivyIkTokenStream, Tokenizer as TantivyIkTokenizerApi,
};

#[derive(Clone)]
pub(super) struct IkTokenizer {
    inner: TantivyIkTokenizer,
}

impl IkTokenizer {
    pub(super) fn new(mode: TokenMode) -> Self {
        Self {
            inner: TantivyIkTokenizer::new(mode),
        }
    }
}

impl Default for IkTokenizer {
    fn default() -> Self {
        Self::new(TokenMode::INDEX)
    }
}

pub(super) struct IkTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for IkTokenStream {
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

impl Tokenizer for IkTokenizer {
    type TokenStream<'a> = IkTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut source = TantivyIkTokenizerApi::token_stream(&mut self.inner, text);
        let mut tokens = Vec::new();
        while TantivyIkTokenStream::advance(&mut source) {
            let source_token = TantivyIkTokenStream::token(&source);
            tokens.push(Token {
                offset_from: source_token.offset_from,
                offset_to: source_token.offset_to,
                position: source_token.position,
                text: source_token.text.to_lowercase(),
                position_length: source_token.position_length,
            });
        }
        IkTokenStream { tokens, index: 0 }
    }
}
