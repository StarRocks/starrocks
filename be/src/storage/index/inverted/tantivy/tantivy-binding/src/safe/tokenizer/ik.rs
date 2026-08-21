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

use ik_rs::core::ik_segmenter::{IKSegmenter, TokenMode};
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use super::spec::IkMode;

#[derive(Clone)]
pub struct IkTokenizer {
    mode: IkMode,
}

impl IkTokenizer {
    pub fn new(mode: IkMode) -> Self {
        Self { mode }
    }
}

pub struct IkTokenStream {
    tokens: Vec<Token>,
    next: usize,
    current: Token,
}

impl Tokenizer for IkTokenizer {
    type TokenStream<'a> = IkTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut boundaries: Vec<usize> = text.char_indices().map(|(offset, _)| offset).collect();
        boundaries.push(text.len());
        let mode = match self.mode {
            IkMode::Search => TokenMode::SEARCH,
            IkMode::Index => TokenMode::INDEX,
        };
        let tokens = IKSegmenter::new()
            .tokenize(text, mode)
            .into_iter()
            .map(|lexeme| Token {
                offset_from: boundaries[lexeme.begin_pos()],
                offset_to: boundaries[lexeme.end_pos()],
                position: lexeme.begin_pos(),
                text: lexeme.lexeme_text().to_string(),
                position_length: lexeme.len(),
            })
            .collect();
        IkTokenStream {
            tokens,
            next: 0,
            current: Token::default(),
        }
    }
}

impl TokenStream for IkTokenStream {
    fn advance(&mut self) -> bool {
        if self.next >= self.tokens.len() {
            return false;
        }
        self.current = self.tokens[self.next].clone();
        self.next += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.current
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.current
    }
}
