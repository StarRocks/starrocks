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

//! Tokenizer factory and shared API.
//!
//! Each custom `tantivy::Tokenizer` impl lives in its own submodule
//! (`cjk_bigram`, `ik`, `jieba`). Tantivy's bundled tokenizers (`SimpleTokenizer`,
//! `RawTokenizer`) are assembled inline below — they have no custom code,
//! so a separate file would only add jump cost.
//!
//! All string → `TextAnalyzer` resolution happens in a single match in
//! `build()`; do not duplicate this dispatch elsewhere.

mod cjk_bigram;
mod ik;
mod jieba;

use ik_rs::core::ik_segmenter::TokenMode;
use tantivy::tokenizer::{
    Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer,
    StopWordFilter, TextAnalyzer,
};
// Note: LowerCaser is still imported for english_analyzer().
// CJK and jieba handle lowercasing inline to avoid the overhead of
// Unicode case-mapping on CJK characters that have no case.

use crate::error::{Result, TantivyBindingError};
use cjk_bigram::CjkBigramTokenizer;
use ik::IkTokenizer;
use jieba::JiebaTokenizer;

pub const TOKENIZER_ENGLISH: &str = "english";
pub const TOKENIZER_JIEBA: &str = "jieba";
pub const TOKENIZER_CJK: &str = "cjk";
pub const TOKENIZER_IK: &str = "ik";
pub const TOKENIZER_IK_SMART: &str = "ik_smart";
pub const TOKENIZER_NGRAM: &str = "ngram";
pub const TOKENIZER_RAW: &str = "raw";

pub const TOKENIZER_NAME: &str = "sr_default";

pub fn build(name: &str) -> Result<TextAnalyzer> {
    if name.starts_with(TOKENIZER_NGRAM) {
        return ngram_analyzer(name);
    }

    match name {
        TOKENIZER_ENGLISH => Ok(english_analyzer()),
        TOKENIZER_CJK => Ok(TextAnalyzer::builder(CjkBigramTokenizer::default())
            .build()),
        TOKENIZER_JIEBA => Ok(TextAnalyzer::builder(JiebaTokenizer::default())
            .build()),
        TOKENIZER_IK => Ok(TextAnalyzer::builder(IkTokenizer::default()).build()),
        TOKENIZER_IK_SMART => Ok(TextAnalyzer::builder(IkTokenizer::new(TokenMode::SEARCH))
            .build()),
        TOKENIZER_RAW => Ok(TextAnalyzer::builder(RawTokenizer::default()).build()),
        other => Err(TantivyBindingError::InvalidArgument(format!(
            "unsupported tokenizer '{other}'; supported: '{TOKENIZER_ENGLISH}', '{TOKENIZER_CJK}', '{TOKENIZER_JIEBA}', '{TOKENIZER_IK}', '{TOKENIZER_IK_SMART}', '{TOKENIZER_NGRAM}:<min_gram>:<max_gram>', '{TOKENIZER_RAW}'"
        ))),
    }
}

pub fn tokenize(tokenizer_name: &str, text: &str) -> Result<Vec<String>> {
    let mut analyzer = build(tokenizer_name)?;
    let mut stream = analyzer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        let t = &stream.token().text;
        if !t.trim().is_empty() {
            tokens.push(t.clone());
        }
    }
    Ok(tokens)
}

fn english_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(
            StopWordFilter::new(Language::English)
                .expect("english stopwords are bundled in the tantivy crate"),
        )
        .build()
}

fn ngram_analyzer(name: &str) -> Result<TextAnalyzer> {
    let mut parts = name.split(':');
    if parts.next() != Some(TOKENIZER_NGRAM) {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "invalid ngram tokenizer '{name}'"
        )));
    }
    let min_gram = parse_ngram_size(parts.next(), "min_gram", name)?;
    let max_gram = parse_ngram_size(parts.next(), "max_gram", name)?;
    if parts.next().is_some() {
        return Err(TantivyBindingError::InvalidArgument(format!(
            "invalid ngram tokenizer '{name}'; expected 'ngram:<min_gram>:<max_gram>'"
        )));
    }

    let tokenizer = NgramTokenizer::new(min_gram, max_gram, false).map_err(|err| {
        TantivyBindingError::InvalidArgument(format!("invalid ngram tokenizer '{name}': {err}"))
    })?;
    Ok(TextAnalyzer::builder(tokenizer).filter(LowerCaser).build())
}

fn parse_ngram_size(value: Option<&str>, key: &str, name: &str) -> Result<usize> {
    value
        .ok_or_else(|| {
            TantivyBindingError::InvalidArgument(format!(
                "invalid ngram tokenizer '{name}'; missing {key}"
            ))
        })?
        .parse::<usize>()
        .map_err(|_| {
            TantivyBindingError::InvalidArgument(format!(
                "invalid ngram tokenizer '{name}'; {key} must be a positive integer"
            ))
        })
}
