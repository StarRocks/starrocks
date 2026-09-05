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

//! Single factory for legacy tokenizer names and schema-versioned AnalyzerSpec JSON.

mod cjk_bigram;
mod ik;
mod jieba;
pub(crate) mod pipeline;
pub mod spec;
mod standard;

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use sha2::{Digest, Sha256};
use tantivy::tokenizer::{TextAnalyzer, Token};

use crate::error::{Result, TantivyBindingError};
use pipeline::PipelineTokenizer;
use spec::AnalyzerSpec;

pub const TOKENIZER_ENGLISH: &str = "english";
pub const TOKENIZER_JIEBA: &str = "jieba";
pub const TOKENIZER_CJK: &str = "cjk";
pub const TOKENIZER_IK: &str = "ik";
pub const TOKENIZER_IK_SMART: &str = "ik_smart";
pub const TOKENIZER_NGRAM: &str = "ngram";
pub const TOKENIZER_RAW: &str = "raw";
pub const TOKENIZER_STANDARD: &str = "standard";
pub const TOKENIZER_NAME: &str = "sr_default";
const MAX_CACHED_ANALYZERS: usize = 1024;

#[derive(Clone)]
pub struct ResolvedAnalyzer {
    pub analyzer: TextAnalyzer,
    pub canonical_json: String,
    pub digest: String,
    pub(crate) pipeline: PipelineTokenizer,
}

fn cache() -> &'static RwLock<HashMap<String, ResolvedAnalyzer>> {
    static CACHE: OnceLock<RwLock<HashMap<String, ResolvedAnalyzer>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn build(definition_or_legacy_name: &str) -> Result<TextAnalyzer> {
    Ok(resolve(definition_or_legacy_name, None)?.analyzer)
}

pub fn resolve(
    definition_or_legacy_name: &str,
    expected_digest: Option<&str>,
) -> Result<ResolvedAnalyzer> {
    let spec = if definition_or_legacy_name.trim_start().starts_with('{') {
        AnalyzerSpec::parse(definition_or_legacy_name)?
    } else {
        AnalyzerSpec::legacy(definition_or_legacy_name)?
    };
    let canonical_json = spec.canonical_json()?;
    let digest = hex_sha256(canonical_json.as_bytes());
    if let Some(expected) = expected_digest.filter(|value| !value.is_empty()) {
        if expected != digest {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "analyzer digest mismatch: expected {expected}, computed {digest}"
            )));
        }
    }
    if let Some(cached) = cache()
        .read()
        .map_err(|_| TantivyBindingError::Internal("analyzer cache lock poisoned".to_string()))?
        .get(&digest)
        .cloned()
    {
        return Ok(cached);
    }
    let pipeline = PipelineTokenizer::new(spec);
    let resolved = ResolvedAnalyzer {
        analyzer: TextAnalyzer::builder(pipeline.clone()).build(),
        canonical_json,
        digest: digest.clone(),
        pipeline,
    };
    let mut cache = cache()
        .write()
        .map_err(|_| TantivyBindingError::Internal("analyzer cache lock poisoned".to_string()))?;
    if cache.len() >= MAX_CACHED_ANALYZERS {
        if let Some(key) = cache.keys().next().cloned() {
            cache.remove(&key);
        }
    }
    cache.insert(digest, resolved.clone());
    Ok(resolved)
}

pub fn tokenize(definition_or_legacy_name: &str, text: &str) -> Result<Vec<String>> {
    Ok(tokenize_detail(definition_or_legacy_name, text)?
        .into_iter()
        .map(|token| token.text)
        .collect())
}

pub fn tokenize_detail(definition_or_legacy_name: &str, text: &str) -> Result<Vec<Token>> {
    let spec = if definition_or_legacy_name.trim_start().starts_with('{') {
        AnalyzerSpec::parse(definition_or_legacy_name)?
    } else {
        AnalyzerSpec::legacy(definition_or_legacy_name)?
    };
    PipelineTokenizer::new(spec).analyze(text)
}

pub fn canonicalize(definition_or_legacy_name: &str) -> Result<(String, String)> {
    let resolved = resolve(definition_or_legacy_name, None)?;
    Ok((resolved.canonical_json, resolved.digest))
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
