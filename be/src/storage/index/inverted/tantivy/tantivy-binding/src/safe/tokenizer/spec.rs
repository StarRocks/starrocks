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

//! Schema-versioned, serializable definition of a StarRocks text analyzer.

use serde::{Deserialize, Serialize};

use crate::error::{Result, TantivyBindingError};

pub const SPEC_VERSION: u32 = 1;
pub const RUNTIME_ABI_VERSION: u32 = 1;
pub const BUILTIN_MODEL_VERSION: &str = "starrocks-tantivy-3.5-v1";

pub const MAX_DEFINITION_BYTES: usize = 64 * 1024;
pub const MAX_PIPELINE_COMPONENTS: usize = 16;
pub const MAX_MAPPING_RULES: usize = 256;
pub const MAX_MAPPING_RULE_BYTES: usize = 1024;
pub const MAX_MAPPING_BYTES: usize = 32 * 1024;
pub const MAX_STOPWORDS: usize = 1024;
pub const MAX_STOPWORD_BYTES: usize = 256;
pub const MAX_STOPWORDS_BYTES: usize = 32 * 1024;
pub const MAX_INPUT_BYTES: usize = 1024 * 1024;
pub const MAX_OUTPUT_TOKENS: usize = 1_000_000;
pub const MAX_TOKEN_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnalyzerSpec {
    #[serde(default = "default_spec_version")]
    pub spec_version: u32,
    #[serde(default = "default_runtime_abi_version")]
    pub runtime_abi_version: u32,
    #[serde(default = "default_builtin_model_version")]
    pub builtin_model_version: String,
    #[serde(default)]
    pub char_filter: Vec<CharFilterSpec>,
    pub tokenizer: TokenizerSpec,
    #[serde(default)]
    pub token_filter: Vec<TokenFilterSpec>,
    #[serde(default)]
    pub resource_refs: Vec<ResourceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CharFilterSpec {
    UnicodeNormalize {
        form: UnicodeNormalizationForm,
    },
    #[serde(alias = "char_replace")]
    Mapping {
        mappings: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UnicodeNormalizationForm {
    Nfc,
    Nfkc,
    Nfd,
    Nfkd,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenizerSpec {
    #[serde(alias = "raw")]
    None,
    English,
    Standard,
    #[serde(alias = "cjk")]
    Chinese,
    Jieba {
        #[serde(default)]
        mode: JiebaMode,
        #[serde(default = "default_true")]
        hmm: bool,
    },
    Ik {
        #[serde(default)]
        mode: IkMode,
    },
    Ngram {
        min_gram: usize,
        max_gram: usize,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JiebaMode {
    #[default]
    Search,
    Default,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IkMode {
    #[default]
    #[serde(alias = "ik_smart")]
    Search,
    #[serde(alias = "ik_max_word")]
    Index,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TokenFilterSpec {
    Lowercase,
    Stop {
        stopwords: Vec<String>,
    },
    Length {
        #[serde(default)]
        min: usize,
        #[serde(default = "default_max_token_bytes")]
        max: usize,
    },
    RemovePunctuation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub name: String,
    pub digest: String,
}

fn default_spec_version() -> u32 {
    SPEC_VERSION
}

fn default_runtime_abi_version() -> u32 {
    RUNTIME_ABI_VERSION
}

fn default_builtin_model_version() -> String {
    BUILTIN_MODEL_VERSION.to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_token_bytes() -> usize {
    MAX_TOKEN_BYTES
}

impl AnalyzerSpec {
    pub fn parse(definition: &str) -> Result<Self> {
        if definition.len() > MAX_DEFINITION_BYTES {
            return Err(invalid(format!(
                "analyzer definition exceeds {MAX_DEFINITION_BYTES} bytes"
            )));
        }
        let spec: AnalyzerSpec = serde_json::from_str(definition)
            .map_err(|e| invalid(format!("invalid analyzer definition: {e}")))?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<()> {
        if self.spec_version != SPEC_VERSION {
            return Err(invalid(format!(
                "unsupported analyzer spec_version {}; expected {SPEC_VERSION}",
                self.spec_version
            )));
        }
        if self.runtime_abi_version != RUNTIME_ABI_VERSION {
            return Err(invalid(format!(
                "unsupported analyzer runtime_abi_version {}; expected {RUNTIME_ABI_VERSION}",
                self.runtime_abi_version
            )));
        }
        if self.builtin_model_version != BUILTIN_MODEL_VERSION {
            return Err(invalid(format!(
                "unsupported builtin_model_version '{}'; expected '{BUILTIN_MODEL_VERSION}'",
                self.builtin_model_version
            )));
        }
        if !self.resource_refs.is_empty() {
            return Err(invalid(
                "resource_refs are not supported by the phase-1 analyzer runtime",
            ));
        }
        if self.char_filter.len() + self.token_filter.len() + 1 > MAX_PIPELINE_COMPONENTS {
            return Err(invalid(format!(
                "analyzer pipeline exceeds {MAX_PIPELINE_COMPONENTS} components"
            )));
        }
        for filter in &self.char_filter {
            if let CharFilterSpec::Mapping { mappings } = filter {
                if mappings.len() > MAX_MAPPING_RULES {
                    return Err(invalid(format!(
                        "mapping contains more than {MAX_MAPPING_RULES} rules"
                    )));
                }
                let mut total = 0usize;
                for rule in mappings {
                    if rule.len() > MAX_MAPPING_RULE_BYTES {
                        return Err(invalid(format!(
                            "mapping rule exceeds {MAX_MAPPING_RULE_BYTES} bytes"
                        )));
                    }
                    total += rule.len();
                    parse_mapping(rule)?;
                }
                if total > MAX_MAPPING_BYTES {
                    return Err(invalid(format!(
                        "mapping rules exceed {MAX_MAPPING_BYTES} bytes in total"
                    )));
                }
            }
        }
        if let TokenizerSpec::Ngram { min_gram, max_gram } = self.tokenizer {
            if min_gram == 0 || max_gram < min_gram || max_gram > 32 || max_gram - min_gram > 16 {
                return Err(invalid(
                    "ngram requires 1 <= min_gram <= max_gram <= 32 and max_gram-min_gram <= 16",
                ));
            }
        }
        for filter in &self.token_filter {
            match filter {
                TokenFilterSpec::Stop { stopwords } => {
                    if stopwords.len() > MAX_STOPWORDS {
                        return Err(invalid(format!(
                            "stop filter contains more than {MAX_STOPWORDS} stopwords"
                        )));
                    }
                    let total = stopwords.iter().try_fold(0usize, |total, word| {
                        if word.len() > MAX_STOPWORD_BYTES {
                            Err(invalid(format!(
                                "stopword exceeds {MAX_STOPWORD_BYTES} bytes"
                            )))
                        } else {
                            Ok(total + word.len())
                        }
                    })?;
                    if total > MAX_STOPWORDS_BYTES {
                        return Err(invalid(format!(
                            "stopwords exceed {MAX_STOPWORDS_BYTES} bytes in total"
                        )));
                    }
                }
                TokenFilterSpec::Length { min, max } if max < min || *max > MAX_TOKEN_BYTES => {
                    return Err(invalid(format!(
                        "length filter requires min <= max <= {MAX_TOKEN_BYTES}"
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| TantivyBindingError::Internal(format!("serialize analyzer spec: {e}")))
    }

    pub fn legacy(name: &str) -> Result<Self> {
        let tokenizer = match name {
            "raw" | "none" => TokenizerSpec::None,
            "english" => TokenizerSpec::English,
            "standard" => TokenizerSpec::Standard,
            "cjk" | "chinese" => TokenizerSpec::Chinese,
            "jieba" => TokenizerSpec::Jieba { mode: JiebaMode::Search, hmm: true },
            "ik" | "ik_max_word" => TokenizerSpec::Ik { mode: IkMode::Index },
            "ik_smart" => TokenizerSpec::Ik { mode: IkMode::Search },
            name if name.starts_with("ngram:") => parse_legacy_ngram(name)?,
            other => {
                return Err(invalid(format!(
                    "unsupported tokenizer '{other}'; supported: none/raw, english, standard, chinese/cjk, jieba, ik/ik_smart/ik_max_word, ngram:<min_gram>:<max_gram>"
                )))
            }
        };
        let mut token_filter = Vec::new();
        match name {
            "english" => {
                token_filter.push(TokenFilterSpec::Length { min: 0, max: 40 });
                token_filter.push(TokenFilterSpec::Lowercase);
                token_filter.push(TokenFilterSpec::Stop {
                    stopwords: ENGLISH_STOPWORDS.iter().map(|s| (*s).to_string()).collect(),
                });
            }
            "standard" => {
                token_filter.push(TokenFilterSpec::Lowercase);
                token_filter.push(TokenFilterSpec::Stop {
                    stopwords: ENGLISH_STOPWORDS.iter().map(|s| (*s).to_string()).collect(),
                });
            }
            name if name.starts_with("ngram:") => token_filter.push(TokenFilterSpec::Lowercase),
            "cjk" | "chinese" | "jieba" | "ik" | "ik_smart" | "ik_max_word" => {
                token_filter.push(TokenFilterSpec::Lowercase)
            }
            _ => {}
        }
        Ok(Self {
            spec_version: SPEC_VERSION,
            runtime_abi_version: RUNTIME_ABI_VERSION,
            builtin_model_version: BUILTIN_MODEL_VERSION.to_string(),
            char_filter: Vec::new(),
            tokenizer,
            token_filter,
            resource_refs: Vec::new(),
        })
    }
}

fn parse_legacy_ngram(name: &str) -> Result<TokenizerSpec> {
    let mut parts = name.split(':');
    if parts.next() != Some("ngram") {
        return Err(invalid(format!("invalid ngram tokenizer '{name}'")));
    }
    let min_gram = parse_ngram_size(parts.next(), "min_gram", name)?;
    let max_gram = parse_ngram_size(parts.next(), "max_gram", name)?;
    if parts.next().is_some() || min_gram == 0 || max_gram < min_gram {
        return Err(invalid(format!(
            "invalid ngram tokenizer '{name}'; expected 0 < min_gram <= max_gram"
        )));
    }
    Ok(TokenizerSpec::Ngram { min_gram, max_gram })
}

fn parse_ngram_size(value: Option<&str>, key: &str, name: &str) -> Result<usize> {
    value
        .ok_or_else(|| invalid(format!("invalid ngram tokenizer '{name}'; missing {key}")))?
        .parse::<usize>()
        .map_err(|_| {
            invalid(format!(
                "invalid ngram tokenizer '{name}'; {key} must be a positive integer"
            ))
        })
}

pub fn parse_mapping(rule: &str) -> Result<(&str, &str)> {
    let (source, target) = rule.split_once("=>").ok_or_else(|| {
        invalid(format!(
            "invalid mapping rule '{rule}'; expected 'source => target'"
        ))
    })?;
    let source = source.trim();
    let target = target.trim();
    if source.is_empty() {
        return Err(invalid("mapping source must not be empty"));
    }
    Ok((source, target))
}

fn invalid(message: impl Into<String>) -> TantivyBindingError {
    TantivyBindingError::InvalidArgument(message.into())
}

const ENGLISH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];
