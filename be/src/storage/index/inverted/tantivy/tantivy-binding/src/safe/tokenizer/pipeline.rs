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

use std::collections::HashSet;

use tantivy::tokenizer::{
    NgramTokenizer, RawTokenizer, SimpleTokenizer, Token, TokenStream, Tokenizer,
};
use unicode_normalization::{char::canonical_combining_class, UnicodeNormalization};

use super::cjk_bigram::CjkBigramTokenizer;
use super::ik::IkTokenizer;
use super::jieba::JiebaTokenizer;
use super::spec::{
    parse_mapping, AnalyzerSpec, CharFilterSpec, TokenFilterSpec, TokenizerSpec,
    UnicodeNormalizationForm, MAX_INPUT_BYTES, MAX_OUTPUT_TOKENS, MAX_TOKEN_BYTES,
};
use super::standard::StandardTokenizer;
use crate::error::{Result, TantivyBindingError};

#[derive(Clone)]
pub struct PipelineTokenizer {
    spec: AnalyzerSpec,
}

impl PipelineTokenizer {
    pub fn new(spec: AnalyzerSpec) -> Self {
        Self { spec }
    }

    pub fn analyze(&self, text: &str) -> Result<Vec<Token>> {
        if text.len() > MAX_INPUT_BYTES {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "analyzer input exceeds {MAX_INPUT_BYTES} bytes"
            )));
        }
        let filtered = apply_char_filters(text, &self.spec.char_filter)?;
        let mut tokens = tokenize_base(&self.spec.tokenizer, &filtered.text)?;
        for token in &mut tokens {
            token.offset_from = filtered.translate_start(token.offset_from);
            token.offset_to = filtered.translate_end(token.offset_to);
        }
        apply_token_filters(&mut tokens, &self.spec.token_filter);
        if tokens.len() > MAX_OUTPUT_TOKENS {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "analyzer output exceeds {MAX_OUTPUT_TOKENS} tokens"
            )));
        }
        if let Some(token) = tokens
            .iter()
            .find(|token| token.text.len() > MAX_TOKEN_BYTES)
        {
            return Err(TantivyBindingError::InvalidArgument(format!(
                "analyzer token at position {} exceeds {MAX_TOKEN_BYTES} bytes",
                token.position
            )));
        }
        Ok(tokens)
    }
}

impl Tokenizer for PipelineTokenizer {
    type TokenStream<'a> = VecTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        // Tantivy's Tokenizer API cannot return an error. Invalid definitions are
        // rejected before construction; document-size failures yield no tokens and
        // are checked explicitly by the SQL/FFI tokenize entry points.
        VecTokenStream::new(self.analyze(text).unwrap_or_default())
    }
}

pub struct VecTokenStream {
    tokens: Vec<Token>,
    next: usize,
    current: Token,
}

impl VecTokenStream {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            next: 0,
            current: Token::default(),
        }
    }
}

impl TokenStream for VecTokenStream {
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

fn tokenize_base(spec: &TokenizerSpec, text: &str) -> Result<Vec<Token>> {
    match spec {
        // RawTokenizer intentionally emits one token even for an empty input.
        // Tantivy uses that placeholder to preserve document alignment for
        // nullable columns, and the BE removes NULL row ids after querying.
        TokenizerSpec::None => collect_raw(RawTokenizer::default(), text),
        TokenizerSpec::English => collect(SimpleTokenizer::default(), text),
        TokenizerSpec::Standard => collect(StandardTokenizer, text),
        TokenizerSpec::Chinese => collect(CjkBigramTokenizer::default(), text),
        TokenizerSpec::Jieba { mode, hmm } => collect(JiebaTokenizer::new(*mode, *hmm), text),
        TokenizerSpec::Ik { mode } => collect(IkTokenizer::new(*mode), text),
        TokenizerSpec::Ngram { min_gram, max_gram } => {
            let tokenizer = NgramTokenizer::all_ngrams(*min_gram, *max_gram).map_err(|e| {
                TantivyBindingError::InvalidArgument(format!("invalid ngram tokenizer: {e}"))
            })?;
            collect(tokenizer, text)
        }
    }
}

fn collect_raw<T: Tokenizer>(mut tokenizer: T, text: &str) -> Result<Vec<Token>> {
    let mut stream = tokenizer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        tokens.push(stream.token().clone());
    }
    Ok(tokens)
}

fn collect<T: Tokenizer>(mut tokenizer: T, text: &str) -> Result<Vec<Token>> {
    let mut stream = tokenizer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        let token = stream.token();
        if !token.text.trim().is_empty() {
            tokens.push(token.clone());
        }
    }
    Ok(tokens)
}

fn apply_token_filters(tokens: &mut Vec<Token>, filters: &[TokenFilterSpec]) {
    for filter in filters {
        match filter {
            TokenFilterSpec::Lowercase => {
                for token in tokens.iter_mut() {
                    token.text = token.text.to_lowercase();
                }
            }
            TokenFilterSpec::Stop { stopwords } => {
                let stopwords: HashSet<&str> = stopwords.iter().map(String::as_str).collect();
                // Keep original positions. The resulting gap is required for
                // correct phrase-query semantics.
                tokens.retain(|token| !stopwords.contains(token.text.as_str()));
            }
            TokenFilterSpec::Length { min, max } => {
                tokens.retain(|token| {
                    let length = token.text.chars().count();
                    length >= *min && length <= *max
                });
            }
            TokenFilterSpec::RemovePunctuation => {
                tokens.retain(|token| token.text.chars().any(char::is_alphanumeric));
            }
        }
    }
}

struct FilteredText {
    text: String,
    starts: Vec<usize>,
    ends: Vec<usize>,
}

impl FilteredText {
    fn identity(text: &str) -> Self {
        let boundaries: Vec<usize> = (0..=text.len()).collect();
        Self {
            text: text.to_string(),
            starts: boundaries.clone(),
            ends: boundaries,
        }
    }

    fn translate_start(&self, offset: usize) -> usize {
        self.starts
            .get(offset)
            .copied()
            .unwrap_or_else(|| *self.starts.last().unwrap_or(&0))
    }

    fn translate_end(&self, offset: usize) -> usize {
        self.ends
            .get(offset)
            .copied()
            .unwrap_or_else(|| *self.ends.last().unwrap_or(&0))
    }
}

fn apply_char_filters(text: &str, filters: &[CharFilterSpec]) -> Result<FilteredText> {
    let mut current = FilteredText::identity(text);
    for filter in filters {
        current = match filter {
            CharFilterSpec::UnicodeNormalize { form } => normalize(current, *form),
            CharFilterSpec::Mapping { mappings } => map_literals(current, mappings)?,
        };
    }
    Ok(current)
}

fn normalize(input: FilteredText, form: UnicodeNormalizationForm) -> FilteredText {
    let mut output = String::new();
    let mut starts = vec![input.translate_start(0)];
    let mut ends = vec![input.translate_end(0)];
    let mut segments = Vec::new();
    let mut segment_start = 0usize;
    let mut previous = None;
    for (offset, ch) in input.text.char_indices() {
        if offset > segment_start
            && canonical_combining_class(ch) == 0
            && !previous.is_some_and(|value| hangul_may_compose(value, ch, form))
        {
            segments.push((segment_start, offset));
            segment_start = offset;
        }
        previous = Some(ch);
    }
    segments.push((segment_start, input.text.len()));
    for (offset, next) in segments {
        let source = &input.text[offset..next];
        let normalized: String = match form {
            UnicodeNormalizationForm::Nfc => source.nfc().collect(),
            UnicodeNormalizationForm::Nfkc => source.nfkc().collect(),
            UnicodeNormalizationForm::Nfd => source.nfd().collect(),
            UnicodeNormalizationForm::Nfkd => source.nfkd().collect(),
        };
        push_segment(
            &mut output,
            &mut starts,
            &mut ends,
            &normalized,
            input.translate_start(offset),
            input.translate_end(next),
        );
    }
    FilteredText {
        text: output,
        starts,
        ends,
    }
}

fn hangul_may_compose(previous: char, current: char, form: UnicodeNormalizationForm) -> bool {
    let previous = previous as u32;
    let current = current as u32;
    let leading = (0x1100..=0x1112).contains(&previous) || (0xA960..=0xA97C).contains(&previous);
    let vowel = (0x1161..=0x1175).contains(&current) || (0xD7B0..=0xD7C6).contains(&current);
    let previous_vowel =
        (0x1161..=0x1175).contains(&previous) || (0xD7B0..=0xD7C6).contains(&previous);
    let trailing = (0x11A8..=0x11C2).contains(&current) || (0xD7CB..=0xD7FB).contains(&current);
    let precomposed_lv = (0xAC00..=0xD7A3).contains(&previous) && (previous - 0xAC00) % 28 == 0;
    if (leading && vowel) || ((previous_vowel || precomposed_lv) && trailing) {
        return true;
    }

    // Compatibility/halfwidth Jamo can first decompose and then compose under
    // NFKC. Keeping adjacent Jamo in one correction segment preserves that
    // normalization while mapping the result back to the original byte span.
    matches!(form, UnicodeNormalizationForm::Nfkc)
        && ((0x3130..=0x318F).contains(&previous) || (0xFFA0..=0xFFDC).contains(&previous))
        && ((0x3130..=0x318F).contains(&current) || (0xFFA0..=0xFFDC).contains(&current))
}

fn map_literals(input: FilteredText, mappings: &[String]) -> Result<FilteredText> {
    let parsed: Vec<(&str, &str)> = mappings
        .iter()
        .map(|rule| parse_mapping(rule))
        .collect::<Result<_>>()?;
    let mut output = String::new();
    let mut starts = vec![input.translate_start(0)];
    let mut ends = vec![input.translate_end(0)];
    let mut offset = 0usize;
    while offset < input.text.len() {
        if let Some((source, target)) = parsed
            .iter()
            .find(|(source, _)| input.text[offset..].starts_with(*source))
        {
            let next = offset + source.len();
            push_segment(
                &mut output,
                &mut starts,
                &mut ends,
                target,
                input.translate_start(offset),
                input.translate_end(next),
            );
            offset = next;
        } else {
            let ch = input.text[offset..]
                .chars()
                .next()
                .expect("offset is in range");
            let next = offset + ch.len_utf8();
            push_segment(
                &mut output,
                &mut starts,
                &mut ends,
                &input.text[offset..next],
                input.translate_start(offset),
                input.translate_end(next),
            );
            offset = next;
        }
    }
    Ok(FilteredText {
        text: output,
        starts,
        ends,
    })
}

fn push_segment(
    output: &mut String,
    starts: &mut Vec<usize>,
    ends: &mut Vec<usize>,
    segment: &str,
    original_start: usize,
    original_end: usize,
) {
    output.push_str(segment);
    if segment.is_empty() {
        if let Some(last) = starts.last_mut() {
            *last = original_end;
        }
        if let Some(last) = ends.last_mut() {
            *last = original_end;
        }
        return;
    }
    for index in 1..=segment.len() {
        starts.push(if index == segment.len() {
            original_end
        } else {
            original_start
        });
        // An expanded/normalized source span may yield more than one token.
        // Any token ending inside that generated span still consumed the
        // original source span, so its end must never collapse to start.
        ends.push(original_end);
    }
}
