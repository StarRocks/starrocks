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

use crate::safe::tokenizer::{
    build, tokenize, TOKENIZER_CJK, TOKENIZER_ENGLISH, TOKENIZER_JIEBA, TOKENIZER_RAW,
};

#[test]
fn english_builds() {
    assert!(build(TOKENIZER_ENGLISH).is_ok());
}

#[test]
fn cjk_builds() {
    assert!(build(TOKENIZER_CJK).is_ok());
}

#[test]
fn jieba_builds() {
    assert!(build(TOKENIZER_JIEBA).is_ok());
}

#[test]
fn raw_builds() {
    assert!(build(TOKENIZER_RAW).is_ok());
}

#[test]
fn unsupported_rejected() {
    match build("ik") {
        Ok(_) => panic!("expected error for ik"),
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("unsupported tokenizer"), "got: {msg}");
        }
    }
}

#[test]
fn old_chinese_name_rejected() {
    match build("chinese") {
        Ok(_) => panic!("expected error for old 'chinese' name"),
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("unsupported tokenizer"), "got: {msg}");
        }
    }
}

#[test]
fn raw_no_split() {
    let tokens = tokenize(TOKENIZER_RAW, "hello world").unwrap();
    assert_eq!(tokens, vec!["hello world"]);
}

#[test]
fn english_tokenize() {
    let tokens = tokenize(TOKENIZER_ENGLISH, "The Quick Brown Fox").unwrap();
    assert!(tokens.contains(&"quick".to_string()));
    assert!(tokens.contains(&"brown".to_string()));
    assert!(tokens.contains(&"fox".to_string()));
    assert!(!tokens.contains(&"the".to_string()));
}

#[test]
fn cjk_bigram_pure_chinese() {
    let tokens = tokenize(TOKENIZER_CJK, "中华人民").unwrap();
    assert_eq!(tokens, vec!["中华", "华人", "人民"]);
}

#[test]
fn cjk_bigram_pure_ascii() {
    let tokens = tokenize(TOKENIZER_CJK, "hello world").unwrap();
    assert_eq!(tokens, vec!["hello", "world"]);
}

#[test]
fn cjk_bigram_mixed() {
    let tokens = tokenize(TOKENIZER_CJK, "java中华人民").unwrap();
    assert_eq!(tokens, vec!["java", "中华", "华人", "人民"]);
}

#[test]
fn cjk_bigram_single_char() {
    let tokens = tokenize(TOKENIZER_CJK, "中").unwrap();
    assert_eq!(tokens, vec!["中"]);
}

#[test]
fn cjk_bigram_japanese() {
    let tokens = tokenize(TOKENIZER_CJK, "東京タワー").unwrap();
    assert_eq!(tokens, vec!["東京", "京タ", "タワ", "ワー"]);
}

#[test]
fn cjk_bigram_punctuation_breaks() {
    let tokens = tokenize(TOKENIZER_CJK, "中华，人民！").unwrap();
    assert_eq!(tokens, vec!["中华", "人民"]);
}

#[test]
fn cjk_bigram_lowercase() {
    let tokens = tokenize(TOKENIZER_CJK, "StarRocks 是数据库").unwrap();
    assert_eq!(tokens, vec!["starrocks", "是数", "数据", "据库"]);
}

#[test]
fn jieba_tokenize() {
    let tokens = tokenize(TOKENIZER_JIEBA, "中华人民共和国成立了").unwrap();
    assert!(tokens.contains(&"中华".to_string()));
    assert!(tokens.contains(&"人民".to_string()));
    assert!(tokens.contains(&"共和国".to_string()));
}

#[test]
fn jieba_mixed_text() {
    let tokens = tokenize(TOKENIZER_JIEBA, "StarRocks 是一款高性能数据库").unwrap();
    assert!(tokens.contains(&"starrocks".to_string()));
    assert!(tokens.contains(&"高性能".to_string()));
    assert!(tokens.contains(&"数据库".to_string()));
}
