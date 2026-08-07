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
    build, tokenize, TOKENIZER_CJK, TOKENIZER_ENGLISH, TOKENIZER_IK, TOKENIZER_IK_SMART,
    TOKENIZER_JIEBA, TOKENIZER_NGRAM, TOKENIZER_RAW, TOKENIZER_STANDARD,
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
fn ik_modes_build() {
    assert!(build(TOKENIZER_IK).is_ok());
    assert!(build(TOKENIZER_IK_SMART).is_ok());
}

#[test]
fn ngram_builds_with_explicit_range() {
    assert!(build(&format!("{TOKENIZER_NGRAM}:2:3")).is_ok());
}

#[test]
fn standard_builds() {
    assert!(build(TOKENIZER_STANDARD).is_ok());
}

#[test]
fn raw_builds() {
    assert!(build(TOKENIZER_RAW).is_ok());
}

#[test]
fn unsupported_rejected() {
    match build("definitely-not-a-tokenizer") {
        Ok(_) => panic!("expected unsupported tokenizer error"),
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("unsupported tokenizer"), "got: {msg}");
        }
    }
}

#[test]
fn invalid_ngram_config_rejected() {
    for name in [
        "ngram",
        "ngram:0:2",
        "ngram:3:2",
        "ngram:two:3",
        "ngram:2:3:4",
    ] {
        let err = match build(name) {
            Ok(_) => panic!("expected invalid ngram tokenizer '{name}' to fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("ngram"), "got: {err}");
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
fn ngram_tokenize_unicode_and_lowercase() {
    let tokens = tokenize("ngram:2:3", "Ab中").unwrap();
    assert_eq!(tokens, vec!["ab", "ab中", "b中"]);
}

#[test]
fn ngram_tokenize_all_inner_grams() {
    let tokens = tokenize("ngram:2:3", "hello").unwrap();
    assert_eq!(tokens, vec!["he", "hel", "el", "ell", "ll", "llo", "lo"]);
}

#[test]
fn standard_tokenize_clucene_grammar() {
    let tokens = tokenize(
        TOKENIZER_STANDARD,
        "The Quick Brown U.S.A. AT&T foo-bar 192.168.1.2 user@example.com 中华人民",
    )
    .unwrap();
    assert_eq!(
        tokens,
        vec![
            "quick",
            "brown",
            "usa",
            "at&t",
            "foo",
            "bar",
            "192.168.1.2",
            "user@example.com",
            "中华人民",
        ]
    );
}

#[test]
fn standard_tokenize_apostrophes_and_dotted_terms() {
    let tokens = tokenize(
        TOKENIZER_STANDARD,
        "can't dog's dogs' host-name.com windowsupdate.microsoft.com--update A&B.C",
    )
    .unwrap();
    assert_eq!(
        tokens,
        vec![
            "can't",
            "dog",
            "dogs",
            "host",
            "name.com",
            "windowsupdate.microsoft.com",
            "update",
            "a&b",
            "c",
        ]
    );
}

#[test]
fn standard_tokenize_mixed_unicode() {
    let tokens = tokenize(TOKENIZER_STANDARD, "abc中华123 人民abc カタカナ한글").unwrap();
    assert_eq!(tokens, vec!["abc中华123", "人民abc", "カタカナ한글"]);
}

#[test]
fn standard_preserves_positions_and_source_offsets() {
    let mut analyzer = build(TOKENIZER_STANDARD).unwrap();
    let mut stream = analyzer.token_stream("The U.S.A. Dog's");

    assert!(stream.advance());
    let acronym = stream.token().clone();
    assert_eq!(acronym.text, "usa");
    assert_eq!((acronym.offset_from, acronym.offset_to), (4, 10));
    assert_eq!(acronym.position, 1);

    assert!(stream.advance());
    let possessive = stream.token().clone();
    assert_eq!(possessive.text, "dog");
    assert_eq!((possessive.offset_from, possessive.offset_to), (11, 16));
    assert_eq!(possessive.position, 2);
    assert!(!stream.advance());
}

#[test]
fn standard_caps_tokens_at_clucene_limit() {
    let input = "x".repeat(256);
    let tokens = tokenize(TOKENIZER_STANDARD, &input).unwrap();
    assert_eq!(tokens, vec!["x".repeat(255)]);
}

// Contract: english_analyzer uses SimpleTokenizer + RemoveLongFilter + LowerCaser
// + StopWordFilter(English). See `english_analyzer()` in safe/tokenizer/mod.rs.
// Key behavior: (1) English stopwords like "the" ARE removed; (2) no Porter
// stemming — inflected forms are kept as-is (e.g. "foxes" stays "foxes").
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

#[test]
fn ik_default_is_index_mode() {
    let text = "中华人民共和国国歌";
    let default_tokens = tokenize(TOKENIZER_IK, text).unwrap();

    assert!(
        default_tokens.contains(&"中华".to_string()),
        "got: {default_tokens:?}"
    );
    assert!(
        default_tokens.contains(&"中华人民共和国".to_string()),
        "got: {default_tokens:?}"
    );
}

#[test]
fn ik_search_mode_is_coarser_than_index_mode() {
    let text = "中华人民共和国国歌";
    let index_tokens = tokenize(TOKENIZER_IK, text).unwrap();
    let search_tokens = tokenize(TOKENIZER_IK_SMART, text).unwrap();

    assert!(index_tokens.len() > search_tokens.len());
    assert_eq!(search_tokens, vec!["中华人民共和国", "国歌"]);
}

#[test]
fn ik_mixed_text_lowercase() {
    let tokens = tokenize(TOKENIZER_IK, "StarRocks数据库").unwrap();
    assert!(tokens.contains(&"starrocks".to_string()), "got: {tokens:?}");
    assert!(tokens.contains(&"数据库".to_string()), "got: {tokens:?}");
}
