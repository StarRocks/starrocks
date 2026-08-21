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
    build, canonicalize, resolve, tokenize, tokenize_detail, TOKENIZER_CJK, TOKENIZER_ENGLISH,
    TOKENIZER_IK, TOKENIZER_IK_SMART, TOKENIZER_JIEBA, TOKENIZER_NGRAM, TOKENIZER_RAW,
    TOKENIZER_STANDARD,
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
fn ik_tokenize_with_builtin_dictionary() {
    let tokens = tokenize("ik", "张华考上了北京大学").unwrap();
    assert!(tokens.contains(&"张华".to_string()), "got: {tokens:?}");
    assert!(tokens.contains(&"北京大学".to_string()), "got: {tokens:?}");
}

#[test]
fn ik_accepts_compatible_mode_names() {
    let smart = canonicalize(r#"{"tokenizer":{"type":"ik","mode":"ik_smart"}}"#)
        .unwrap()
        .0;
    let max_word = canonicalize(r#"{"tokenizer":{"type":"ik","mode":"ik_max_word"}}"#)
        .unwrap()
        .0;
    assert!(smart.contains(r#""mode":"search""#));
    assert!(max_word.contains(r#""mode":"index""#));
}

#[test]
fn legacy_chinese_alias_builds() {
    assert!(build("chinese").is_ok());
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
#[test]
fn raw_preserves_empty_placeholder() {
    let tokens = tokenize(TOKENIZER_RAW, "").unwrap();
    assert_eq!(tokens, vec![""]);
}

// The legacy English adapter preserves SimpleTokenizer + length + lowercase +
// bundled English stopword behavior.
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
fn legacy_standard_preserves_english_adapter_behavior() {
    assert_eq!(
        tokenize("standard", "The Quick Fox").unwrap(),
        tokenize("english", "The Quick Fox").unwrap()
    );
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

const PIPELINE: &str = r#"{
  "char_filter":[
    {"type":"unicode_normalize","form":"nfkc"},
    {"type":"mapping","mappings":["＆ => &"]}
  ],
  "tokenizer":{"type":"standard"},
  "token_filter":[
    {"type":"lowercase"},
    {"type":"stop","stopwords":["the"]},
    {"type":"length","min":2,"max":32}
  ]
}"#;

#[test]
fn pipeline_is_canonical_and_digest_is_stable() {
    let (canonical, digest) = canonicalize(PIPELINE).unwrap();
    assert_eq!(digest.len(), 64);
    assert_eq!(
        canonicalize(&canonical).unwrap(),
        (canonical.clone(), digest)
    );
    assert!(canonical.contains("\"spec_version\":1"));
    assert!(canonical.contains("\"runtime_abi_version\":1"));
    assert!(canonical.contains("\"resource_refs\""));
}

#[test]
fn canonical_definition_matches_fe_contract() {
    let definition = r#"{"token_filter":[{"type":"lowercase"}],"tokenizer":{"type":"cjk"}}"#;
    let expected = r#"{"spec_version":1,"runtime_abi_version":1,"builtin_model_version":"starrocks-tantivy-3.5-v1","char_filter":[],"tokenizer":{"type":"chinese"},"token_filter":[{"type":"lowercase"}],"resource_refs":[]}"#;
    let (canonical, digest) = canonicalize(definition).unwrap();
    assert_eq!(canonical, expected);
    assert_eq!(
        digest,
        "7b054591ed8e95c775dac57c1b1a7a9e4649d420d6ee814ec269e4768aa6a8f2"
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

#[test]
fn analyzer_digest_mismatch_fails_closed() {
    let (_, digest) = canonicalize(PIPELINE).unwrap();
    assert!(resolve(PIPELINE, Some(&digest)).is_ok());
    let error = match resolve(
        PIPELINE,
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
    ) {
        Ok(_) => panic!("expected digest mismatch"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("analyzer digest mismatch"));
}

#[test]
fn pipeline_applies_char_and_token_filters_in_order() {
    let tokens = tokenize(PIPELINE, "Ｔｈｅ Quick＆BROWN a").unwrap();
    assert_eq!(tokens, vec!["quick&brown"]);
}

#[test]
fn stop_filter_preserves_position_gap_and_original_offsets() {
    let definition = r#"{
      "tokenizer":{"type":"standard"},
      "token_filter":[{"type":"lowercase"},{"type":"stop","stopwords":["and"]}]
    }"#;
    let tokens = tokenize_detail(definition, "Red AND Blue").unwrap();
    assert_eq!(tokens.len(), 2);
    assert_eq!((tokens[0].text.as_str(), tokens[0].position), ("red", 0));
    assert_eq!((tokens[1].text.as_str(), tokens[1].position), ("blue", 2));
    assert_eq!((tokens[1].offset_from, tokens[1].offset_to), (8, 12));
}

#[test]
fn mapping_corrects_offsets_to_original_input() {
    let definition = r#"{
      "char_filter":[{"type":"mapping","mappings":["＆ => and"]}],
      "tokenizer":{"type":"standard"}
    }"#;
    let tokens = tokenize_detail(definition, "A＆B").unwrap();
    assert_eq!(
        tokens
            .iter()
            .map(|token| token.text.as_str())
            .collect::<Vec<_>>(),
        vec!["AandB"]
    );
    assert_eq!((tokens[0].offset_from, tokens[0].offset_to), (0, 5));
}

#[test]
fn mapping_expansion_never_produces_zero_length_offsets() {
    let definition = r#"{
      "char_filter":[{"type":"mapping","mappings":["X => a b"]}],
      "tokenizer":{"type":"standard"}
    }"#;
    let tokens = tokenize_detail(definition, "X").unwrap();
    assert_eq!(
        tokens
            .iter()
            .map(|token| (token.text.as_str(), token.offset_from, token.offset_to))
            .collect::<Vec<_>>(),
        vec![("a", 0, 1), ("b", 0, 1)]
    );
}

#[test]
fn nfc_composes_combining_sequence_and_preserves_original_offsets() {
    let definition = r#"{
      "char_filter":[{"type":"unicode_normalize","form":"nfc"}],
      "tokenizer":{"type":"standard"}
    }"#;
    let tokens = tokenize_detail(definition, "Cafe\u{301}").unwrap();
    assert_eq!(tokens[0].text, "Café");
    assert_eq!((tokens[0].offset_from, tokens[0].offset_to), (0, 6));
}

#[test]
fn nfd_expansion_preserves_original_end_offset() {
    let definition = r#"{
      "char_filter":[{"type":"unicode_normalize","form":"nfd"}],
      "tokenizer":{"type":"standard"}
    }"#;
    let tokens = tokenize_detail(definition, "é").unwrap();
    assert_eq!(tokens[0].text, "e");
    assert_eq!((tokens[0].offset_from, tokens[0].offset_to), (0, 2));
}

#[test]
fn nfc_composes_hangul_jamo_and_preserves_original_offsets() {
    let definition = r#"{
      "char_filter":[{"type":"unicode_normalize","form":"nfc"}],
      "tokenizer":{"type":"standard"}
    }"#;
    let tokens = tokenize_detail(definition, "\u{1100}\u{1161}").unwrap();
    assert_eq!(tokens[0].text, "가");
    assert_eq!((tokens[0].offset_from, tokens[0].offset_to), (0, 6));
}

#[test]
fn phase_one_rejects_external_resources_and_unknown_fields() {
    let resource = r#"{
      "tokenizer":{"type":"jieba","user_dictionary":"s3://bucket/dict"}
    }"#;
    assert!(canonicalize(resource)
        .unwrap_err()
        .to_string()
        .contains("unknown field"));

    let refs = r#"{
      "tokenizer":{"type":"standard"},
      "resource_refs":[{"name":"dict","digest":"abc"}]
    }"#;
    assert!(canonicalize(refs)
        .unwrap_err()
        .to_string()
        .contains("resource_refs are not supported"));
}

#[test]
fn ngram_pipeline_honors_bounds() {
    let definition = r#"{"tokenizer":{"type":"ngram","min_gram":2,"max_gram":3}}"#;
    assert_eq!(
        tokenize(definition, "abcd").unwrap(),
        vec!["ab", "abc", "bc", "bcd", "cd"]
    );
}

#[test]
fn analyzer_limits_fail_without_truncation() {
    let invalid_ngram = r#"{"tokenizer":{"type":"ngram","min_gram":1,"max_gram":33}}"#;
    assert!(canonicalize(invalid_ngram).is_err());

    let oversized = "x".repeat(crate::safe::tokenizer::spec::MAX_INPUT_BYTES + 1);
    let error = tokenize("raw", &oversized).unwrap_err();
    assert!(error.to_string().contains("input exceeds"));
}
