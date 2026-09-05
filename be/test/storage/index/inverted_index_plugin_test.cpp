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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/tablet_index.h"

namespace starrocks {

TEST(InvertedIndexPluginTest, factory_test) {
    auto builtin_res = InvertedPluginFactory::get_plugin(InvertedImplementType::BUILTIN);
    ASSERT_TRUE(builtin_res.ok());
    ASSERT_NE(nullptr, builtin_res.value());

    auto clucene_res = InvertedPluginFactory::get_plugin(InvertedImplementType::CLUCENE);
    ASSERT_TRUE(clucene_res.ok());
    ASSERT_NE(nullptr, clucene_res.value());

    auto invalid_res = InvertedPluginFactory::get_plugin(static_cast<InvertedImplementType>(-1));
    ASSERT_FALSE(invalid_res.ok());
}

TEST(InvertedIndexPluginTest, option_test) {
    TabletIndex tablet_index;

    // Test get_inverted_imp_type
    {
        auto res = get_inverted_imp_type(tablet_index);
        ASSERT_FALSE(res.ok());

        tablet_index.set_common_properties({{INVERTED_IMP_KEY, TYPE_CLUCENE}});
        res = get_inverted_imp_type(tablet_index);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::CLUCENE, res.value());

        tablet_index.set_common_properties({{INVERTED_IMP_KEY, TYPE_BUILTIN}});
        res = get_inverted_imp_type(tablet_index);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::BUILTIN, res.value());

        tablet_index.set_common_properties({{INVERTED_IMP_KEY, "invalid"}});
        res = get_inverted_imp_type(tablet_index);
        ASSERT_FALSE(res.ok());
    }

    // Test parser string and type conversions
    {
        ASSERT_EQ(INVERTED_INDEX_PARSER_NONE,
                  inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_NONE));
        ASSERT_EQ(INVERTED_INDEX_PARSER_STANDARD,
                  inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_STANDARD));
        ASSERT_EQ(INVERTED_INDEX_PARSER_ENGLISH,
                  inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_ENGLISH));
        ASSERT_EQ(INVERTED_INDEX_PARSER_CHINESE,
                  inverted_index_parser_type_to_string(InvertedIndexParserType::PARSER_CHINESE));
        ASSERT_EQ(INVERTED_INDEX_PARSER_UNKNOWN,
                  inverted_index_parser_type_to_string(static_cast<InvertedIndexParserType>(-1)));

        ASSERT_EQ(InvertedIndexParserType::PARSER_NONE,
                  get_inverted_index_parser_type_from_string(INVERTED_INDEX_PARSER_NONE));
        ASSERT_EQ(InvertedIndexParserType::PARSER_STANDARD,
                  get_inverted_index_parser_type_from_string(INVERTED_INDEX_PARSER_STANDARD));
        ASSERT_EQ(InvertedIndexParserType::PARSER_ENGLISH,
                  get_inverted_index_parser_type_from_string(INVERTED_INDEX_PARSER_ENGLISH));
        ASSERT_EQ(InvertedIndexParserType::PARSER_CHINESE,
                  get_inverted_index_parser_type_from_string(INVERTED_INDEX_PARSER_CHINESE));
        ASSERT_EQ(InvertedIndexParserType::PARSER_UNKNOWN, get_inverted_index_parser_type_from_string("unknown"));
    }

    // Test get_parser_string_from_properties
    {
        std::map<std::string, std::string> props;
        ASSERT_EQ(INVERTED_INDEX_PARSER_NONE, get_parser_string_from_properties(props));

        props[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
        ASSERT_EQ(INVERTED_INDEX_PARSER_ENGLISH, get_parser_string_from_properties(props));
    }

    // Test is_tokenized_from_properties
    {
        std::map<std::string, std::string> props;
        ASSERT_FALSE(is_tokenized_from_properties(props));

        props[INVERTED_INDEX_TOKENIZED_KEY] = "true";
        ASSERT_TRUE(is_tokenized_from_properties(props));

        props[INVERTED_INDEX_TOKENIZED_KEY] = "false";
        ASSERT_FALSE(is_tokenized_from_properties(props));
    }
}

TEST(InvertedIndexPluginTest, builtin_plugin_test) {
    auto* plugin = &BuiltinPlugin::get_instance();
    ASSERT_NE(nullptr, plugin);

    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "c0", "path", &tablet_index, &writer));
    ASSERT_NE(nullptr, writer);

    std::unique_ptr<InvertedReader> reader;
    auto tablet_index_sp = std::make_shared<TabletIndex>(tablet_index);
    ASSERT_OK(plugin->create_inverted_index_reader("path", tablet_index_sp, TYPE_VARCHAR, &reader));
    ASSERT_NE(nullptr, reader);
}

// Tests for get_support_phrase_from_properties(). This helper is the single source of
// truth for the BE-side interpretation of the GIN `support_phrase` property; both the
// CLucene writer (to decide whether to keep term positions on disk) and the reader
// (to decide whether MATCH_PHRASE_QUERY can be served) read it. The contract is:
//   1. missing key  -> false  (backward compatibility with pre-feature indexes)
//   2. "true" / "True" / "TRUE" -> true  (case-insensitive on value)
//   3. anything else (including "false", "yes", "1", empty string) -> false
TEST(InvertedIndexPluginTest, support_phrase_property_test) {
    // Missing key must default to false so old metadata (no support_phrase) keeps the
    // legacy behavior where MATCH_PHRASE is unavailable.
    {
        std::map<std::string, std::string> props;
        ASSERT_FALSE(get_support_phrase_from_properties(props));
    }

    // Canonical "true" enables phrase support.
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "true";
        ASSERT_TRUE(get_support_phrase_from_properties(props));
    }

    // Value is matched case-insensitively. This mirrors the FE-side validator
    // (IndexAnalyzer.checkInvertedIndexSupportPhrase uses equalsIgnoreCase) so that
    // metadata round-tripped from older FE versions or hand-edited TabletIndex
    // dumps still works.
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "True";
        ASSERT_TRUE(get_support_phrase_from_properties(props));
    }
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "TRUE";
        ASSERT_TRUE(get_support_phrase_from_properties(props));
    }

    // Explicit "false" stays false.
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "false";
        ASSERT_FALSE(get_support_phrase_from_properties(props));
    }

    // Anything that is not "true" must be treated as false; we explicitly do not
    // accept "yes" / "1" / etc. to keep the contract strict and predictable.
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "yes";
        ASSERT_FALSE(get_support_phrase_from_properties(props));
    }
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "1";
        ASSERT_FALSE(get_support_phrase_from_properties(props));
    }
    {
        std::map<std::string, std::string> props;
        props[INVERTED_INDEX_SUPPORT_PHRASE_KEY] = "";
        ASSERT_FALSE(get_support_phrase_from_properties(props));
    }
}

} // namespace starrocks
