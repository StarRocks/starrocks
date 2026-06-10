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

#include "storage/index/inverted/builtin/builtin_plugin.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/tablet_index.h"
#include "testutil/assert.h"

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
    // Each branch builds a fresh TabletIndex because add_common_properties
    // is insert-only (no overwrite of an existing key).
    auto check = [](const std::string& imp_lib_value) {
        TabletIndex t;
        t.add_common_properties(INVERTED_IMP_KEY, imp_lib_value);
        return get_inverted_imp_type(t);
    };

    {
        TabletIndex empty;
        auto res = get_inverted_imp_type(empty);
        ASSERT_FALSE(res.ok());
    }

    {
        auto res = check(TYPE_CLUCENE);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::CLUCENE, res.value());
    }
    {
        auto res = check(TYPE_BUILTIN);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::BUILTIN, res.value());
    }
    {
        auto res = check(TYPE_TANTIVY);
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::TANTIVY, res.value());
    }
    {
        // Case-insensitive — DDL may carry "TANTIVY", "Tantivy", etc.
        auto res = check("TANTIVY");
        ASSERT_TRUE(res.ok());
        ASSERT_EQ(InvertedImplementType::TANTIVY, res.value());
    }
    {
        auto res = check("invalid");
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

// A plugin that emits a per-(seg, index) standalone file (builtin / clucene)
// reports `need_compound() == false` and only implements `finish`. The
// default `finish_compound` should never be called on such a plugin —
// ColumnWriter dispatches via need_compound(), so the default body is a
// last-resort safety net rather than a sentinel. Verify the protocol bit and
// that calling finish_compound directly fails loudly (not NotSupported, since
// that status no longer drives any behavioral fallback).
TEST(InvertedIndexPluginTest, builtin_does_not_produce_compound) {
    auto* plugin = &BuiltinPlugin::get_instance();
    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    TabletIndex tablet_index;
    std::unique_ptr<InvertedWriter> writer;
    ASSERT_OK(plugin->create_inverted_index_writer(typeinfo, "c0", "path", &tablet_index, &writer));
    ASSERT_NE(nullptr, writer);

    EXPECT_FALSE(writer->need_compound());

    auto compound_or = writer->finish_compound(nullptr);
    ASSERT_FALSE(compound_or.ok());
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

} // namespace starrocks
