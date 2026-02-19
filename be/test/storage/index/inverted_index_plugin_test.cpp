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

} // namespace starrocks
