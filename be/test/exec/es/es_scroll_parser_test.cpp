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

#include "exec/es/es_scroll_parser.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <rapidjson/document.h>
DIAGNOSTIC_POP

namespace starrocks {

using ScrollParser = ScrollParser;
using ColumnHelper = ColumnHelper;
using ColumnPtr = ColumnPtr;

TEST(ScrollParserTest, ArrayTest) {
    std::unique_ptr<ScrollParser> scroll_parser = std::make_unique<ScrollParser>(false);

    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_TINYINT);

    auto get_parsed_column = [&scroll_parser](const rapidjson::Document& document, const TypeDescriptor& t,
                                              bool pure_doc_value) {
        ColumnPtr column = ColumnHelper::create_column(t, true);
        if (pure_doc_value) {
            const rapidjson::Value& val = document["fields"]["array"];
            EXPECT_TRUE(scroll_parser->_append_value_from_json_val(column.get(), t, val, pure_doc_value).ok());
        } else {
            const rapidjson::Value& val = document["_source"]["array"];
            EXPECT_TRUE(scroll_parser->_append_value_from_json_val(column.get(), t, val, pure_doc_value).ok());
        }
        return column;
    };

    {
        // {
        //     "_source": {
        //         "array": 1
        //     },
        //     "fields": {
        //         "array": [1]
        //     }
        // }

        auto validator = [](const ColumnPtr& col) {
            auto arr = col->get(0).get_array();
            EXPECT_EQ(1, arr.size());
            EXPECT_EQ(1, arr[0].get_int8());
        };

        rapidjson::Document document;
        document.Parse(R"({"_source":{"array":1},"fields":{"array":[1]}})");

        auto doc_column = get_parsed_column(document, t, true);
        auto source_column = get_parsed_column(document, t, false);

        validator(doc_column);
        validator(source_column);
    }

    {
        // {
        //     "_source": {
        //         "array": [1, 2, 3]
        //     },
        //     "fields": {
        //         "array": [1, 2, 3]
        //     }
        // }

        auto validator = [](const ColumnPtr& col) {
            auto arr = col->get(0).get_array();
            EXPECT_EQ(3, arr.size());
            EXPECT_EQ(1, arr[0].get_int8());
            EXPECT_EQ(2, arr[1].get_int8());
            EXPECT_EQ(3, arr[2].get_int8());
        };

        rapidjson::Document document;
        document.Parse(R"({"_source":{"array":[1,2,3]},"fields":{"array":[1,2,3]}})");

        auto doc_column = get_parsed_column(document, t, true);
        auto source_column = get_parsed_column(document, t, false);

        validator(doc_column);
        validator(source_column);
    }

    {
        // {
        //     "_source": {
        //         "array": [1, [2, 3]]
        //     },
        //     "fields": {
        //         "array": [1, 2, 3]
        //     }
        // }

        auto validator = [](const ColumnPtr& col) {
            auto arr = col->get(0).get_array();
            EXPECT_EQ(3, arr.size());
            EXPECT_EQ(1, arr[0].get_int8());
            EXPECT_EQ(2, arr[1].get_int8());
            EXPECT_EQ(3, arr[2].get_int8());
        };

        rapidjson::Document document;
        document.Parse(R"({"_source":{"array":[1,[2,3]]},"fields":{"array":[1,2,3]}})");

        auto doc_column = get_parsed_column(document, t, true);
        auto source_column = get_parsed_column(document, t, false);

        validator(doc_column);
        validator(source_column);
    }

    {
        // {
        //     "_source": {
        //         "array": [1, null, 2]
        //     },
        //     "fields": {
        //         "array": [1, 2]
        //     }
        // }

        auto validator = [](const ColumnPtr& col) {
            auto arr = col->get(0).get_array();
            EXPECT_EQ(2, arr.size());
            EXPECT_EQ(1, arr[0].get_int8());
            EXPECT_EQ(2, arr[1].get_int8());
        };

        rapidjson::Document document;
        document.Parse(R"({"_source":{"array":[1,null,2]},"fields":{"array":[1,2]}})");

        auto doc_column = get_parsed_column(document, t, true);
        auto source_column = get_parsed_column(document, t, false);

        validator(doc_column);
        validator(source_column);
    }

    {
        // {
        //     "_source": {
        //         "array": null
        //     },
        //     "fields": {
        //         "array": []
        //     }
        // }

        auto validator = [](const ColumnPtr& col) {
            auto arr = col->get(0).get_array();
            EXPECT_EQ(0, arr.size());
        };

        rapidjson::Document document;
        document.Parse(R"({"_source":{"array":null},"fields":{"array":[]}})");

        auto doc_column = get_parsed_column(document, t, true);
        auto source_column = get_parsed_column(document, t, false);

        validator(doc_column);
        validator(source_column);
    }
}

// TODO: We should add more detailed UT in the future.

} // namespace starrocks
