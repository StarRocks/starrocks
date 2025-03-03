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
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <rapidjson/document.h>
DIAGNOSTIC_POP

namespace starrocks {

using ScrollParser = ScrollParser;
using ColumnHelper = ColumnHelper;
using ColumnPtr = ColumnPtr;
struct SlotDesc {
    std::string name;
    TypeDescriptor type;
};

class ScrollParserTest : public ::testing::Test {
public:
    void SetUp() override { _create_runtime_state(""); }
    void TearDown() override {}

protected:
    void _create_runtime_state(const std::string& timezone);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);
    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
};

void ScrollParserTest::_create_runtime_state(const std::string& timezone) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (timezone != "") {
        query_globals.__set_time_zone(timezone);
    }
    _runtime_state = _pool.add(new RuntimeState(fragment_id, query_options, query_globals, nullptr));
    _runtime_state->init_instance_mem_tracker();
}

TupleDescriptor* ScrollParserTest::_create_tuple_desc(SlotDesc* descs) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    int slot_id = 0;
    while (descs->name != "") {
        slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        descs += 1;
        slot_id += 1;
    }
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());
    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];
    return tuple_desc;
}

TEST_F(ScrollParserTest, ArrayTest) {
    std::unique_ptr<ScrollParser> scroll_parser = std::make_unique<ScrollParser>(false);

    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_TINYINT);

    auto get_parsed_column = [&scroll_parser](const rapidjson::Document& document, const TypeDescriptor& t,
                                              bool pure_doc_value) {
        MutableColumnPtr column = ColumnHelper::create_column(t, true);
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

        auto validator = [](const MutableColumnPtr& col) {
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

        auto validator = [](const MutableColumnPtr& col) {
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

        auto validator = [](const MutableColumnPtr& col) {
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

        auto validator = [](const MutableColumnPtr& col) {
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

        auto validator = [](const MutableColumnPtr& col) {
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

TEST_F(ScrollParserTest, JsonTestOK) {
    {
        SlotDesc slot_descs[] = {{"contactData", TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON)}, {""}};
        std::unique_ptr<ScrollParser> scroll_parser = std::make_unique<ScrollParser>(false);
        scroll_parser->set_params(_create_tuple_desc(slot_descs), nullptr, _runtime_state->timezone());
        Status st = scroll_parser->parse(
                R"( {"_scroll_id":"xxxx","hits":{"total":1,"hits":[{"_id":"1YGqWI4BIIvgRRz3t8R_","_source":{"contactData":[{"firstName":"Jane","lastName":"Doe","emailAddress":"jane.doe@example.com","phoneNumber":"+1987654321","contactType":"email"}]}}]}})",
                true);
        EXPECT_TRUE(st.ok()) << st.message();
        ChunkPtr chunk;
        bool line_eos = false;
        st = scroll_parser->fill_chunk(_runtime_state, &chunk, &line_eos);
        EXPECT_TRUE(st.ok()) << st.message();
        std::string result = chunk->debug_row(0);
        EXPECT_EQ(
                result,
                R"([[{"contactType": "email", "emailAddress": "jane.doe@example.com", "firstName": "Jane", "lastName": "Doe", "phoneNumber": "+1987654321"}]])");
    }
    {
        SlotDesc slot_descs[] = {{"contactData", TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON)}, {""}};
        std::unique_ptr<ScrollParser> scroll_parser = std::make_unique<ScrollParser>(false);
        scroll_parser->set_params(_create_tuple_desc(slot_descs), nullptr, _runtime_state->timezone());
        Status st = scroll_parser->parse(
                R"( {"_scroll_id":"xxxx","hits":{"total":1,"hits":[{"_id":"1YGqWI4BIIvgRRz3t8R_","_source":{"contactData":123}}]}})",
                true);
        EXPECT_TRUE(st.ok()) << st.message();
        ChunkPtr chunk;
        bool line_eos = false;
        st = scroll_parser->fill_chunk(_runtime_state, &chunk, &line_eos);
        EXPECT_FALSE(st.ok()) << st.message();
    }
}

} // namespace starrocks
