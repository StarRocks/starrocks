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

#include <google/protobuf/stubs/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "base/utility/defer_op.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

namespace {
std::vector<std::string>* g_protobuf_logs = nullptr;

void capture_protobuf_log(google::protobuf::LogLevel, const char*, int, const std::string& message) {
    if (g_protobuf_logs != nullptr) {
        g_protobuf_logs->push_back(message);
    }
}
} // namespace

// The global dict words carried by PTabletWriterOpenRequest are raw column values.
// A low-cardinality VARCHAR column may hold non-UTF-8 bytes (GBK text, binary
// blobs, ...). When the field was declared as a protobuf `string`, every
// serialize/parse of the open request logged
//   "String field 'starrocks.PSlotDescriptor.global_dict_words' contains invalid UTF-8 data"
// for each such word. Declaring it as `bytes` keeps the wire format and the C++
// accessors unchanged and stops the check from firing.
TEST(TabletSinkGlobalDictWordsTest, non_utf8_dict_words_round_trip_without_protobuf_warnings) {
    std::vector<std::string> logs;
    g_protobuf_logs = &logs;
    auto* old_handler = google::protobuf::SetLogHandler(&capture_protobuf_log);
    DeferOp restore([&]() {
        google::protobuf::SetLogHandler(old_handler);
        g_protobuf_logs = nullptr;
    });

    const std::string non_utf8_word(
            "\xff\xfe\x80"
            "abc",
            6);
    const std::string utf8_word("中文");

    PTabletWriterOpenRequest request;
    auto* slot = request.mutable_schema()->add_slot_descs();
    slot->set_id(1);
    slot->set_parent(0);
    slot->mutable_slot_type();
    slot->set_column_pos(0);
    slot->set_byte_offset(0);
    slot->set_null_indicator_byte(0);
    slot->set_null_indicator_bit(0);
    slot->set_col_name("c1");
    slot->set_slot_idx(0);
    slot->set_is_materialized(true);
    slot->add_global_dict_words(non_utf8_word);
    slot->add_global_dict_words(utf8_word);
    slot->set_global_dict_version(7);

    std::string buffer;
    ASSERT_TRUE(request.SerializePartialToString(&buffer));

    PTabletWriterOpenRequest parsed;
    ASSERT_TRUE(parsed.ParsePartialFromString(buffer));
    ASSERT_EQ(1, parsed.schema().slot_descs_size());
    const auto& parsed_slot = parsed.schema().slot_descs(0);
    ASSERT_EQ(2, parsed_slot.global_dict_words_size());
    EXPECT_EQ(non_utf8_word, parsed_slot.global_dict_words(0));
    EXPECT_EQ(utf8_word, parsed_slot.global_dict_words(1));
    EXPECT_EQ(7, parsed_slot.global_dict_version());

    EXPECT_TRUE(logs.empty()) << "unexpected protobuf log: " << logs.front();
}

} // namespace starrocks
