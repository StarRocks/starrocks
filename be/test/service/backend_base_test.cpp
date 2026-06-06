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
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "common/status.h"
#include "gen_cpp/InternalService_types.h"

using namespace apache::thrift;

namespace starrocks {

// Test case: verify Status::to_thrift() properly sets __isset.error_msgs flag
// This validates the fix for get_next() error message handling
TEST(StatusToThriftTest, not_found_error_message_isset_flag) {
    // Create a NOT_FOUND status with error message
    std::string error_msg = "context_id=test_ctx, send_offset=0, context_offset=100";
    Status st = Status::NotFound(error_msg);

    // Convert to TStatus
    TStatus t_status;
    st.to_thrift(&t_status);

    // Verify status code
    ASSERT_EQ(TStatusCode::NOT_FOUND, t_status.status_code);

    // Verify __isset.error_msgs is set to true (this is the key fix)
    ASSERT_TRUE(t_status.__isset.error_msgs) << "__isset.error_msgs should be true after to_thrift()";

    // Verify error_msgs is not empty
    ASSERT_FALSE(t_status.error_msgs.empty()) << "error_msgs should not be empty";

    // Verify error message content
    ASSERT_EQ(1, t_status.error_msgs.size());
    ASSERT_EQ(error_msg, t_status.error_msgs[0]);

    // Additional verification: Ensure that when __isset.error_msgs flag is set,
    // error messages are correctly serialized and deserialized
    auto buffer = std::make_shared<transport::TMemoryBuffer>();
    auto protocol = std::make_shared<protocol::TBinaryProtocol>(buffer);
    t_status.write(protocol.get());

    std::string serialized_data = buffer->getBufferAsString();
    ASSERT_FALSE(serialized_data.find(error_msg) == std::string::npos)
            << "Error message should appear in serialized data when __isset flag is true";

    // Deserialize to verify round-trip behavior
    TStatus deserialized_status;
    auto read_buffer = std::make_shared<transport::TMemoryBuffer>(
            (uint8_t*)serialized_data.data(), (uint32_t)serialized_data.size(), transport::TMemoryBuffer::COPY);
    auto read_protocol = std::make_shared<protocol::TBinaryProtocol>(read_buffer);
    deserialized_status.read(read_protocol.get());

    ASSERT_FALSE(deserialized_status.error_msgs.empty())
            << "Error messages should be preserved after serialization/deserialization";
    ASSERT_TRUE(deserialized_status.__isset.error_msgs) << "__isset.error_msgs should be true after deserialization";
    ASSERT_EQ(error_msg, deserialized_status.error_msgs[0]);
}

// Test case: demonstrate the bug - directly setting TStatus without __isset flag
// This simulates the old buggy code behavior
TEST(StatusToThriftTest, bug_case_missing_isset_flag) {
    TStatus t_status;

    // Simulate the old buggy code in backend_base.cpp (before fix)
    t_status.status_code = TStatusCode::NOT_FOUND;
    t_status.error_msgs.push_back("context_id=test, send_offset=0, context_offset=100");
    // BUG: Missing t_status.__isset.error_msgs = true;

    // Verify the bug: __isset.error_msgs is false by default
    ASSERT_FALSE(t_status.__isset.error_msgs) << "Bug: __isset.error_msgs is false even though error_msgs has content";

    // Verify error_msgs vector has content locally (but Thrift will skip it during serialization)
    ASSERT_FALSE(t_status.error_msgs.empty()) << "error_msgs vector has content locally";

    // This demonstrates the bug: even though error_msgs has content,
    // Thrift will skip serialization because __isset.error_msgs is false.
    // The client will receive NOT_FOUND status but error_msgs will be null.

    // Additional verification: Demonstrate the bug through actual serialization/deserialization process
    TStatus buggy_status;
    buggy_status.status_code = TStatusCode::NOT_FOUND;
    buggy_status.error_msgs.push_back("context_id=test, send_offset=0, context_offset=100");
    // Note: Intentionally not setting buggy_status.__isset.error_msgs = true;

    // Serialize to show the bug
    auto buffer = std::make_shared<transport::TMemoryBuffer>();
    auto protocol = std::make_shared<protocol::TBinaryProtocol>(buffer);
    buggy_status.write(protocol.get());

    std::string serialized_data = buffer->getBufferAsString();
    ASSERT_TRUE(serialized_data.find("context_id=test, send_offset=0, context_offset=100") == std::string::npos)
            << "Error message should not be serialized when __isset.error_msgs is false";

    // Deserialize to verify the bug effect
    TStatus deserialized_status;
    auto read_buffer = std::make_shared<transport::TMemoryBuffer>(
            (uint8_t*)serialized_data.data(), (uint32_t)serialized_data.size(), transport::TMemoryBuffer::COPY);
    auto read_protocol = std::make_shared<protocol::TBinaryProtocol>(read_buffer);
    deserialized_status.read(read_protocol.get());

    ASSERT_TRUE(deserialized_status.error_msgs.empty())
            << "Client receives empty error_msgs due to missing __isset flag";
    ASSERT_FALSE(deserialized_status.__isset.error_msgs) << "__isset.error_msgs remains false after round-trip";
}

} // namespace starrocks