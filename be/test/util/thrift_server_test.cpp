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

#include "common/util/thrift_server.h"

#include <gtest/gtest.h>
#include <thrift/TConfiguration.h>
#include <thrift/transport/TBufferTransports.h>

#include <memory>

#include "common/config_thrift_server_fwd.h"

namespace starrocks {

// The BE thrift server (BackendService) previously used the default
// TBufferedTransportFactory, so config::thrift_max_message_size never took effect on the
// server read path and thrift's default 100MB limit applied, making large
// submit_tasks messages fail with "MaxMessageSize reached". ConfigurableBufferedTransportFactory
// fixes that by attaching a TConfiguration built from create_thrift_configuration().
class ThriftServerTest : public testing::Test {};

// The transport produced by ConfigurableBufferedTransportFactory must carry the
// configured maxMessageSize (config::thrift_max_message_size), not thrift's default.
TEST_F(ThriftServerTest, configurable_transport_factory_honors_max_message_size) {
    ConfigurableBufferedTransportFactory factory;
    auto underlying = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    auto transport = factory.getTransport(underlying);
    ASSERT_NE(nullptr, transport);

    auto config = transport->getConfiguration();
    ASSERT_NE(nullptr, config);
    EXPECT_EQ(static_cast<int>(config::thrift_max_message_size), config->getMaxMessageSize());
}

// Sanity check contrasting with the default factory: the default TBufferedTransportFactory
// uses thrift's default maxMessageSize (100MB), which is exactly the limit this factory
// works around.
TEST_F(ThriftServerTest, configurable_transport_factory_differs_from_default) {
    // Note: use a local copy of thrift's default constant to avoid ODR-using the static
    // const member through EXPECT_EQ's by-reference parameters.
    const int default_max_message_size = 100 * 1024 * 1024;
    apache::thrift::transport::TBufferedTransportFactory default_factory;
    auto default_transport = default_factory.getTransport(std::make_shared<apache::thrift::transport::TMemoryBuffer>());
    EXPECT_EQ(default_max_message_size, default_transport->getConfiguration()->getMaxMessageSize());

    ConfigurableBufferedTransportFactory factory;
    auto transport = factory.getTransport(std::make_shared<apache::thrift::transport::TMemoryBuffer>());
    EXPECT_EQ(static_cast<int>(config::thrift_max_message_size), transport->getConfiguration()->getMaxMessageSize());
}

// thrift_max_message_size is a mutable config that can be changed at runtime. The factory
// must build the TConfiguration from the current config value on every getTransport() call,
// so that a runtime change takes effect on subsequent connections without restarting the BE.
TEST_F(ThriftServerTest, configurable_transport_factory_picks_up_runtime_config_change) {
    const int32_t original = config::thrift_max_message_size;
    ConfigurableBufferedTransportFactory factory;

    config::thrift_max_message_size = 100 * 1024 * 1024; // 100MB
    auto t1 = factory.getTransport(std::make_shared<apache::thrift::transport::TMemoryBuffer>());
    EXPECT_EQ(100 * 1024 * 1024, t1->getConfiguration()->getMaxMessageSize());

    // Simulate a runtime config update; the same factory must reflect the new value on the
    // next connection.
    config::thrift_max_message_size = 512 * 1024 * 1024; // 512MB
    auto t2 = factory.getTransport(std::make_shared<apache::thrift::transport::TMemoryBuffer>());
    EXPECT_EQ(512 * 1024 * 1024, t2->getConfiguration()->getMaxMessageSize());

    // The transport created before the change keeps its original limit.
    EXPECT_EQ(100 * 1024 * 1024, t1->getConfiguration()->getMaxMessageSize());

    config::thrift_max_message_size = original;
}

} // namespace starrocks
