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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/thrift_util.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <memory>
#include <sstream>
#include <vector>

#include "common/status.h"

namespace starrocks {

class TNetworkAddress;
class ThriftServer;

// Utility class to serialize thrift objects to a binary format.  This object
// should be reused if possible to reuse the underlying memory.
// Note: thrift will encode NULLs into the serialized buffer so it is not valid
// to treat it as a string.
class ThriftSerializer {
public:
    // If compact, the objects will be serialized using the Compact Protocol.  Otherwise,
    // we'll use the binary protocol.
    // Note: the deserializer must be matching.
    ThriftSerializer(bool compact, int initial_buffer_size);

    // Serializes obj into result.  Result will contain a copy of the memory.
    template <class T>
    [[nodiscard]] Status serialize(T* obj, std::vector<uint8_t>* result) {
        uint32_t len = 0;
        uint8_t* buffer = nullptr;
        RETURN_IF_ERROR(serialize<T>(obj, &len, &buffer));
        result->resize(len);
        memcpy(&((*result)[0]), buffer, len);
        return Status::OK();
    }

    // serialize obj into a memory buffer.  The result is returned in buffer/len.  The
    // memory returned is owned by this object and will be invalid when another object
    // is serialized.
    template <class T>
    [[nodiscard]] Status serialize(T* obj, uint32_t* len, uint8_t** buffer) {
        try {
            _mem_buffer->resetBuffer();
            obj->write(_protocol.get());
        } catch (std::exception& e) {
            std::stringstream msg;
            msg << "Couldn't serialize thrift object:\n" << e.what();
            return Status::InternalError(msg.str());
        }

        _mem_buffer->getBuffer(buffer, len);
        return Status::OK();
    }

    template <class T>
    [[nodiscard]] Status serialize(T* obj, std::string* result) {
        try {
            _mem_buffer->resetBuffer();
            obj->write(_protocol.get());
        } catch (apache::thrift::TApplicationException& e) {
            std::stringstream msg;
            msg << "Couldn't serialize thrift object:\n" << e.what();
            return Status::InternalError(msg.str());
        }

        *result = _mem_buffer->getBufferAsString();
        return Status::OK();
    }

    template <class T>
    [[nodiscard]] Status serialize(T* obj) {
        try {
            _mem_buffer->resetBuffer();
            obj->write(_protocol.get());
        } catch (apache::thrift::TApplicationException& e) {
            std::stringstream msg;
            msg << "Couldn't serialize thrift object:\n" << e.what();
            return Status::InternalError(msg.str());
        }

        return Status::OK();
    }

    void get_buffer(uint8_t** buffer, uint32_t* length) { _mem_buffer->getBuffer(buffer, length); }

private:
    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> _mem_buffer;
    std::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
};

enum TProtocolType {
    // Use TCompactProtocol to deserialize msg
    COMPACT, // 0
    // Use TBinaryProtocol to deserialize msg
    BINARY, // 1
    // Use TJSONProtocol to deserialize msg
    JSON // 2
};

// Utility to create a protocol (deserialization) object for 'mem'.
std::shared_ptr<apache::thrift::protocol::TProtocol> create_deserialize_protocol(
        const std::shared_ptr<apache::thrift::transport::TMemoryBuffer>& mem, TProtocolType type);

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
[[nodiscard]] Status deserialize_thrift_msg(const uint8_t* buf, uint32_t* len, TProtocolType type,
                                            T* deserialized_msg) {
    // Deserialize msg bytes into c++ thrift msg using memory
    // transport. TMemoryBuffer is not const-safe, although we use it in
    // a const-safe way, so we have to explicitly cast away the const.
    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
            new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto = create_deserialize_protocol(tmem_transport, type);

    try {
        deserialized_msg->read(tproto.get());
    } catch (std::exception& e) {
        std::stringstream msg;
        msg << "couldn't deserialize thrift msg:\n" << e.what();
        return Status::InternalError(msg.str());
    } catch (...) {
        // TODO: Find the right exception for 0 bytes
        return Status::InternalError("Unknown exception");
    }

    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
    return Status::OK();
}

template <class T>
[[nodiscard]] Status deserialize_thrift_msg(const uint8_t* buf, uint32_t* len, const std::string& protocol,
                                            T* deserialized_msg) {
    if (protocol == "json") {
        return deserialize_thrift_msg<T>(buf, len, TProtocolType::JSON, deserialized_msg);
    } else if (protocol == "compact") {
        return deserialize_thrift_msg<T>(buf, len, TProtocolType::COMPACT, deserialized_msg);
    } else {
        return deserialize_thrift_msg<T>(buf, len, TProtocolType::BINARY, deserialized_msg);
    }
}

// Redirects all Thrift logging to VLOG(1)
void init_thrift_logging();

// Wait for a server that is running locally to start accepting
// connections, up to a maximum timeout
[[nodiscard]] Status wait_for_local_server(const ThriftServer& server, int num_retries, int retry_interval_ms);

// Wait for a server to start accepting connections, up to a maximum timeout
[[nodiscard]] Status wait_for_server(const std::string& host, int port, int num_retries, int retry_interval_ms);

// Utility method to print address as address:port
void t_network_address_to_string(const TNetworkAddress& address, std::string* out);

// Compares two TNetworkAddresses alphanumerically by their host:port
// string representation
bool t_network_address_comparator(const TNetworkAddress& a, const TNetworkAddress& b);

template <typename ThriftStruct>
ThriftStruct from_json_string(const std::string& json_val) {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    ThriftStruct ts;
    auto* buffer = new TMemoryBuffer((uint8_t*)json_val.c_str(), (uint32_t)json_val.size());
    std::shared_ptr<TTransport> trans(buffer);
    TJSONProtocol protocol(trans);
    ts.read(&protocol);
    return ts;
}

} // namespace starrocks
