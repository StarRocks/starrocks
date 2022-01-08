// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/broker_writer.h

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

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "exec/file_writer.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class ExecEnv;
class TBrokerRangeDesc;
class TNetworkAddress;

// Reader of broker file
class BrokerWriter : public FileWriter {
public:
    BrokerWriter(ExecEnv* env, const std::vector<TNetworkAddress>& broker_addresses,
                 const std::map<std::string, std::string>& properties, std::string path, int64_t start_offset);
    ~BrokerWriter() override;

    Status open() override;

    Status write(const uint8_t* buf, size_t buf_len, size_t* written_len) override;

    Status close() override;

private:
    ExecEnv* _env;
    const std::vector<TNetworkAddress>& _addresses;
    const std::map<std::string, std::string>& _properties;
    std::string _path;
    int64_t _cur_offset;

    bool _is_closed;
    TBrokerFD _fd;

    // TODO: use for retry if one broker down
    int _addr_idx;
};

} // end namespace starrocks
