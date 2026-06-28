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

#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace starrocks {

class LoadStreamMgr;
class StreamLoadContext;
class TUniqueId;

// Used to register all streams in process so that other modules can get them.
class StreamContextMgr {
public:
    explicit StreamContextMgr(LoadStreamMgr* load_stream_mgr);
    ~StreamContextMgr();

    Status put(const std::string& id, StreamLoadContext* stream);
    StreamLoadContext* get(const std::string& id);
    void remove(const std::string& id);
    std::vector<std::string> get_ids();

    Status create_channel_context(const std::string& label, int channel_id, const std::string& db_name,
                                  const std::string& table_name, int32_t format, StreamLoadContext*& ctx,
                                  const TUniqueId& load_id, long txn_id);
    Status put_channel_context(const std::string& label, const std::string& table_name, int channel_id,
                               StreamLoadContext* ctx);
    StreamLoadContext* get_channel_context(const std::string& label, const std::string& table_name, int channel_id);
    void remove_channel_context(StreamLoadContext* ctx);
    Status finish_body_sink(const std::string& label, const std::string& table_name, int channel_id);

private:
    LoadStreamMgr* _load_stream_mgr;
    std::mutex _lock;
    std::unordered_map<std::string, StreamLoadContext*> _stream_map;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<int, StreamLoadContext*>>>
            _channel_stream_map;
};

} // namespace starrocks
