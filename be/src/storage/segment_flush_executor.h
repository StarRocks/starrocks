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

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "storage/olap_define.h"
#include "util/threadpool.h"

namespace brpc {
class Controller;
}

namespace google::protobuf {
class Closure;
}

namespace starrocks {

class DataDir;
class ExecEnv;
class PTabletWriterAddSegmentRequest;
class PTabletWriterAddSegmentResult;
class ThreadPoolToken;

class DeltaWriter;

class SegmentFlushToken {
public:
    SegmentFlushToken(std::unique_ptr<ThreadPoolToken> flush_pool_token,
                      std::shared_ptr<starrocks::DeltaWriter> delta_writer);

    Status submit(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                  PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done);

    void cancel();

    void wait();

private:
    std::unique_ptr<ThreadPoolToken> _flush_token;
    std::shared_ptr<DeltaWriter> _writer;
};

class SegmentFlushExecutor {
public:
    SegmentFlushExecutor() = default;
    ~SegmentFlushExecutor() = default;

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    Status init(const std::vector<DataDir*>& data_dirs);

    Status update_max_threads(int max_threads);

    std::unique_ptr<SegmentFlushToken> create_flush_token(
            const std::shared_ptr<starrocks::DeltaWriter>& delta_writer,
            ThreadPool::ExecutionMode execution_mode = ThreadPool::ExecutionMode::CONCURRENT);

private:
    std::unique_ptr<ThreadPool> _flush_pool;
};

} // namespace starrocks
