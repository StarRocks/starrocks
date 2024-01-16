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

#include <future>
#include <common/status.h>
#include <column/chunk.h>
#include <runtime/runtime_state.h>
#include <boost/thread/future.hpp>

#include "rolling_file_writer.h"

namespace starrocks::pipeline {

class ConnectorChunkSink {
public:
    // define a customized Future to composite mutliple std::future(s)
    // struct Future {
    //     bool immediate_ready;
    //     Status immediate_ready_status;
    //     std::vector<std::future<Status>> futures;
    // };

    virtual ~ConnectorChunkSink() = 0;
    virtual std::future<Status> add(ChunkPtr chunk) = 0;
    virtual void abort() = 0;
};

class FilesChunkSink : public ConnectorChunkSink {
public:
    FilesChunkSink();

    std::future<Status> add(ChunkPtr chunk) override;

    void abort() override;

private:
    bool _partitioned_write;
    int _driver_id;

    std::map<std::string, int> _partition_writer_sequence;
    std::map<std::string, std::unique_ptr<FileWriter>> _partition_writers;

    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";
};

}



