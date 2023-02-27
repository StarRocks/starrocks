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

#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/spiller_path_provider.h"
#include "runtime/runtime_state.h"

namespace starrocks {
struct SpillFormatContext;
class SpillFormater;
class SpillerFactory;

class SpilledInputStream {
public:
    virtual ~SpilledInputStream() = default;
    virtual StatusOr<ChunkUniquePtr> read(SpillFormatContext& context) = 0;
    virtual bool is_ready() = 0;
    virtual void close() = 0;

    void mark_eof() { _eof = true; }
    bool eof() const { return _eof; }

private:
    std::atomic_bool _eof = false;
};

using SpilledInputStreamList = std::vector<std::shared_ptr<SpilledInputStream>>;

class SpillRestoreTask {
public:
    virtual ~SpillRestoreTask() = default;
    virtual Status do_read(SpillFormatContext& context) = 0;

    bool eos() { return _eos; }
    void mark_eos() { _eos = true; }

private:
    std::atomic_bool _eos{};
};
using SpillRestoreTaskPtr = std::shared_ptr<SpillRestoreTask>;

// spilled input stream with restore tasks
struct InputStreamWithTasks {
    std::shared_ptr<SpilledInputStream> stream;
    std::vector<SpillRestoreTaskPtr> tasks;
};

// A collection of all spilled files in a partition.
// A stream can be constructed from a SpilledFileGroup
class SpilledFileGroup {
public:
    SpilledFileGroup(const SpillFormater& formater) : _formater(formater) {}

    // not thread safe
    void append_file(std::shared_ptr<SpillFile> file) { _files.emplace_back(std::move(file)); }

    StatusOr<InputStreamWithTasks> as_flat_stream(std::weak_ptr<SpillerFactory> factory);

    StatusOr<InputStreamWithTasks> as_sorted_stream(std::weak_ptr<SpillerFactory> factory, RuntimeState* state,
                                                    const SortExecExprs* sort_exprs, const SortDescs* descs);

    const std::vector<std::shared_ptr<SpillFile>>& files() const { return _files; }

private:
    const SpillFormater& _formater;
    std::vector<std::shared_ptr<SpillFile>> _files;
};

} // namespace starrocks