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

#include "connector/hive/paimon/paimon_cpp_loader.h"

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <string>

#include "base/utility/dynamic_util.h"
#include "connector/hive/paimon/paimon_chunk_sink.h"
#include "connector/hive/paimon/paimon_cpp_shim_api.h"
#include "fmt/format.h"

namespace starrocks {
namespace {

constexpr const char* kShimLibraryName = "libstarrocks_paimon.so";

std::string shim_library_path() {
    const char* home = std::getenv("STARROCKS_HOME");
    if (home == nullptr) {
        return kShimLibraryName;
    }
    return std::string(home) + "/lib/paimon-cpp-lib/" + kShimLibraryName;
}

// Never dlclose a usable shim: BE objects keep its vtables for their entire lifetime.
std::mutex load_mutex;
void* shim_handle = nullptr; // Protected by load_mutex.
std::atomic<StarRocksPaimonCreateScannerFn> create_scanner_fn{nullptr};
std::atomic<StarRocksPaimonCreateSinkFn> create_sink_fn{nullptr};
std::atomic<StarRocksPaimonBucketIdsFn> bucket_ids_fn{nullptr};

template <typename Fn>
StatusOr<Fn> load_capability(std::atomic<Fn>& cached, const char* symbol, bool reader) {
    auto fn = cached.load(std::memory_order_acquire);
    if (fn != nullptr) return fn;
    std::lock_guard<std::mutex> guard(load_mutex);
    fn = cached.load(std::memory_order_acquire);
    if (fn != nullptr) return fn;
    if (shim_handle == nullptr) {
        const auto path = shim_library_path();
        if (Status st = dynamic_open(path.c_str(), &shim_handle); !st.ok()) {
            shim_handle = nullptr;
            return Status::InternalError(fmt::format(
                    "Paimon native {} is unavailable: {}. Install the matching BE's paimon-cpp-lib package.{}",
                    reader ? "reader" : "writer", st.message(),
                    reader ? " Set paimon_reader_mode=JNI to use the JNI reader instead." : ""));
        }
    }
    void* address = nullptr;
    if (Status st = dynamic_lookup(shim_handle, symbol, &address); !st.ok()) {
        // Do not tear down the independently available scanner when an older shim lacks writes.
        return Status::NotSupported(fmt::format(
                "Paimon shim lacks {} capability ({}). Install the matching BE's paimon-cpp-lib package: {}",
                reader ? "reader" : "native writer v1", symbol, st.message()));
    }
    fn = reinterpret_cast<Fn>(address);
    cached.store(fn, std::memory_order_release);
    return fn;
}

class DeferredPaimonSink final : public connector::ConnectorSink {
public:
    DeferredPaimonSink(std::shared_ptr<connector::PaimonChunkSinkContext> context, int32_t driver_id)
            : _context(std::move(context)), _driver_id(driver_id) {}

    Status init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                connector::SinkMemoryManager* memory) override {
        auto result = load_capability(create_sink_fn, STARROCKS_PAIMON_CREATE_SINK_SYMBOL, false);
        if (!result.ok()) return _status = result.status();
        _delegate.reset(result.value()(_context, _driver_id));
        if (_delegate == nullptr) return _status = Status::InternalError("Paimon shim returned a null writer");
        return _status = _delegate->init(poller, profile, memory);
    }
    Status add(const ChunkPtr& chunk) override {
        return _delegate ? _delegate->add(chunk) : uninitialized_status();
    }
    Status finish() override { return _delegate ? _delegate->finish() : uninitialized_status(); }
    void rollback() override {
        if (_delegate) _delegate->rollback();
    }
    bool is_finished() override { return _delegate ? _delegate->is_finished() : true; }
    Status status() override { return _delegate ? _delegate->status() : uninitialized_status(); }
    connector::SinkOperatorMemoryManager* op_mem_mgr() const override {
        return _delegate ? _delegate->op_mem_mgr() : nullptr;
    }

private:
    Status uninitialized_status() const {
        return _status.ok() ? Status::InternalError("Paimon sink is not initialized") : _status;
    }
    std::shared_ptr<connector::PaimonChunkSinkContext> _context;
    int32_t _driver_id;
    std::unique_ptr<connector::ConnectorSink> _delegate;
    Status _status;
};

} // namespace

StatusOr<HdfsScanner*> create_paimon_cpp_scanner() {
    ASSIGN_OR_RETURN(auto fn, load_capability(create_scanner_fn, STARROCKS_PAIMON_CREATE_SCANNER_SYMBOL, true));
    return fn();
}

std::unique_ptr<connector::ConnectorSink> create_paimon_cpp_chunk_sink(
        std::shared_ptr<connector::PaimonChunkSinkContext> context, int32_t driver_id) {
    return std::make_unique<DeferredPaimonSink>(std::move(context), driver_id);
}

StatusOr<std::vector<int32_t>> calculate_paimon_cpp_bucket_ids(
        const TPaimonTableSink& sink, const ChunkPtr& chunk, const std::vector<ExprContext*>& output_exprs) {
    ASSIGN_OR_RETURN(auto fn, load_capability(bucket_ids_fn, STARROCKS_PAIMON_BUCKET_IDS_SYMBOL, false));
    return fn(sink, chunk, output_exprs);
}

StatusOr<std::unique_ptr<connector::ConnectorSink>> connector::PaimonChunkSinkProvider::create_sink(int32_t driver_id) {
    return create_paimon_cpp_chunk_sink(_context, driver_id);
}

} // namespace starrocks
