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

#include "connector/hive/paimon/paimon_chunk_sink.h"
#include "connector/hive/paimon/paimon_cpp_shim_api.h"
#include "fmt/format.h"
#include "runtime/paimon_cpp_library.h"

namespace starrocks {
namespace {

template <typename Fn>
StatusOr<Fn> load_capability(PaimonCppLibrary::Capability capability, const char* symbol, bool reader) {
    auto result = PaimonCppLibrary::instance().get_symbol(capability);
    if (!result.ok()) {
        if (result.status().is_not_supported()) {
            return Status::NotSupported(fmt::format(
                    "Paimon shim lacks {} capability ({}). Install the matching BE's paimon-cpp-lib package: {}",
                    reader ? "reader" : "native writer v1", symbol, result.status().message()));
        }
        return Status::InternalError(
                fmt::format("Paimon native {} is unavailable: {}. Install the matching BE's paimon-cpp-lib package.{}",
                            reader ? "reader" : "writer", result.status().message(),
                            reader ? " Set paimon_reader_mode=JNI to use the JNI reader instead." : ""));
    }
    return reinterpret_cast<Fn>(result.value());
}

class DeferredPaimonSink final : public connector::ConnectorSink {
public:
    DeferredPaimonSink(std::shared_ptr<connector::PaimonChunkSinkContext> context, int32_t driver_id)
            : _context(std::move(context)), _driver_id(driver_id) {}

    Status init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                connector::SinkMemoryManager* memory) override {
        auto result = load_capability<StarRocksPaimonCreateSinkFn>(PaimonCppLibrary::Capability::SINK,
                                                                   STARROCKS_PAIMON_CREATE_SINK_SYMBOL, false);
        if (!result.ok()) return _status = result.status();
        _delegate.reset(result.value()(_context, _driver_id));
        if (_delegate == nullptr) return _status = Status::InternalError("Paimon shim returned a null writer");
        return _status = _delegate->init(poller, profile, memory);
    }
    Status add(const ChunkPtr& chunk) override { return _delegate ? _delegate->add(chunk) : uninitialized_status(); }
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
    ASSIGN_OR_RETURN(auto fn,
                     load_capability<StarRocksPaimonCreateScannerFn>(PaimonCppLibrary::Capability::SCANNER,
                                                                     STARROCKS_PAIMON_CREATE_SCANNER_SYMBOL, true));
    return fn();
}

std::unique_ptr<connector::ConnectorSink> create_paimon_cpp_chunk_sink(
        std::shared_ptr<connector::PaimonChunkSinkContext> context, int32_t driver_id) {
    return std::make_unique<DeferredPaimonSink>(std::move(context), driver_id);
}

StatusOr<std::unique_ptr<connector::ConnectorSink>> connector::PaimonChunkSinkProvider::create_sink(int32_t driver_id) {
    return create_paimon_cpp_chunk_sink(_context, driver_id);
}

} // namespace starrocks
