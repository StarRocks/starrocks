// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>

#include "column/chunk.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "util/brpc_stub_cache.h"

namespace starrocks::pipeline {
using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;

struct TransmitChunkInfo {
    // For BUCKET_SHUFFLE_HASH_PARTITIONED, multiple channels may be related to
    // a same exchange source fragment instance, so we should use fragment_instance_id
    // of the destination as the key of destination instead of channel_id.
    TUniqueId fragment_instance_id;
    doris::PBackendService_Stub* brpc_stub;
    PTransmitChunkParamsPtr params;
    butil::IOBuf attachment;
    int64_t attachment_physical_bytes;
};

// TimeTrace is introduced to estimate time more accurately.
// For every update
// 1. times will be increased by 1.
// 2. sample time will be accumulated to accumulated_time.
// 3. sample concurrency will be accumulated to accumulated_concurrency.
// So we can get the average time of each direction by
// `average_concurrency = accumulated_concurrency / times`
// `average_time = accumulated_time / average_concurrency`
struct TimeTrace {
    int32_t times = 0;
    int64_t accumulated_time = 0;
    int32_t accumulated_concurrency = 0;

    void update(int64_t time, int32_t concurrency) {
        times++;
        accumulated_time += time;
        accumulated_concurrency += concurrency;
    }
};

class ExchangeSinkBuffer {
public:
    virtual ~ExchangeSinkBuffer() = default;

    virtual void add_request(TransmitChunkInfo& request) = 0;

    virtual bool is_full() const = 0;

    virtual void set_finishing() = 0;

    virtual bool is_finished() const = 0;

    // Add counters to the given profile
    virtual void update_profile(RuntimeProfile* profile) = 0;

    virtual void cancel_one_sinker() = 0;
};

} // namespace starrocks::pipeline