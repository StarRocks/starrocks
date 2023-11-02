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

#include <glog/logging.h>

#include <cstdint>
#include <vector>

#include "gen_cpp/RuntimeFilter_types.h"
#include "util/guard.h"

namespace starrocks {
class RuntimeFilterLayout {
public:
    void init(const TRuntimeFilterLayout& layout);
    void init(int filter_id, const std::vector<int32_t>& bucketseq_to_instance);
    int filter_id() const { return _filter_id; }
    TRuntimeFilterLayoutMode::type local_layout() const { return _local_layout; }
    TRuntimeFilterLayoutMode::type global_layout() const { return _global_layout; }
    bool pipeline_level_multi_partitioned() const { return _pipeline_level_multi_partitioned; }
    int num_instances() const { return _num_instances; }
    int num_drivers_per_instance() const { return _num_drivers_per_instance; };
    const std::vector<int32_t>& bucketseq_to_instance() const { return _bucketseq_to_instance; }

    const std::vector<int32_t>& bucketseq_to_driverseq() const { return _bucketseq_to_driverseq; }
    const std::vector<int32_t>& bucketseq_to_partition() const { return _bucketseq_to_partition; }

protected:
    int _filter_id;
    TRuntimeFilterLayoutMode::type _local_layout;
    TRuntimeFilterLayoutMode::type _global_layout;
    bool _pipeline_level_multi_partitioned = false;
    int _num_instances;
    int _num_drivers_per_instance;
    std::vector<int32_t> _bucketseq_to_instance;
    std::vector<int32_t> _bucketseq_to_driverseq;
    std::vector<int32_t> _bucketseq_to_partition;
};

class WithLayoutMixin {
public:
    void init(const TRuntimeFilterDescription& desc);
    const RuntimeFilterLayout& layout() const { return _layout; }

protected:
    RuntimeFilterLayout _layout;
};
static constexpr uint32_t BUCKET_ABSENT = 2147483647;
VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutSingletonGuard, layout_is_singleton,
            TRuntimeFilterLayoutMode::SINGLETON);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutPipelineShuffleGuard, layout_is_pipeline_shuffle,
            TRuntimeFilterLayoutMode::PIPELINE_SHUFFLE);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutGlobalShuffle2LGuard, layout_is_global_shuffle_2l,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_2L);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutGlobalShuffle1LGuard, layout_is_global_shuffle_1l,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutShuffleGuard, layout_is_shuffle,
            TRuntimeFilterLayoutMode::PIPELINE_SHUFFLE, TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_2L);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutPipelineBucketLXGuard, layout_is_pipeline_bucket_lx,
            TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutPipelineBucketGuard, layout_is_pipeline_bucket,
            TRuntimeFilterLayoutMode::PIPELINE_BUCKET);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutGlobalBucket1LGuard, layout_is_global_bucket_1l,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutGlobalBucket2LLXlGuard, layout_is_global_bucket_2l_lx,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L_LX);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutGlobalBucket2LGuard, layout_is_global_bucket_2l,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L);

VALUE_GUARD(TRuntimeFilterLayoutMode::type, LayoutBucketGuard, layout_is_bucket,
            TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX, TRuntimeFilterLayoutMode::PIPELINE_BUCKET,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L, TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L_LX,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L)

constexpr bool is_multi_partitioned(TRuntimeFilterLayoutMode::type mode) {
    return mode != TRuntimeFilterLayoutMode::NONE && mode != TRuntimeFilterLayoutMode::SINGLETON;
}

template <template <TRuntimeFilterLayoutMode::type> typename F, typename... Args>
auto dispatch_layout(bool global, const RuntimeFilterLayout& layout, Args&&... args) {
    TRuntimeFilterLayoutMode::type layout_mode = global ? layout.global_layout() : layout.local_layout();
    switch (layout_mode) {
    case TRuntimeFilterLayoutMode::SINGLETON:
        return F<TRuntimeFilterLayoutMode::SINGLETON>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::PIPELINE_SHUFFLE:
        return F<TRuntimeFilterLayoutMode::PIPELINE_SHUFFLE>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_2L:
        return F<TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_2L>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L:
        return F<TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX:
        return F<TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::PIPELINE_BUCKET:
        return F<TRuntimeFilterLayoutMode::PIPELINE_BUCKET>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L_LX:
        return F<TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L_LX>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L:
        return F<TRuntimeFilterLayoutMode::GLOBAL_BUCKET_2L>()(layout, std::forward<Args>(args)...);
    case TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L:
        return F<TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L>()(layout, std::forward<Args>(args)...);
    default:
        CHECK(false) << "Unknown type: " << layout_mode;
        __builtin_unreachable();
    }
}

} // namespace starrocks
