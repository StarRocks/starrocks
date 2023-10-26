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

#include "exprs/runtime_filter_layout.h"

#include <glog/logging.h>

namespace starrocks {
void RuntimeFilterLayout::init(const TRuntimeFilterLayout& layout) {
    this->_filter_id = layout.filter_id;
    this->_local_layout = layout.local_layout;
    this->_global_layout = layout.global_layout;
    this->_pipeline_level_multi_partitioned = layout.pipeline_level_multi_partitioned;
    this->_num_instances = layout.num_instances;
    this->_num_drivers_per_instance = layout.num_drivers_per_instance;
    if (layout.__isset.bucketseq_to_instance) {
        this->_bucketseq_to_instance = layout.bucketseq_to_instance;
    }
    if (layout.__isset.bucketseq_to_driverseq) {
        this->_bucketseq_to_driverseq = layout.bucketseq_to_driverseq;
    }
    if (layout.__isset.bucketseq_to_partition) {
        this->_bucketseq_to_partition = layout.bucketseq_to_partition;
    }
}

void RuntimeFilterLayout::init(int filter_id, const std::vector<int32_t>& bucketseq_to_instance) {
    this->_filter_id = filter_id;
    this->_local_layout = TRuntimeFilterLayoutMode::SINGLETON;
    this->_global_layout = bucketseq_to_instance.empty() ? TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L
                                                         : TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L;
    this->_pipeline_level_multi_partitioned = false;
    this->_num_instances = 0;
    this->_num_drivers_per_instance = 0;
    this->_bucketseq_to_instance = bucketseq_to_instance;
}

void WithLayoutMixin::init(const TRuntimeFilterDescription& desc) {
    if (desc.__isset.layout) {
        _layout.init(desc.layout);
    } else {
        if (desc.__isset.bucketseq_to_instance) {
            _layout.init(desc.filter_id, desc.bucketseq_to_instance);
        } else {
            _layout.init(desc.filter_id, {});
        }
    }
}

} // namespace starrocks