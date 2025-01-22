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

#ifdef WITH_TENANN

#include "storage/index/vector/tenann/tenann_index_builder.h"

#include <tenann/util/threads.h>

#include "column/array_column.h"
#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "tenann/factory/index_factory.h"

namespace starrocks {

// =============== TenAnnIndexBuilderProxy =============

Status TenAnnIndexBuilderProxy::init() {
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(_tablet_index, std::map<std::string, std::string>{}))

    RETURN_IF_ERROR(success_once(_init_once, []() {
                        tenann::OmpSetNumThreads(config::config_vector_index_build_concurrency);
                        return Status::OK();
                    }).status());

    const auto& params = meta.common_params();

    if (!params.contains(index::vector::DIM)) {
        return Status::InvalidArgument("dim is needed because it's a critical common param");
    }
    _dim = params[index::vector::DIM];

    if (!params.contains(index::vector::METRIC_TYPE)) {
        return Status::InvalidArgument("metric_type is needed because it's a critical common param");
    }
    _is_input_normalized = params.contains(index::vector::IS_VECTOR_NORMED) &&
                           params[index::vector::IS_VECTOR_NORMED] &&
                           params[index::vector::METRIC_TYPE] == tenann::MetricType::kCosineSimilarity;

    auto meta_copy = meta;
    if (meta.index_type() == tenann::IndexType::kFaissIvfPq && config::enable_vector_index_block_cache) {
        meta_copy.index_writer_options()[tenann::IndexWriterOptions::write_index_cache_key] = false;
    } else {
        meta_copy.index_writer_options()[tenann::IndexWriterOptions::write_index_cache_key] = true;
    }

    try {
        // build and write index
        _index_builder = tenann::IndexFactory::CreateBuilderFromMeta(meta_copy);
        _index_builder->index_writer()->SetIndexCache(tenann::IndexCache::GetGlobalInstance());
        if (_is_element_nullable) {
            _index_builder->EnableCustomRowId();
        }
        _index_builder->Open(_index_path);

        if (!_index_builder->is_opened()) {
            return Status::InternalError("Can not open index path in " + _index_path);
        }
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }

    return Status::OK();
}

template <bool is_input_normalized>
static Status valid_input_vector(const ArrayColumn& input_column, const size_t index_dim,
                                 const uint8_t* is_element_nulls) {
    if (input_column.empty()) {
        return Status::OK();
    }

    const size_t num_rows = input_column.size();
    const auto* offsets = reinterpret_cast<const uint32_t*>(input_column.offsets().raw_data());
    const auto* nums = reinterpret_cast<const float*>(input_column.elements().raw_data());

    for (size_t i = 0; i < num_rows; i++) {
        const size_t input_dim = offsets[i + 1] - offsets[i];

        if (input_dim != index_dim) {
            return Status::InvalidArgument(
                    strings::Substitute("The dimensions of the vector written are inconsistent, index dim is "
                                        "$0 but data dim is $1",
                                        index_dim, input_dim));
        }

        if constexpr (is_input_normalized) {
            double sum = 0;
            for (int j = 0; j < input_dim; j++) {
                const size_t offset = offsets[i] + j;
                if (!is_element_nulls[offset]) {
                    sum += nums[offset] * nums[offset];
                }
            }
            if (std::abs(sum - 1) > 1e-3) {
                return Status::InvalidArgument(
                        "The input vector is not normalized but `metric_type` is cosine_similarity and "
                        "`is_vector_normed` is true");
            }
        }
    }

    return Status::OK();
}

Status TenAnnIndexBuilderProxy::add(const Column& array_column, const size_t offset) {
    DCHECK(array_column.is_array());
    const auto& array_col = down_cast<const ArrayColumn&>(array_column);

    DCHECK(array_col.elements_column()->is_nullable());
    const auto& nullable_elements = down_cast<const NullableColumn&>(array_col.elements());
    const auto& is_element_nulls = nullable_elements.null_column_ref();
    const uint8_t* is_element_nulls_data = is_element_nulls.raw_data();

    if (_is_input_normalized) {
        RETURN_IF_ERROR(valid_input_vector<true>(array_col, _dim, is_element_nulls_data));
    } else {
        RETURN_IF_ERROR(valid_input_vector<false>(array_col, _dim, is_element_nulls_data));
    }

    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(array_col.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(array_col.size()),
                                                .elem_type = tenann::kFloatType};
        std::vector<int64_t> row_ids(array_col.size());
        std::iota(row_ids.begin(), row_ids.end(), offset);
        _index_builder->Add({vector_view}, row_ids.data(), is_element_nulls_data);
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenAnnIndexBuilderProxy::flush() {
    try {
        _index_builder->Flush();
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

void TenAnnIndexBuilderProxy::close() const {
    if (_index_builder && !_index_builder->is_closed()) {
        _index_builder->Close();
    }
}

} // namespace starrocks
#endif
