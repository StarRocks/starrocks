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
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "tenann/factory/index_factory.h"

namespace starrocks {
// =============== TenAnnIndexBuilderProxy =============

Status TenAnnIndexBuilderProxy::init() {
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(_tablet_index, std::map<std::string, std::string>{}))

    RETURN_IF_ERROR(success_once(_init_once, []() {
                        tenann::OmpSetNumThreads(config::config_tenann_build_concurrency);
                        return Status::OK();
                    }).status());

    CHECK(meta.common_params().contains("dim")) << "Dim is a critical common param";
    _dim = meta.common_params()["dim"];

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
        if (_src_is_nullable) {
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

Status TenAnnIndexBuilderProxy::add(const Column& data) {
    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(data.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(data.size()),
                                                .elem_type = tenann::kFloatType};
        if (data.is_array() && data.size() != 0) {
            const auto& cur_array = down_cast<const ArrayColumn&>(data);
            auto offsets = cur_array.offsets();
            size_t last_offset = 0;
            auto* offsets_data = reinterpret_cast<uint32_t*>(offsets.mutable_raw_data());
            for (size_t i = 1; i < offsets.size(); i++) {
                if (_dim != (offsets_data[i] - last_offset)) {
                    LOG(WARNING) << "index dim: " << _dim << ", written dim: " << offsets_data[i] - last_offset;
                    return Status::InvalidArgument("The dimensions of the vector written are inconsistent");
                }
                last_offset = offsets_data[i];
            }
        }

        _index_builder->Add({vector_view});
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenAnnIndexBuilderProxy::add(const Column& data, const Column& null_map, const size_t offset) {
    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(data.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(data.size()),
                                                .elem_type = tenann::kFloatType};
        if (data.is_array() && data.size() != 0) {
            const auto& cur_array = down_cast<const ArrayColumn&>(data);
            auto offsets = cur_array.offsets();
            size_t last_offset = 0;
            auto* offsets_data = reinterpret_cast<uint32_t*>(offsets.mutable_raw_data());
            for (size_t i = 1; i < offsets.size(); i++) {
                if (_dim != (offsets_data[i] - last_offset)) {
                    LOG(WARNING) << "index dim: " << _dim << ", written dim: " << offsets_data[i] - last_offset;
                    return Status::InvalidArgument("The dimensions of the vector written are inconsistent");
                }
                last_offset = offsets_data[i];
            }
        }
        std::vector<int64_t> row_ids(data.size());
        std::iota(row_ids.begin(), row_ids.end(), offset);
        _index_builder->Add({vector_view}, row_ids.data(), null_map.raw_data());

    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenAnnIndexBuilderProxy::write(const Column& data) {
    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(data.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(data.size()),
                                                .elem_type = tenann::kFloatType};

        _index_builder->Add({vector_view});
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenAnnIndexBuilderProxy::write(const Column& data, const Column& null_map) {
    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(data.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(data.size()),
                                                .elem_type = tenann::kFloatType};

        std::vector<int64_t> row_ids(data.size());
        std::iota(row_ids.begin(), row_ids.end(), 0);
        _index_builder->Add({vector_view}, row_ids.data(), null_map.raw_data());

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

void TenAnnIndexBuilderProxy::close() {
    if (_index_builder && !_index_builder->is_closed()) {
        _index_builder->Close();
    }
}
} // namespace starrocks
#endif
