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

#include "index_builder.h"

#include <tenann/common/error.h>
#include <tenann/factory/index_factory.h>
#include <tenann/index/index_writer.h>

#include <boost/algorithm/string/join.hpp>
#include <memory>

#include "column/array_column.h"
#include "fs/fs_util.h"
#include "storage/tenann_index_utils.h"

namespace starrocks {

// =============== TextIndexBuilder =============
Status TextIndexBuilder::init() {
    ASSIGN_OR_RETURN(_output_index_file, fs::new_writable_file(_index_path))
    return Status::OK();
}

Status TextIndexBuilder::write(const Column& data) {
    const auto& data_column = down_cast<const ArrayColumn&>(data);

    for (size_t i = 0; i < data_column.size(); i++) {
        std::stringstream ss;
        std::vector<std::string> data_line;
        size_t element_size = data_column.get(i).get_array().size();
        for (int j = 0; j < element_size; j++) {
            data_line.emplace_back(std::to_string(data_column.get(i).get_array().at(j).get_float()));
        }
        ss << boost::algorithm::join(data_line, ",") << " : " << i;
        if (i < data_column.size() - 1) {
            ss << "\n";
        }
        _output_index_file->append(ss.str());
    }
    return Status::OK();
}

Status TextIndexBuilder::write(const Column& data, const Column& invalid_row_id_column) {
    const auto& data_column = down_cast<const ArrayColumn&>(data);
    const auto& null_column = down_cast<const NullColumn&>(invalid_row_id_column);

    for (size_t i = 0; i < data_column.size(); i++) {
        std::stringstream ss;
        std::vector<std::string> data_line;
        size_t element_size = data_column.get(i).get_array().size();
        if (null_column.get(i).get_int8()) {
            continue;
        }
        for (int j = 0; j < element_size; j++) {
            data_line.emplace_back(std::to_string(data_column.get(i).get_array().at(j).get_float()));
        }
        ss << boost::algorithm::join(data_line, ",") << " : " << i;
        if (i < data_column.size() - 1) {
            ss << "\n";
        }
        _output_index_file->append(ss.str());
    }
    return Status::OK();
}

Status TextIndexBuilder::flush() {
    _output_index_file->flush(WritableFile::FLUSH_SYNC);
    return Status::OK();
}

Status TextIndexBuilder::close() {
    _output_index_file->close();
    return Status::OK();
}

// =============== TenAnnIndexBuilderProxy =============

Status TenAnnIndexBuilderProxy::init() {
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(_tablet_index))

    CHECK(meta.common_params().contains("dim")) << "Dim is a critical common param";
    _dim = meta.common_params()["dim"];

    // build and write index
    _index_builder = tenann::IndexFactory::CreateBuilderFromMeta(meta);
    tenann::IndexWriterRef index_writer = tenann::IndexFactory::CreateWriterFromMeta(meta);
    // _index_builder->SetIndexWriter(index_writer).SetIndexCache(tenann::IndexCache::GetGlobalInstance());

    return Status::OK();
}

Status TenAnnIndexBuilderProxy::write(const Column& data) {
    try {
        auto vector_view = tenann::ArraySeqView{.data = const_cast<uint8_t*>(data.raw_data()),
                                                .dim = _dim,
                                                .size = static_cast<uint32_t>(data.size()),
                                                .elem_type = tenann::kFloatType};

        // _index_builder->Build({vector_view});
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenAnnIndexBuilderProxy::write(const Column& data, const Column& row_id_column) {
    return Status::InternalError("Not support vector columns contains null");
}

Status TenAnnIndexBuilderProxy::flush() {
    try {
        // _index_builder->WriteIndex(_index_path, true);
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

} // namespace starrocks