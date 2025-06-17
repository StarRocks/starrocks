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

#include "storage/index/vector/vector_index_writer.h"

#include "fs/fs_util.h"
#include "util/runtime_profile.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

void VectorIndexWriter::create(const std::shared_ptr<TabletIndex>& tablet_index,
                               const std::string& vector_index_file_path, bool is_element_nullable,
                               std::unique_ptr<VectorIndexWriter>* res) {
    *res = std::make_unique<VectorIndexWriter>(tablet_index, vector_index_file_path, is_element_nullable);
}

Status VectorIndexWriter::init() {
    auto index_type_iter = _tablet_index->common_properties().find("index_type");
    if (index_type_iter->second != "ivfpq") {
        _start_vector_index_build_threshold = 0;
        return Status::OK();
    }

    auto find_result = _tablet_index->common_properties().find("index_build_threshold");
    if (find_result != _tablet_index->common_properties().end()) {
        _start_vector_index_build_threshold = std::atoi(find_result->second.c_str());
    }
    return Status::OK();
}

Status VectorIndexWriter::append(const Column& src) {
    int64_t duration = 0;
    {
        SCOPED_RAW_TIMER(&duration);

        if (_index_builder == nullptr) {
            if (_row_size + src.size() >= _start_vector_index_build_threshold) {
                RETURN_IF_ERROR(_prepare_index_builder());
            } else {
                if (_buffer_column == nullptr) {
                    _buffer_column = src.clone();
                } else {
                    _buffer_column->append(src, 0, src.size());
                }
            }
        }

        if (_index_builder != nullptr) {
            RETURN_IF_ERROR(_append_data(src, _next_row_id));
        }
    }

    _next_row_id += src.size();
    _buffer_size += src.byte_size();
    _row_size += src.size();

    return Status::OK();
}

Status VectorIndexWriter::finish(uint64_t* index_size) {
    if (!_next_row_id) {
        return Status::OK();
    }

    int64_t duration = 0;
    {
        SCOPED_RAW_TIMER(&duration);

        if (_index_builder.get() != nullptr) {
            // flush with index
            RETURN_IF_ERROR(_index_builder->flush());
        } else {
            // flush with empty mark
            RETURN_IF_ERROR(VectorIndexBuilder::flush_empty(_vector_index_file_path));
        }
        if (index_size) {
            ASSIGN_OR_RETURN(auto file_ptr, fs::new_random_access_file(_vector_index_file_path))
            ASSIGN_OR_RETURN(auto index_file_size, file_ptr->get_size())
            *index_size += index_file_size;
        }
    }

    return Status::OK();
}

uint64_t VectorIndexWriter::size() const {
    return _row_size;
}

uint64_t VectorIndexWriter::estimate_buffer_size() const {
    return _buffer_size;
}

Status VectorIndexWriter::_prepare_index_builder() {
    ASSIGN_OR_RETURN(auto index_builder_type,
                     VectorIndexBuilderFactory::get_index_builder_type_from_config(_tablet_index))
    ASSIGN_OR_RETURN(_index_builder,
                     VectorIndexBuilderFactory::create_index_builder(_tablet_index, _vector_index_file_path,
                                                                     index_builder_type, _is_element_nullable));
    RETURN_IF_ERROR(_index_builder->init());

    if (_buffer_column != nullptr) {
        RETURN_IF_ERROR(_append_data(*_buffer_column, 0));
        _buffer_column.reset();
    }
    return Status::OK();
}

Status VectorIndexWriter::_append_data(const Column& src, size_t offset) {
    DCHECK(src.is_array());
    RETURN_IF_ERROR(_index_builder->add(src, offset));
    return Status::OK();
}

} // namespace starrocks
