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

#include "common/config_vector_index_fwd.h"
#include "common/runtime_profile.h"
#include "fs/fs_util.h"
#include "runtime/starrocks_metrics.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/vector_index_file_writer.h"

namespace starrocks {

VectorIndexWriter::VectorIndexWriter(std::shared_ptr<TabletIndex> tablet_index, std::string vector_index_file_path,
                                     bool is_element_nullable)
        : _tablet_index(std::move(tablet_index)),
          _vector_index_file_path(std::move(vector_index_file_path)),
          _start_vector_index_build_threshold(config::config_vector_index_default_build_threshold),
          _is_element_nullable(is_element_nullable) {
    // Element of array column must be nullable.
    DCHECK(_is_element_nullable);
}

VectorIndexWriter::~VectorIndexWriter() = default;

void VectorIndexWriter::create(const std::shared_ptr<TabletIndex>& tablet_index,
                               const std::string& vector_index_file_path, bool is_element_nullable,
                               std::unique_ptr<VectorIndexWriter>* res) {
    *res = std::make_unique<VectorIndexWriter>(tablet_index, vector_index_file_path, is_element_nullable);
}

Status VectorIndexWriter::init() {
    // Step 1: enforce minimum threshold by algorithm type.
    // IVFPQ requires training points >= nlist. If the input is below this,
    // faiss will throw, so we floor the threshold at nlist to skip the build
    // rather than crash. Other algorithms (HNSW etc.) keep the default
    // threshold from config so callers that want "build immediately" must
    // explicitly request it via index_build_threshold below.
    auto index_type_it = _tablet_index->common_properties().find("index_type");
    if (index_type_it != _tablet_index->common_properties().end()) {
        std::string index_type = index_type_it->second;
        std::transform(index_type.begin(), index_type.end(), index_type.begin(), ::tolower);
        if (index_type == "ivfpq") {
            auto nlist_it = _tablet_index->index_properties().find("nlist");
            if (nlist_it != _tablet_index->index_properties().end()) {
                auto nlist = static_cast<uint32_t>(std::atoi(nlist_it->second.c_str()));
                _start_vector_index_build_threshold = std::max(_start_vector_index_build_threshold, nlist);
            }
        }
    }

    // Step 2: user-specified property has final control over threshold.
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
            RETURN_IF_ERROR(_index_builder->close());
        } else {
            // Threshold not met: skip file generation entirely. Readers fall back
            // to brute-force scan via NotFound; vacuum won't see any vector_index_id
            // recorded in segment_meta because has_vector_index_written() stays
            // false (standalone_index_size remained 0), so no phantom .vi paths
            // are advertised downstream.
            return Status::OK();
        }
        if (index_size) {
            ASSIGN_OR_RETURN(auto file_ptr, fs::new_random_access_file(_vector_index_file_path));
            ASSIGN_OR_RETURN(auto index_file_size, file_ptr->get_size());
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
                     VectorIndexBuilderFactory::get_index_builder_type_from_config(_tablet_index));
    const int omp_threads = std::max(1, static_cast<int>(config::config_vector_index_build_concurrency));
#ifdef WITH_TENANN
    // Create VectorIndexFileWriter to support remote FS (S3/HDFS) in shared-data mode.
    // TenANN doesn't understand staros:// scheme, so we create a WritableFile through
    // StarRocks FS and bridge it to tenann via VectorIndexFileWriter.
    ASSIGN_OR_RETURN(auto wfile, fs::new_writable_file(_vector_index_file_path));
    auto file_writer = std::make_unique<VectorIndexFileWriter>(std::move(wfile));
    ASSIGN_OR_RETURN(_index_builder, VectorIndexBuilderFactory::create_index_builder(
                                             _tablet_index, _vector_index_file_path, index_builder_type,
                                             _is_element_nullable, omp_threads, file_writer.get()));
    _file_writer_holder = std::move(file_writer);
#else
    ASSIGN_OR_RETURN(_index_builder, VectorIndexBuilderFactory::create_index_builder(
                                             _tablet_index, _vector_index_file_path, index_builder_type,
                                             _is_element_nullable, omp_threads));
#endif
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
