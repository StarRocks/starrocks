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

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <limits>

#include "column/nullable_column.h"
#include "column/raw_data_visitor.h"
#include "common/config_vector_index_fwd.h"
#include "common/runtime_profile.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
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
    // Threshold resolution is the single source of truth shared with the async build
    // path (lake::get_vector_index_build_threshold); see resolve_vector_index_build_threshold
    // for the precedence rules (config default -> user override -> IVFPQ nlist floor).
    _start_vector_index_build_threshold = resolve_vector_index_build_threshold(*_tablet_index);
    return Status::OK();
}

Status VectorIndexWriter::append(const Column& src) {
    DCHECK(src.is_array());

    // Input validation is performed by ArrayColumnWriter::append before this
    // method is reached, so the writer trusts the data and skips re-checking.

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

// Mirrors the original valid_input_vector<is_input_normalized> in
// tenann_index_builder.cpp byte-for-byte; the only structural change is that
// the compile-time template parameter becomes a runtime argument. Behavior
// parity is intentional — this lifts the same validation up so it runs from
// every caller (writer below threshold, async build task) instead of only
// when TenAnnIndexBuilderProxy::add is reached.
Status validate_vector_index_input(const ArrayColumn& array_col, size_t dim, bool is_input_normalized) {
    if (array_col.empty()) {
        return Status::OK();
    }

    const size_t num_rows = array_col.size();
    const auto& offsets = array_col.offsets().immutable_data();
    RawDataVisitor rv;
    RETURN_IF_ERROR(array_col.elements().accept(&rv));
    const auto* nums = reinterpret_cast<const float*>(rv.result());

    // Element column is nullable for vector-indexed array columns; the null
    // mask is only consumed when normalization is checked, but we extract it
    // unconditionally to mirror the original control flow.
    const auto& nullable_elements = down_cast<const NullableColumn&>(array_col.elements());
    const uint8_t* is_element_nulls = nullable_elements.null_column_ref().raw_data();

    for (size_t i = 0; i < num_rows; i++) {
        const size_t input_dim = offsets[i + 1] - offsets[i];

        if (input_dim != dim) {
            return Status::InvalidArgument(
                    strings::Substitute("The dimensions of the vector written are inconsistent, index dim is "
                                        "$0 but data dim is $1",
                                        dim, input_dim));
        }

        if (is_input_normalized) {
            double sum = 0;
            for (size_t j = 0; j < input_dim; j++) {
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

namespace {
// Checked parse of a property value into uint32_t; returns false when the key is absent
// or the value is malformed / out of range, so callers keep their default rather than
// silently using atoi()'s 0 or a wrapped-negative value.
bool parse_u32_property(const std::map<std::string, std::string>& props, const std::string& key, uint32_t* out) {
    auto it = props.find(key);
    if (it == props.end()) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(it->second.c_str(), &end, 10);
    if (errno != 0 || end == it->second.c_str() || *end != '\0' || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *out = static_cast<uint32_t>(parsed);
    return true;
}
} // namespace

uint32_t resolve_vector_index_build_threshold(const TabletIndex& index) {
    const auto& common = index.common_properties();

    // 1. config default.
    uint32_t threshold = static_cast<uint32_t>(config::config_vector_index_default_build_threshold);

    // 2. user-specified property overrides the default.
    uint32_t user_threshold = 0;
    if (parse_u32_property(common, "index_build_threshold", &user_threshold)) {
        threshold = user_threshold;
    }

    // 3. IVFPQ needs at least `nlist` training points or faiss throws, so floor the threshold
    //    at nlist. Applied LAST so a user override can never drop the threshold below nlist.
    auto type_it = common.find("index_type");
    if (type_it != common.end()) {
        std::string index_type = type_it->second;
        std::transform(index_type.begin(), index_type.end(), index_type.begin(), ::tolower);
        if (index_type == "ivfpq") {
            uint32_t nlist = 0;
            if (parse_u32_property(index.index_properties(), "nlist", &nlist)) {
                threshold = std::max(threshold, nlist);
            }
        }
    }
    return threshold;
}

} // namespace starrocks
