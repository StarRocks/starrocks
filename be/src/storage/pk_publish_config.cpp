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

#include "storage/pk_publish_config.h"

#include <algorithm>
#include <charconv>
#include <string>
#include <string_view>
#include <vector>

#include "common/config_primary_key_fwd.h"
#include "common/logging.h"
#include "gen_cpp/lake_service.pb.h"

namespace starrocks {

namespace {

// One property this CN reads, and the values it accepts.
//
// The range is the one com.starrocks.catalog.PublishProperty enforces, and exists for the same
// reason: to catch a value with one zero too many, not to narrow what be.conf allows. Keeping the two
// in step matters because a range only this side knew about would reject, at every publish, a value
// the user who typed it was told was fine.
struct Spec {
    std::string_view name;
    int64_t min_value;
    int64_t max_value;
};

constexpr Spec kMemtableMaxCount{"pk_index_memtable_max_count", 1, 64};
constexpr Spec kMemtableMaxBytes{"pk_index_memtable_max_bytes", 1, 4294967296L};
constexpr Spec kRebuildFilesThreshold{"pk_index_rebuild_files_threshold", 0, 100000};
constexpr Spec kRebuildRowsThreshold{"pk_index_rebuild_rows_threshold", 0, 10000000000L};
constexpr Spec kParallelExecutionMinRows{"pk_index_parallel_execution_min_rows", 1, 100000000L};
constexpr Spec kColumnReadBatchBytes{"pk_column_read_batch_bytes", 1, 4294967296L};
constexpr Spec kRowsMapperReadParallelism{"pk_rows_mapper_read_parallelism", 1, 256};
constexpr Spec kRowsMapperReadBatchBytes{"pk_rows_mapper_read_batch_bytes", 1, 67108864L};
constexpr Spec kCompactionReplaceBatchRows{"pk_compaction_replace_batch_rows", 1, 10000000};

// What a request carried, in a fixed order. A proto map iterates in no defined order, so two lines
// printed for the same set would otherwise differ, which is exactly when a reader is comparing them.
std::string describe(const PublishPropertyPB& publish_property) {
    std::vector<std::string_view> names;
    names.reserve(publish_property.properties().size());
    for (const auto& [name, value] : publish_property.properties()) {
        names.emplace_back(name);
    }
    std::sort(names.begin(), names.end());

    std::string result;
    for (const auto& name : names) {
        if (!result.empty()) {
            result += ", ";
        }
        result += name;
        result += '=';
        result += publish_property.properties().at(std::string(name));
    }
    return result.empty() ? "nothing" : result;
}

// Reads one property the table set, if it set that one and the value is one a read site can run with.
//
// FE checks the same range before storing the value, so a value failing here came from an FE that did
// not have the check -- metadata written by an older version. That must not fail a publish: the
// property is dropped, the read site falls back to this node's config, and the warning says which
// tablet and which value, once per revision rather than once per publish.
template <typename T>
std::optional<T> read(const PublishPropertyPB& publish_property, int64_t tablet_id, const Spec& spec) {
    auto it = publish_property.properties().find(std::string(spec.name));
    if (it == publish_property.properties().end()) {
        return std::nullopt;
    }
    const std::string& text = it->second;
    int64_t value = 0;
    const char* begin = text.data();
    const char* end = begin + text.size();
    auto [stop, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc() || stop != end) {
        LOG(WARNING) << "tablet " << tablet_id << " ignores publish property " << spec.name << " at revision "
                     << publish_property.revision() << ": '" << text
                     << "' is not an integer; using this node's config instead";
        return std::nullopt;
    }
    if (value < spec.min_value || value > spec.max_value) {
        LOG(WARNING) << "tablet " << tablet_id << " ignores publish property " << spec.name << " at revision "
                     << publish_property.revision() << ": " << value << " is outside [" << spec.min_value << ", "
                     << spec.max_value << "]; using this node's config instead";
        return std::nullopt;
    }
    return static_cast<T>(value);
}

// Adds one property to `out`, if the table set it. Widening to int64_t loses nothing: every spec's
// range is one int64_t holds, which declare() checks.
template <typename T>
void append(std::vector<std::pair<std::string_view, int64_t>>& out, const Spec& spec, const std::optional<T>& value) {
    if (value.has_value()) {
        out.emplace_back(spec.name, static_cast<int64_t>(*value));
    }
}

} // namespace

std::vector<std::pair<std::string_view, int64_t>> PkPublishConfig::properties() const {
    std::vector<std::pair<std::string_view, int64_t>> result;
    append(result, kMemtableMaxCount, _memtable_max_count);
    append(result, kMemtableMaxBytes, _memtable_max_bytes);
    append(result, kRebuildFilesThreshold, _rebuild_files_threshold);
    append(result, kRebuildRowsThreshold, _rebuild_rows_threshold);
    append(result, kParallelExecutionMinRows, _parallel_execution_min_rows);
    append(result, kColumnReadBatchBytes, _column_read_batch_bytes);
    append(result, kRowsMapperReadParallelism, _rows_mapper_read_parallelism);
    append(result, kRowsMapperReadBatchBytes, _rows_mapper_read_batch_bytes);
    append(result, kCompactionReplaceBatchRows, _compaction_replace_batch_rows);
    return result;
}

PkPublishConfig::PkPublishConfig(int64_t tablet_id, const PublishPropertyPB& publish_property)
        : _revision(publish_property.revision()) {
    _memtable_max_count = read<int32_t>(publish_property, tablet_id, kMemtableMaxCount);
    _memtable_max_bytes = read<int64_t>(publish_property, tablet_id, kMemtableMaxBytes);
    _rebuild_files_threshold = read<int32_t>(publish_property, tablet_id, kRebuildFilesThreshold);
    _rebuild_rows_threshold = read<int64_t>(publish_property, tablet_id, kRebuildRowsThreshold);
    _parallel_execution_min_rows = read<int64_t>(publish_property, tablet_id, kParallelExecutionMinRows);
    _column_read_batch_bytes = read<int64_t>(publish_property, tablet_id, kColumnReadBatchBytes);
    _rows_mapper_read_parallelism = read<int32_t>(publish_property, tablet_id, kRowsMapperReadParallelism);
    _rows_mapper_read_batch_bytes = read<int64_t>(publish_property, tablet_id, kRowsMapperReadBatchBytes);
    _compaction_replace_batch_rows = read<int32_t>(publish_property, tablet_id, kCompactionReplaceBatchRows);
}

std::shared_ptr<const PkPublishConfig> PkPublishConfig::update(const std::shared_ptr<const PkPublishConfig>& current,
                                                               int64_t tablet_id,
                                                               const PublishPropertyPB& publish_property) {
    const int64_t revision = publish_property.revision();
    if (revision < current->revision()) {
        // Publishes can reach a node out of order. Keeping the newer set is the point of comparing by
        // order rather than by difference, but an older one arriving at all is worth saying out loud.
        LOG(WARNING) << "tablet " << tablet_id << " keeps publish config revision " << current->revision()
                     << " and ignores revision " << revision << ", which arrived after it and carried "
                     << describe(publish_property);
        return current;
    }
    if (revision == current->revision()) {
        // The ordinary case: a request carrying the revision this tablet already runs with, which a
        // retry and every publish between two ALTERs both look like. Nothing to re-read.
        return current;
    }
    auto updated = std::shared_ptr<const PkPublishConfig>(new PkPublishConfig(tablet_id, publish_property));
    LOG(INFO) << "tablet " << tablet_id << " publish config moved from revision " << current->revision() << " to "
              << updated->revision() << ", now carrying " << describe(publish_property);
    return updated;
}

int32_t PkPublishConfig::memtable_max_count() const {
    return _memtable_max_count.value_or(config::pk_index_memtable_max_count);
}

int64_t PkPublishConfig::memtable_max_bytes() const {
    return _memtable_max_bytes.value_or(config::l0_max_mem_usage);
}

int32_t PkPublishConfig::rebuild_files_threshold() const {
    return _rebuild_files_threshold.value_or(config::cloud_native_pk_index_rebuild_files_threshold);
}

int64_t PkPublishConfig::rebuild_rows_threshold() const {
    return _rebuild_rows_threshold.value_or(config::cloud_native_pk_index_rebuild_rows_threshold);
}

int64_t PkPublishConfig::parallel_execution_min_rows() const {
    // A table cannot set this below 1, but be.conf can still hold a value that is not positive, and
    // callers divide by what comes back. The built-in stands in for such a config, exactly as it did
    // before a table could set this at all.
    constexpr int64_t kBuiltIn = 16384;
    const int64_t value = _parallel_execution_min_rows.value_or(config::pk_index_parallel_execution_min_rows);
    return value > 0 ? value : kBuiltIn;
}

int64_t PkPublishConfig::column_read_batch_bytes() const {
    return _column_read_batch_bytes.value_or(config::pk_column_lazy_load_threshold_bytes);
}

int32_t PkPublishConfig::rows_mapper_read_parallelism() const {
    return _rows_mapper_read_parallelism.value_or(config::lake_rows_mapper_read_parallelism);
}

int64_t PkPublishConfig::rows_mapper_read_batch_bytes() const {
    return _rows_mapper_read_batch_bytes.value_or(config::lake_rows_mapper_sub_chunk_bytes);
}

int32_t PkPublishConfig::compaction_replace_batch_rows() const {
    return _compaction_replace_batch_rows.value_or(config::primary_key_compaction_replace_batch_rows);
}

} // namespace starrocks
