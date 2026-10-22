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

#include "storage/query/range_group_task.h"

#include <algorithm>
#include <charconv>

#include "storage/lake/tablet_range_helper.h"

namespace starrocks {
namespace {

constexpr size_t kMaxTaskValueBytes = 16UL * 1024 * 1024;
constexpr size_t kMaxTaskTotalBytes = 64UL * 1024 * 1024;

Status validate_value_size(size_t bytes, size_t* total_bytes) {
    if (bytes > kMaxTaskValueBytes) {
        return Status::Corruption("grouped range task value bytes exceed cap 16777216");
    }
    *total_bytes += bytes;
    if (*total_bytes > kMaxTaskTotalBytes) {
        return Status::Corruption("grouped range task total value bytes exceed cap 67108864");
    }
    return Status::OK();
}

Status validate_identity(const RangeGroupTask& task) {
    if (task.group_id < 0) {
        return Status::InvalidArgument("grouped range task requires a nonnegative group ID");
    }
    const auto& ids = task.sort_key_unique_ids;
    if (ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2]) {
        return Status::InvalidArgument("grouped range task requires distinct sort-key unique IDs");
    }
    return Status::OK();
}

Status validate_clip(const TabletRangePB& range) {
    size_t total_bytes = 0;
    auto check_bound = [&total_bytes](const TuplePB& bound) -> Status {
        if (bound.values_size() != 3) {
            return Status::InvalidArgument("grouped range task requires three values per finite bound");
        }
        for (const auto& value : bound.values()) {
            if (!value.has_variant_type() || value.variant_type() != VariantTypePB::NORMAL_VALUE ||
                !value.has_value() || !value.has_type() || value.type().types_size() != 1 ||
                value.type().types(0).type() != TTypeNodeType::SCALAR || !value.type().types(0).has_scalar_type() ||
                value.type().types(0).scalar_type().type() != TPrimitiveType::BIGINT) {
                return Status::InvalidArgument("grouped range task requires NORMAL BIGINT finite values");
            }
            RETURN_IF_ERROR(validate_value_size(value.value().size(), &total_bytes));
            // The legacy BIGINT storage parser uses strtol permissively. Query task
            // bounds must instead reject overflow and partially parsed literals.
            int64_t parsed;
            const auto& bytes = value.value();
            auto [end, error] = std::from_chars(bytes.data(), bytes.data() + bytes.size(), parsed);
            if (error != std::errc{} || end != bytes.data() + bytes.size()) {
                return Status::InvalidArgument("grouped range task has an invalid BIGINT finite value");
            }
        }
        return Status::OK();
    };
    if (range.has_lower_bound()) RETURN_IF_ERROR(check_bound(range.lower_bound()));
    if (range.has_upper_bound()) RETURN_IF_ERROR(check_bound(range.upper_bound()));
    RETURN_IF_ERROR(lake::TabletRangeHelper::validate_tablet_range(range));
    if (range.has_lower_bound() && range.has_upper_bound()) {
        std::array<int64_t, 3> lower{}, upper{};
        for (int i = 0; i < 3; ++i) {
            const auto& lo = range.lower_bound().values(i).value();
            const auto& hi = range.upper_bound().values(i).value();
            std::from_chars(lo.data(), lo.data() + lo.size(), lower[i]);
            std::from_chars(hi.data(), hi.data() + hi.size(), upper[i]);
        }
        if (!(lower < upper)) {
            return Status::InvalidArgument("grouped range task requires a nonempty ordered clip");
        }
    }
    return Status::OK();
}

} // namespace

StatusOr<RangeGroupTaskPtr> decode_range_group_task(const TGroupedRangeTask& wire) {
    if (!wire.__isset.group_id || !wire.__isset.key_range || !wire.__isset.sort_key_unique_ids) {
        return Status::InvalidArgument("grouped range task is missing group ID, key range or sort-key unique IDs");
    }
    if (wire.sort_key_unique_ids.size() != 3) {
        return Status::InvalidArgument("grouped range task requires three sort-key unique IDs");
    }
    auto task = std::make_shared<RangeGroupTask>();
    task->group_id = wire.group_id;
    std::copy(wire.sort_key_unique_ids.begin(), wire.sort_key_unique_ids.end(), task->sort_key_unique_ids.begin());
    RETURN_IF_ERROR(validate_identity(*task));
    // Guard the exact supported thrift type shape before the general decoder, which
    // assumes a well-formed type tree. Optional presence is never inferred from defaults.
    size_t total_bytes = 0;
    auto check_bound = [&total_bytes](const TTuple& bound) -> Status {
        if (bound.values.size() != 3) {
            return Status::InvalidArgument("grouped range task requires three values per finite bound");
        }
        for (const auto& v : bound.values) {
            if (!v.__isset.variant_type || v.variant_type != TVariantType::NORMAL_VALUE || !v.__isset.value ||
                !v.__isset.type || !v.type.__isset.types || v.type.types.size() != 1) {
                return Status::InvalidArgument("grouped range task requires typed NORMAL BIGINT finite values");
            }
            const auto& node = v.type.types[0];
            if (node.type != TTypeNodeType::SCALAR || !node.__isset.scalar_type ||
                node.scalar_type.type != TPrimitiveType::BIGINT) {
                return Status::InvalidArgument("grouped range task supports only BIGINT finite values");
            }
            RETURN_IF_ERROR(validate_value_size(v.value.size(), &total_bytes));
        }
        return Status::OK();
    };
    if (wire.key_range.__isset.lower_bound) RETURN_IF_ERROR(check_bound(wire.key_range.lower_bound));
    if (wire.key_range.__isset.upper_bound) RETURN_IF_ERROR(check_bound(wire.key_range.upper_bound));
    ASSIGN_OR_RETURN(task->key_range, lake::TabletRangeHelper::convert_t_range_to_pb_range(wire.key_range));
    RETURN_IF_ERROR(validate_clip(task->key_range));
    return RangeGroupTaskPtr(std::move(task));
}

Status validate_range_group_schema(const RangeGroupTask& task, const TabletSchema& schema) {
    RETURN_IF_ERROR(validate_identity(task));
    const auto& sort_keys = schema.sort_key_idxes();
    if (schema.keys_type() != DUP_KEYS || sort_keys.size() != 3) {
        return Status::InvalidArgument("grouped range task requires DUP_KEYS with exactly three sort columns");
    }
    for (int i = 0; i < 3; ++i) {
        if (sort_keys[i] >= schema.num_columns()) {
            return Status::InvalidArgument("grouped range task has an invalid schema sort-column index");
        }
        const auto& col = schema.column(sort_keys[i]);
        if (col.type() != TYPE_BIGINT || col.is_nullable() || col.unique_id() != task.sort_key_unique_ids[i]) {
            return Status::InvalidArgument(
                    "grouped range task requires matching ordered BIGINT NOT NULL sort-column IDs");
        }
    }
    return validate_clip(task.key_range);
}

StatusOr<SeekRange> decode_range_group_seek(const RangeGroupTask& task, const TabletSchemaCSPtr& schema) {
    if (schema == nullptr) {
        return Status::InvalidArgument("grouped range task requires a captured or archived schema");
    }
    RETURN_IF_ERROR(validate_range_group_schema(task, *schema));
    // All finite values are BIGINT Datums stored by value. No string/Slice lifetime
    // escapes this restricted helper, so a null MemPool is safe.
    return lake::TabletRangeHelper::create_seek_range_from(task.key_range, schema, nullptr);
}

} // namespace starrocks
