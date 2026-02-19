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

#include "storage/virtual_column_utils.h"

#include <memory>
#include <utility>

#include "column/column.h"
#include "gen_cpp/PlanNodes_constants.h"
#include "runtime/descriptors.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowid_column_iterator.h"
#include "storage/types.h"
#include "types/logical_type.h"

namespace starrocks {
struct VirtualColumnDefinition;
using IteratorCreator = ColumnIterator* (*)(const VirtualColumnFactory::Options& options,
                                            const VirtualColumnDefinition& def);
using ColumnAppender = Status (*)(const VirtualColumnFactory::Options& options, const VirtualColumnDefinition& def,
                                  Column* column);

struct VirtualColumnDefinition {
    VirtualColumnDefinition(std::string name_, LogicalType type_, bool nullable_, IteratorCreator iterator_creator_,
                            ColumnAppender appender_)
            : name(std::move(name_)),
              type(type_),
              nullable(nullable_),
              iterator_creator(iterator_creator_),
              appender(appender_) {}
    const std::string name;
    const LogicalType type;
    bool nullable;
    IteratorCreator iterator_creator;
    ColumnAppender appender;
};

struct TabletExtractor {
    auto extract(const VirtualColumnFactory::Options& options) { return options.tablet_id; }
};

struct SegmentExtractor {
    auto extract(const VirtualColumnFactory::Options& options) { return options.segment_id; }
};

struct RowIdExtractor {
    auto extract(const VirtualColumnFactory::Options& options) { return options.num_rows; }
};

template <class Extractor>
ColumnIterator* create_iterator(const VirtualColumnFactory::Options& options, const VirtualColumnDefinition& def) {
    auto value = Extractor().extract(options);
    size_t schema_length = sizeof(value);
    TypeInfoPtr type_info = get_type_info(def.type);
    return new DefaultValueColumnIterator(true, std::to_string(value), def.nullable, type_info, schema_length,
                                          options.num_rows);
}

ColumnIterator* create_virtual_row_id_iterator(const VirtualColumnFactory::Options& options,
                                               const VirtualColumnDefinition& def) {
    return new TRowIdColumnIterator<int64_t>();
}

template <class Extractor>
Status append_datum(const VirtualColumnFactory::Options& options, const VirtualColumnDefinition& def, Column* column) {
    column->append_datum(Datum(Extractor().extract(options)));
    return Status::OK();
}

struct VirtualColumnDefinition VIRTUAL_COLUMNS[] = {
        VirtualColumnDefinition(PlanNodesConstants().TABLET_ID_COLUMN_NAME, TYPE_BIGINT, false,
                                create_iterator<TabletExtractor>, append_datum<TabletExtractor>),
        VirtualColumnDefinition(PlanNodesConstants().SEGMENT_ID_COLUMN_NAME, TYPE_BIGINT, false,
                                create_iterator<SegmentExtractor>, append_datum<SegmentExtractor>),
        VirtualColumnDefinition(PlanNodesConstants().ROW_ID_COLUMN_NAME, TYPE_BIGINT, false,
                                create_virtual_row_id_iterator, append_datum<RowIdExtractor>),
};

class SlotDescriptor;
bool is_virtual_column(const std::string_view col_name) {
    for (const auto& vc : VIRTUAL_COLUMNS) {
        if (vc.name == col_name) {
            return true;
        }
    }
    return false;
}

StatusOr<TabletSchemaCSPtr> extend_schema_by_virtual_columns(const TabletSchemaCSPtr& schema,
                                                             const std::vector<SlotDescriptor*>& slots) {
    bool has_virtual_column = false;
    has_virtual_column =
            std::any_of(slots.begin(), slots.end(), [](const SlotDescriptor* slot) { return slot->is_virtual(); });
    if (!has_virtual_column) {
        return schema;
    }
    TabletSchemaSPtr tmp_schema = TabletSchema::copy(*schema);
    for (const auto& slot : slots) {
        const std::string& col_name = slot->col_name();
        if (slot->is_virtual()) {
            bool found = false;
            for (const auto& vc : VIRTUAL_COLUMNS) {
                if (vc.name == col_name) {
                    TabletColumn tc;
                    tc.set_name(vc.name);
                    tc.set_type(vc.type);
                    tc.set_is_nullable(vc.nullable);
                    tc.set_is_virtual_column(true);

                    auto keys_type = schema->keys_type();
                    if (keys_type == KeysType::UNIQUE_KEYS || keys_type == KeysType::AGG_KEYS) {
                        tc.set_aggregation(StorageAggregateType::STORAGE_AGGREGATE_REPLACE);
                    }

                    tmp_schema->append_column(tc);
                    found = true;
                    break;
                }
            }
            if (!found) {
                return Status::InternalError(fmt::format("Unknown virtual column: {}", col_name));
            }
        }
    }

    return tmp_schema;
}

StatusOr<TabletSchemaCSPtr> extend_schema_by_virtual_columns(const TabletSchemaCSPtr& schema) {
    TabletSchemaSPtr tmp_schema = TabletSchema::copy(*schema);
    // copy extended info from original schema
    for (size_t i = 0; i < schema->num_columns(); ++i) {
        const TabletColumn& col = schema->column(i);
        const TabletColumn& dst_col = tmp_schema->column(i);
        if (col.extended_info() != nullptr) {
            const_cast<TabletColumn&>(dst_col).set_extended_info(
                    std::make_unique<ExtendedColumnInfo>(*col.extended_info()));
        }
    }
    for (const auto& vc : VIRTUAL_COLUMNS) {
        TabletColumn tc;
        tc.set_name(vc.name);
        tc.set_type(vc.type);
        tc.set_is_nullable(vc.nullable);
        tc.set_is_virtual_column(true);

        auto keys_type = schema->keys_type();
        if (keys_type == KeysType::UNIQUE_KEYS || keys_type == KeysType::AGG_KEYS) {
            tc.set_aggregation(StorageAggregateType::STORAGE_AGGREGATE_REPLACE);
        }

        tmp_schema->append_column(tc);
    }
    return tmp_schema;
}

StatusOr<ColumnIterator*> VirtualColumnFactory::create_virtual_column_iterator(const Options& options,
                                                                               const std::string_view col_name) {
    for (const auto& vc : VIRTUAL_COLUMNS) {
        if (vc.name == col_name) {
            return vc.iterator_creator(options, vc);
        }
    }
    return Status::InternalError(fmt::format("unknown virtual column: {}", col_name));
}

Status VirtualColumnFactory::append_to_column(const Options& options, const std::string_view col_name, Column* column) {
    for (const auto& vc : VIRTUAL_COLUMNS) {
        if (vc.name == col_name) {
            return vc.appender(options, vc, column);
        }
    }
    return Status::InternalError(fmt::format("unknown virtual column: {}", col_name));
}

} // namespace starrocks