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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exprs/cast_expr.h"
#include "types/logical_type.h"

namespace starrocks {

// Cast item in column
// NOTE: cast in rowwise is not efficent but intuitive
class CastColumnItemVisitor final : public ColumnVisitorAdapter<CastColumnItemVisitor> {
public:
    CastColumnItemVisitor(int row, const std::string& field_name, vpack::Builder* builder)
            : ColumnVisitorAdapter(this), _row(row), _field_name(field_name), _builder(builder) {}

    static Status cast_datum_to_json(const ColumnPtr& col, int row, const std::string& name, vpack::Builder* builder) {
        CastColumnItemVisitor visitor(row, name, builder);
        return col->accept(&visitor);
    }

    template <class T, typename = std::enable_if<ColumnTraits<T>::ColumnType, void>>
    Status do_visit(const FixedLengthColumn<T>& col) {
        auto value = col.get(_row).template get<T>();
        _builder->add(_field_name, value);
        return {};
    }

    Status do_visit(const JsonColumn& col) {
        JsonValue* json = col.get_object(_row);
        _builder->add(_field_name, json->to_vslice());
        return {};
    }

    Status do_visit(const BinaryColumn& col) {
        Slice slice = col.get_slice(_row);
        _builder->add(_field_name, vpack::Value(std::string_view(slice.data, slice.size)));
        return {};
    }

    Status do_visit(const StructColumn& col) {
        auto& names = col.field_names();
        auto& columns = col.fields();
        for (int i = 0; i < columns.size(); i++) {
            auto& name = names[i];
            auto& field_column = columns[i];
            RETURN_IF_ERROR(cast_datum_to_json(field_column, _row, name, _builder));
        }
        return {};
    }

    Status do_visit(const MapColumn& col) {
        size_t map_size = col.get_map_size(_row);
        size_t map_start = col.get_map_size(_row - 1);
        auto key_col = col.keys_column();
        auto val_col = col.values_column();

        for (int i = 0; i < map_size; i++) {
            int index = map_start + i;
            // TODO(murphy) cast to string instead of debug
            std::string name = key_col->debug_item(index);
            RETURN_IF_ERROR(cast_datum_to_json(val_col, index, name, _builder));
        }
        return {};
    }

    template <class ColumnType>
    Status do_visit(const ColumnType& _) {
        return Status::NotSupported("not supported");
    }

private:
    int _row;
    const std::string& _field_name;
    vpack::Builder* _builder;
};

// Cast nested type(including struct/map/* to json)
StatusOr<ColumnPtr> cast_nested_to_json(const ColumnPtr& column) {
    ColumnBuilder<TYPE_JSON> column_builder(column->size());
    vpack::Builder json_builder;
    for (int row = 0; row < column->size(); row++) {
        json_builder.clear();
        RETURN_IF_ERROR(CastColumnItemVisitor::cast_datum_to_json(column, row, "", &json_builder));

        JsonValue json(json_builder.slice());
        column_builder.append(std::move(json));
    }
    return column_builder.build(false);
}

} // namespace starrocks
