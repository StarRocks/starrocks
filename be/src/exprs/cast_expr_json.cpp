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

#include <velocypack/Exception.h>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/column_visitor_adapter.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exprs/cast_expr.h"
#include "exprs/decimal_cast_expr.h"

namespace starrocks {

template <typename, typename = void>
constexpr bool is_type_complete_v = false;

template <typename T>
constexpr bool is_type_complete_v<T, std::void_t<decltype(sizeof(T))>> = true;

// Cast item in column
// NOTE: cast in rowwise is not efficent but intuitive
class CastColumnItemVisitor final : public ColumnVisitorAdapter<CastColumnItemVisitor> {
public:
    CastColumnItemVisitor(int row, const std::string& field_name, vpack::Builder* builder)
            : ColumnVisitorAdapter(this), _row(row), _field_name(field_name), _builder(builder) {}

    static Status cast_datum_to_json(const ColumnPtr& col, int row, const std::string& name, vpack::Builder* builder) {
        CastColumnItemVisitor visitor(row, name, builder);
        try {
            return col->accept(&visitor);
        } catch (const arangodb::velocypack::Exception& e) {
            std::string data = col->debug_item(row);
            LOG(WARNING) << "cast to json failed: error_code=" << e.errorCode() << ", error_msg=" << e.what()
                         << ", data=" << data;
            return Status::DataQualityError("cast to json failed: " + data);
        }
    }

    template <class T>
    void _add_element(T&& value) {
        if (_field_name.empty()) {
            _builder->add(vpack::Value(value));
        } else {
            _builder->add(_field_name, vpack::Value(value));
        }
    }

    template <class T, typename = std::enable_if<is_type_complete_v<typename ColumnTraits<T>::ColumnType>, void>>
    Status do_visit(const FixedLengthColumn<T>& col) {
        if constexpr (CastToString::extend_type<T>()) {
            // Cast extended type to string in JSON
            auto value = col.get(_row).template get<T>();
            std::string str = CastToString::apply<T, std::string>(value);
            _add_element(std::move(str));
        } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
            auto value = col.get(_row).template get<T>();
            _add_element(std::move(value));
        } else {
            return Status::NotSupported("not supported");
        }
        return {};
    }

    template <class T>
    Status do_visit(const DecimalV3Column<T>& col) {
        int precision = col.precision();
        int scale = col.scale();
        auto value = col.get(_row).template get<T>();
        auto str = DecimalV3Cast::to_string<T>(value, precision, scale);
        _add_element(std::move(str));
        return {};
    }

    Status do_visit(const JsonColumn& col) {
        JsonValue* json = col.get_object(_row);
        if (_field_name.empty()) {
            _builder->add(json->to_vslice());
        } else {
            _builder->add(_field_name, json->to_vslice());
        }
        return {};
    }

    Status do_visit(const BinaryColumn& col) {
        Slice slice = col.get_slice(_row);
        _add_element(std::string_view(slice.data, slice.size));
        return {};
    }

    Status do_visit(const StructColumn& col) {
        if (_field_name.empty()) {
            _builder->openObject();
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Object));
        }
        auto& names = col.field_names();
        auto& columns = col.fields();
        for (int i = 0; i < columns.size(); i++) {
            auto name = names.size() > i ? names[i] : fmt::format("k{}", i);
            auto& field_column = columns[i];
            RETURN_IF_ERROR(cast_datum_to_json(field_column, _row, name, _builder));
        }
        if (!_builder->isClosed()) {
            _builder->close();
        }

        return {};
    }

    Status do_visit(const MapColumn& col) {
        if (_field_name.empty()) {
            _builder->openObject();
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Object));
        }
        auto [map_start, map_size] = col.get_map_offset_size(_row);
        auto key_col = col.keys_column();
        auto val_col = col.values_column();

        if (key_col->has_null()) {
            return Status::NotSupported("key of Map should not be null");
        }
        if (key_col->is_nullable()) {
            key_col = ColumnHelper::as_column<NullableColumn>(key_col)->data_column();
        }

        for (int i = map_start; i < map_start + map_size; i++) {
            std::string name;
            if (key_col->is_binary()) {
                auto binary_col = ColumnHelper::as_column<BinaryColumn>(key_col);
                name = binary_col->get_slice(i);
            } else if (key_col->is_large_binary()) {
                auto binary_col = ColumnHelper::as_column<LargeBinaryColumn>(key_col);
                name = binary_col->get_slice(i);
            } else {
                // TODO(murphy) cast to string instead of debug
                name = key_col->debug_item(i);
            }

            // JSON doesn't support empty key, so just skip it
            if (name.empty()) {
                continue;
            }
            // VLOG(2) << "map key " << i << ": " << key_col->debug_item(i) << " , name=" << name;
            RETURN_IF_ERROR(cast_datum_to_json(val_col, i, name, _builder));
        }

        if (!_builder->isClosed()) {
            _builder->close();
        }
        return {};
    }

    Status do_visit(const ArrayColumn& col) {
        if (_field_name.empty()) {
            _builder->openArray();
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Array));
        }

        auto [offset, size] = col.get_element_offset_size(_row);
        auto elements = col.elements_column();
        for (int i = offset; i < offset + size; i++) {
            RETURN_IF_ERROR(cast_datum_to_json(elements, i, "", _builder));
        }

        if (!_builder->isClosed()) {
            _builder->close();
        }

        return {};
    }

    Status do_visit(const NullableColumn& col) {
        if (col.is_null(_row)) {
            _add_element(vpack::ValueType::Null);
        } else {
            RETURN_IF_ERROR(cast_datum_to_json(col.data_column(), _row, _field_name, _builder));
        }
        return {};
    }

    // for type like hll and bitmap, right now only output NULL
    template <class T>
    Status do_visit(const ObjectColumn<T>& col) {
        _add_element(vpack::ValueType::Null);
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
// TODO(murphy): optimize the performance with columnwise-casting
StatusOr<ColumnPtr> cast_nested_to_json(const ColumnPtr& column, bool allow_throw_exception) {
    ColumnBuilder<TYPE_JSON> column_builder(column->size());
    vpack::Builder json_builder;
    if (allow_throw_exception) {
        for (int row = 0; row < column->size(); row++) {
            if (column->is_null(row)) {
                column_builder.append_null();
                continue;
            }
            json_builder.clear();
            RETURN_IF_ERROR(CastColumnItemVisitor::cast_datum_to_json(column, row, "", &json_builder));
            JsonValue json(json_builder.slice());
            column_builder.append(std::move(json));
        }
    } else {
        for (int row = 0; row < column->size(); row++) {
            if (column->is_null(row)) {
                column_builder.append_null();
                continue;
            }
            json_builder.clear();
            auto st = CastColumnItemVisitor::cast_datum_to_json(column, row, "", &json_builder);
            if (!st.ok()) {
                column_builder.append_null();
                continue;
            }

            JsonValue json(json_builder.slice());
            column_builder.append(std::move(json));
        }
    }

    return column_builder.build(false);
}

StatusOr<std::string> cast_type_to_json_str(const ColumnPtr& column, int idx) {
    vpack::Builder json_builder;
    json_builder.clear();
    RETURN_IF_ERROR(CastColumnItemVisitor::cast_datum_to_json(column, idx, "", &json_builder));
    JsonValue json(json_builder.slice());

    return json.to_string();
}

} // namespace starrocks
