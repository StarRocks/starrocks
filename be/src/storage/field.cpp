// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/field.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "storage/field.h"

namespace starrocks {
Field::Field(const TabletColumn& column)
        : _name(column.name()),
            _type_info(get_type_info(column)),
            _key_coder(get_key_coder(column.type())),
            _index_size(column.index_length()),
            _length(column.length()),
            _is_nullable(column.is_nullable()) {
    DCHECK(column.type() != OLAP_FIELD_TYPE_DECIMAL32 && column.type() != OLAP_FIELD_TYPE_DECIMAL64 &&
            column.type() != OLAP_FIELD_TYPE_DECIMAL128);
}

Field::Field(const TabletColumn& column, std::shared_ptr<TypeInfo>&& type_info)
            : _name(column.name()),
              _type_info(type_info),
              _key_coder(get_key_coder(column.type())),
              _index_size(column.index_length()),
              _length(column.length()),
              _is_nullable(column.is_nullable()) {}

Field* FieldFactory::create_by_type(const FieldType& type) {
    TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
    return create(column);
}

Field* FieldFactory::create(const TabletColumn& column) {
    // for key column
    if (column.is_key()) {
        switch (column.type()) {
        case OLAP_FIELD_TYPE_CHAR:
            return new CharField(column);
        case OLAP_FIELD_TYPE_VARCHAR:
            return new VarcharField(column);
        case OLAP_FIELD_TYPE_ARRAY: {
            std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
            auto* local = new Field(column);
            local->add_sub_field(std::move(item_field));
            return local;
        }
        case OLAP_FIELD_TYPE_DECIMAL32:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL32>>(
                                                column.precision(), column.scale()));
        case OLAP_FIELD_TYPE_DECIMAL64:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL64>>(
                                                column.precision(), column.scale()));
        case OLAP_FIELD_TYPE_DECIMAL128:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL128>>(
                                                column.precision(), column.scale()));
        default:
            return new Field(column);
        }
    }

    // for value column
    switch (column.aggregation()) {
    case OLAP_FIELD_AGGREGATION_NONE:
    case OLAP_FIELD_AGGREGATION_SUM:
    case OLAP_FIELD_AGGREGATION_MIN:
    case OLAP_FIELD_AGGREGATION_MAX:
    case OLAP_FIELD_AGGREGATION_REPLACE:
    case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
        switch (column.type()) {
        case OLAP_FIELD_TYPE_CHAR:
            return new CharField(column);
        case OLAP_FIELD_TYPE_VARCHAR:
            return new VarcharField(column);
        case OLAP_FIELD_TYPE_ARRAY: {
            std::unique_ptr<Field> item_field(FieldFactory::create(column.subcolumn(0)));
            std::unique_ptr<Field> local = std::make_unique<Field>(column);
            local->add_sub_field(std::move(item_field));
            return local.release();
        }
        case OLAP_FIELD_TYPE_DECIMAL32:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL32>>(
                                                column.precision(), column.scale()));
        case OLAP_FIELD_TYPE_DECIMAL64:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL64>>(
                                                column.precision(), column.scale()));
        case OLAP_FIELD_TYPE_DECIMAL128:
            return new Field(column, std::make_shared<DecimalTypeInfo<OLAP_FIELD_TYPE_DECIMAL128>>(
                                                column.precision(), column.scale()));
        default:
            return new Field(column);
        }
    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        return new HllAggField(column);
    case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return new BitmapAggField(column);
    case OLAP_FIELD_AGGREGATION_PERCENTILE_UNION:
        return new PercentileAggField(column);
    case OLAP_FIELD_AGGREGATION_UNKNOWN:
        LOG(WARNING) << "WOW! value column agg type is unknown";
        return nullptr;
    }
    LOG(WARNING) << "WOW! value column no agg type";
    return nullptr;
}
}

