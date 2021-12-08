// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/schema.cpp

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

#include "storage/schema.h"

#include "util/scoped_cleanup.h"

namespace starrocks {

Schema::Schema(const Schema& other) {
    _copy_from(other);
}

Schema& Schema::operator=(const Schema& other) {
    if (this != &other) {
        _copy_from(other);
    }
    return *this;
}

void Schema::_copy_from(const Schema& other) {
    _col_ids = other._col_ids;
    _col_offsets = other._col_offsets;

    _num_key_columns = other._num_key_columns;
    _schema_size = other._schema_size;

    // Deep copy _cols
    // TODO(lingbin): really need clone?
    _cols.resize(other._cols.size(), nullptr);
    ScopedCleanup release_guard([&] {
        for (auto col : _cols) {
            delete col;
        }
    });
    for (auto cid : _col_ids) {
        _cols[cid] = other._cols[cid]->clone();
    }
    release_guard.cancel();
}

void Schema::_init(const std::vector<TabletColumn>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size(), nullptr);
    _col_offsets.resize(_cols.size(), -1);

    size_t offset = 0;
    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        _cols[cid] = FieldFactory::create(cols[cid]);

        _col_offsets[cid] = offset;
        // Plus 1 byte for null byte
        offset += _cols[cid]->size() + 1;
    }

    _schema_size = offset;
}

void Schema::_init(const std::vector<const Field*>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size(), nullptr);
    _col_offsets.resize(_cols.size(), -1);

    size_t offset = 0;
    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        // TODO(lingbin): is it necessary to clone Field? each SegmentIterator will
        // use this func, can we avoid clone?
        _cols[cid] = cols[cid]->clone();

        _col_offsets[cid] = offset;
        // Plus 1 byte for null byte
        offset += _cols[cid]->size() + 1;
    }

    _schema_size = offset;
}

Schema::~Schema() {
    for (auto col : _cols) {
        delete col;
    }
}

Status Schema::convert_to(const std::vector<FieldType>& new_types, bool* converted,
                          std::unique_ptr<Schema>* output) const {
    bool needs_convert = false;
    for (auto cid : _col_ids) {
        if (_cols[cid]->type() != new_types[cid]) {
            needs_convert = true;
            break;
        }
    }
    if (!needs_convert) {
        *converted = false;
        return Status::OK();
    }
    std::unique_ptr<Schema> new_schema(new Schema());

    new_schema->_col_ids = _col_ids;
    new_schema->_cols.resize(_cols.size(), nullptr);
    new_schema->_col_offsets.resize(_col_offsets.size(), -1);

    size_t offset = 0;
    for (int cid = 0; cid < _cols.size(); ++cid) {
        if (_cols[cid] == nullptr) {
            continue;
        }
        if (_cols[cid]->type() == new_types[cid]) {
            new_schema->_cols[cid] = _cols[cid]->clone();
        } else {
            std::unique_ptr<Field> new_field;
            RETURN_IF_ERROR(_cols[cid]->convert_to(new_types[cid], &new_field));
            new_schema->_cols[cid] = new_field.release();
        }
        new_schema->_col_offsets[cid] = offset;
        // Plus 1 byte for null byte
        offset += new_schema->_cols[cid]->size() + 1;
    }

    new_schema->_schema_size = offset;
    new_schema->_num_key_columns = _num_key_columns;

    *output = std::move(new_schema);
    *converted = true;
    return Status::OK();
}

std::string Schema::debug_string() const {
    std::stringstream ss;
    ss << "colIds=[";
    for (int i = 0; i < _col_ids.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << _col_ids[i];
    }
    ss << "],cols=[";
    for (int i = 0; i < _cols.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        if (_cols[i] == nullptr) {
            ss << "null";
        } else {
            ss << _cols[i]->debug_string();
        }
    }
    ss << "],offset=[";
    for (int i = 0; i < _col_offsets.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << _col_offsets[i];
    }
    ss << "],num_key_columns=" << _num_key_columns << ",schema_size=" << _schema_size;
    return ss.str();
}

} // namespace starrocks
