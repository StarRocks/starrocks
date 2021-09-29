// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_cond.cpp

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

#include "storage/olap_cond.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <cstring>
#include <string>
#include <utility>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/utils.h"
#include "storage/wrapper_field.h"

using std::nothrow;
using std::pair;
using std::string;
using std::vector;

// This file is mainly used to process the query conditions and delete conditions sent by users
// Logically both can be divided into three levels
// Condition->Condcolumn->Cond
// Condition represents a single condition issued by the user
// Condcolumn represents the set of all conditions on a column.
// Conds Represents a single condition on a column.
// For query conditions, the relationship between conditions at all levels is logical AND
// For delete condition is different. Cond and Condcolumn is logical AND, Condition is logical OR.

// implementation-specific.
// eval is used to filter query conditions. Including filter row, block, version,
// which layer is used depends on where the call is made.
//   1. There are no separate filter rows for filtering conditions. This is done in the query layer,
//   2. Filter block in SegmentReader.
//   3. Filter version in Reader. call delta_pruing_filter
//
// del_eval used to filter delete conditions, Including filter block and version,
// But this filter has one more state than eval, which is partial filtering
//   1. Filtering of rows in DeleteHandler.
//      this part is called directly delete_condition_eval,
//      The eval function is called internally,
//      Because filtering a Row does not involve the state of partial filtering.
//   2. Filter block in SegmentReader, call del_eval
//   3. Filter version in Reader, call rowset_pruning_filter

namespace starrocks {

static const uint16_t MAX_OP_STR_LENGTH = 3;

static CondOp parse_op_type(const string& op) {
    size_t op_size = op.size();
    if (op_size > MAX_OP_STR_LENGTH) {
        return OP_NULL;
    }

    switch (op[0]) {
    case '=':
        return OP_EQ;
    case '!':
        // for !*=
        if (op_size == 3 && op[1] == '*') {
            return OP_NOT_IN;
        }
        return OP_NE;
    case '*':
        return OP_IN;
    case '>':
        return (op[1] == '=') ? OP_GE : OP_GT;
    case '<':
        return (op[1] == '=') ? OP_LE : OP_LT;
    case 'i':
    case 'I':
        return (op[1] == 's' || op[1] == 'S') ? OP_IS : OP_NULL;
    default:
        return OP_NULL;
    }
}

Cond::Cond() {}

Cond::~Cond() {
    delete operand_field;
    for (const auto& it : operand_set) {
        delete it;
    }
    min_value_filed = nullptr;
    max_value_filed = nullptr;
}

OLAPStatus Cond::init(const TCondition& tcond, const TabletColumn& column) {
    // Parse op type
    op = parse_op_type(tcond.condition_op);
    if (op == OP_NULL || (op != OP_IN && op != OP_NOT_IN && tcond.condition_values.size() != 1)) {
        LOG(WARNING) << "Condition op type is invalid. name=" << tcond.column_name << " op=" << op
                     << " size=" << tcond.condition_values.size();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    if (op == OP_IS) {
        // 'is null' or 'is not null'
        auto operand = tcond.condition_values.begin();
        std::unique_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
        if (f == nullptr) {
            LOG(WARNING) << "Create field failed. name=" << tcond.column_name << " operand=" << *operand
                         << " op_type=" << op;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        if (strcasecmp(operand->c_str(), "NULL") == 0) {
            f->set_null();
        } else {
            f->set_not_null();
        }
        operand_field = f.release();
    } else if (op != OP_IN && op != OP_NOT_IN) {
        auto operand = tcond.condition_values.begin();
        std::unique_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
        if (f == nullptr) {
            LOG(WARNING) << "Create field failed. name=" << tcond.column_name << " operand=" << *operand
                         << " op_type=" << op;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        OLAPStatus res = f->from_string(*operand);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "Create field failed. name=tcond.column_name"
                         << " operand=" << *operand << " op_type=" << op;
            return res;
        }
        operand_field = f.release();
    } else {
        for (const auto& operand : tcond.condition_values) {
            std::unique_ptr<WrapperField> f(WrapperField::create(column, operand.length()));
            if (f == nullptr) {
                LOG(WARNING) << "Create field failed. name=" << tcond.column_name << ", operand=" << operand
                             << " op_type=" << op;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            OLAPStatus res = f->from_string(operand);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "Create field failed. name=" << tcond.column_name << " operand=" << operand
                             << " op_type=" << op;
                return res;
            }
            if (min_value_filed == nullptr || f->cmp(min_value_filed) < 0) {
                min_value_filed = f.get();
            }
            if (max_value_filed == nullptr || f->cmp(max_value_filed) > 0) {
                max_value_filed = f.get();
            }
            auto insert_result = operand_set.insert(f.get());
            if (!insert_result.second) {
                LOG(WARNING) << "Duplicate operand in in-predicate.[condition=" << operand << "]";
                // Duplicated, let std::unique_ptr delete field
            } else {
                // Normal case, release this std::unique_ptr
                f.release();
            }
        }
    }

    return OLAP_SUCCESS;
}

bool Cond::eval(const RowCursorCell& cell) const {
    if (cell.is_null() && op != OP_IS) {
        // any operand and NULL operation is false
        return false;
    }

    switch (op) {
    case OP_EQ:
        return operand_field->field()->compare_cell(*operand_field, cell) == 0;
    case OP_NE:
        return operand_field->field()->compare_cell(*operand_field, cell) != 0;
    case OP_LT:
        return operand_field->field()->compare_cell(*operand_field, cell) > 0;
    case OP_LE:
        return operand_field->field()->compare_cell(*operand_field, cell) >= 0;
    case OP_GT:
        return operand_field->field()->compare_cell(*operand_field, cell) < 0;
    case OP_GE:
        return operand_field->field()->compare_cell(*operand_field, cell) <= 0;
    case OP_IN: {
        WrapperField wrapperField(const_cast<Field*>(min_value_filed->field()), cell);
        auto ret = operand_set.find(&wrapperField) != operand_set.end();
        wrapperField.release_field();
        return ret;
    }
    case OP_NOT_IN: {
        WrapperField wrapperField(const_cast<Field*>(min_value_filed->field()), cell);
        auto ret = operand_set.find(&wrapperField) == operand_set.end();
        wrapperField.release_field();
        return ret;
    }
    case OP_IS: {
        return operand_field->is_null() == cell.is_null();
    }
    default:
        // Unknown operation type, just return false
        return false;
    }
}

bool Cond::eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
    // Version is filtered by a single query condition on a single column
    // when we apply column statistic, Field can be NULL when type is varchar,
    // we just ignore this cond
    if (statistic.first == nullptr || statistic.second == nullptr) {
        return true;
    }
    if (OP_IS != op && statistic.first->is_null()) {
        return true;
    }
    switch (op) {
    case OP_EQ: {
        return operand_field->cmp(statistic.first) >= 0 && operand_field->cmp(statistic.second) <= 0;
    }
    case OP_NE: {
        return operand_field->cmp(statistic.first) < 0 || operand_field->cmp(statistic.second) > 0;
    }
    case OP_LT: {
        return operand_field->cmp(statistic.first) > 0;
    }
    case OP_LE: {
        return operand_field->cmp(statistic.first) >= 0;
    }
    case OP_GT: {
        return operand_field->cmp(statistic.second) < 0;
    }
    case OP_GE: {
        return operand_field->cmp(statistic.second) <= 0;
    }
    case OP_IN: {
        return min_value_filed->cmp(statistic.second) <= 0 && max_value_filed->cmp(statistic.first) >= 0;
    }
    case OP_NOT_IN: {
        return min_value_filed->cmp(statistic.second) > 0 || max_value_filed->cmp(statistic.first) < 0;
    }
    case OP_IS: {
        return operand_field->is_null() ? statistic.first->is_null() : !statistic.second->is_null();
    }
    default:
        break;
    }

    return false;
}

int Cond::del_eval(const std::pair<WrapperField*, WrapperField*>& stat) const {
    // Version is filtered by a single delete condition on a single column

    // When we apply column statistics, stat maybe null.
    if (stat.first == nullptr || stat.second == nullptr) {
        //for string type, the column statistics may be not recorded in block level
        //so it can be ignored for ColumnStatistics.
        return DEL_PARTIAL_SATISFIED;
    }

    if (OP_IS != op) {
        if (stat.first->is_null() && stat.second->is_null()) {
            return DEL_NOT_SATISFIED;
        }
        if (stat.first->is_null() && !stat.second->is_null()) {
            return DEL_PARTIAL_SATISFIED;
        }
    }

    int ret = DEL_NOT_SATISFIED;
    switch (op) {
    case OP_EQ: {
        if (operand_field->cmp(stat.first) == 0 && operand_field->cmp(stat.second) == 0) {
            ret = DEL_SATISFIED;
        } else if (operand_field->cmp(stat.first) >= 0 && operand_field->cmp(stat.second) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_NOT_SATISFIED;
        }
        return ret;
    }
    case OP_NE: {
        if (operand_field->cmp(stat.first) == 0 && operand_field->cmp(stat.second) == 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) >= 0 && operand_field->cmp(stat.second) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_SATISFIED;
        }
        return ret;
    }
    case OP_LT: {
        if (operand_field->cmp(stat.first) <= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.second) > 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_LE: {
        if (operand_field->cmp(stat.first) < 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.second) >= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GT: {
        if (operand_field->cmp(stat.second) >= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) < 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GE: {
        if (operand_field->cmp(stat.second) > 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) <= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_IN: {
        if (stat.first->cmp(stat.second) == 0) {
            if (operand_set.find(stat.first) != operand_set.end()) {
                ret = DEL_SATISFIED;
            } else {
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (min_value_filed->cmp(stat.second) <= 0 && max_value_filed->cmp(stat.first) >= 0) {
                ret = DEL_PARTIAL_SATISFIED;
            }
        }
        return ret;
    }
    case OP_NOT_IN: {
        if (stat.first->cmp(stat.second) == 0) {
            if (operand_set.find(stat.first) == operand_set.end()) {
                ret = DEL_SATISFIED;
            } else {
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (min_value_filed->cmp(stat.second) > 0 || max_value_filed->cmp(stat.first) < 0) {
                ret = DEL_PARTIAL_SATISFIED;
            }
        }
        return ret;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                // impossible that min is not null and max is null
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_NOT_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                ret = DEL_SATISFIED;
            }
        }
        return ret;
    }
    default:
        break;
    }
    return ret;
}

bool Cond::eval(const segment_v2::BloomFilter* bf) const {
    // By a single column BloomFilter filter the block
    switch (op) {
    case OP_EQ: {
        if (operand_field->is_string_type()) {
            Slice* slice = (Slice*)(operand_field->ptr());
            return bf->test_bytes(slice->data, slice->size);
        } else {
            return bf->test_bytes(operand_field->ptr(), operand_field->size());
        }
    }
    case OP_IN: {
        for (const WrapperField* f : operand_set) {
            if (f->is_string_type()) {
                Slice* slice = (Slice*)(f->ptr());
                RETURN_IF(bf->test_bytes(slice->data, slice->size), true);
            } else {
                RETURN_IF(bf->test_bytes(f->ptr(), f->size()), true);
            }
        }
        return false;
    }
    case OP_IS: {
        // IS [NOT] NULL can only used in to filter IS NULL predicate.
        if (operand_field->is_null()) {
            return bf->test_bytes(nullptr, 0);
        }
    }
    default:
        break;
    }

    return true;
}

// Convert a Cond to another type Cond
Status Cond::convert_to(FieldType type, std::unique_ptr<Cond>* output) const {
    std::unique_ptr<Cond> new_cond(new Cond());

    new_cond->op = op;
    if (operand_field != nullptr) {
        new_cond->operand_field = WrapperField::create_by_type(type);
        if (operand_field->is_null()) {
            // When OP is is_null or is not null, src_field may be null
            new_cond->operand_field->set_is_null(true);
        } else {
            new_cond->operand_field->set_is_null(false);
            new_cond->operand_field->from_string(operand_field->to_string());
        }
    }
    for (auto& src_field : operand_set) {
        std::unique_ptr<WrapperField> dst_field(WrapperField::create_by_type(type));

        // When this is in predicate, just convert as not null.
        dst_field->set_is_null(false);
        dst_field->from_string(src_field->to_string());

        new_cond->operand_set.emplace(dst_field.release());
    }

    *output = std::move(new_cond);
    return Status::OK();
}

CondColumn::~CondColumn() {
    for (auto& it : _conds) {
        delete it;
    }
}

// PRECONDITION 1. index is valid; 2. at least has one operand
OLAPStatus CondColumn::add_cond(const TCondition& tcond, const TabletColumn& column) {
    std::unique_ptr<Cond> cond(new Cond());
    auto res = cond->init(tcond, column);
    if (res != OLAP_SUCCESS) {
        return res;
    }
    _conds.push_back(cond.release());
    return OLAP_SUCCESS;
}

bool CondColumn::eval(const RowCursor& row) const {
    // Filters a single row of data by all the query criteria on a column
    auto cell = row.cell(_col_index);
    for (const auto& each_cond : _conds) {
        // As long as there is one condition not satisfied, we can return false
        if (!each_cond->eval(cell)) {
            return false;
        }
    }

    return true;
}

bool CondColumn::eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
    // Filters version by all the query condition on a column
    for (const auto& each_cond : _conds) {
        if (!each_cond->eval(statistic)) {
            return false;
        }
    }

    return true;
}

int CondColumn::del_eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
    // Filters version by all delete conditions on a column

    /*
     * the relationship between cond A and B is A & B.
     * if all delete condition is satisfied, the data can be filtered.
     * elseif any delete condition is not satisfied, the data can't be filtered.
     * else is the partial satisfied.
    */
    bool del_partial_statified = false;
    bool del_not_statified = false;
    for (const auto& each_cond : _conds) {
        int del_ret = each_cond->del_eval(statistic);
        if (DEL_SATISFIED == del_ret) {
            continue;
        }
        if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_statified = true;
        } else {
            del_not_statified = true;
            break;
        }
    }
    if (del_not_statified || _conds.empty()) {
        // if the size of condcolumn vector is zero,
        // the delete condition is not satisfied.
        return DEL_NOT_SATISFIED;
    }
    if (del_partial_statified) {
        return DEL_PARTIAL_SATISFIED;
    } else {
        return DEL_SATISFIED;
    }
}

bool CondColumn::eval(const segment_v2::BloomFilter* bf) const {
    for (const auto& each_cond : _conds) {
        if (!each_cond->eval(bf)) {
            return false;
        }
    }

    return true;
}

Status CondColumn::convert_to(FieldType new_type, std::unique_ptr<CondColumn>* output) const {
    std::unique_ptr<CondColumn> new_cond_column(new CondColumn());
    new_cond_column->_is_key = _is_key;
    new_cond_column->_col_index = _col_index;
    for (const auto src_cond : _conds) {
        std::unique_ptr<Cond> new_cond;
        RETURN_IF_ERROR(src_cond->convert_to(new_type, &new_cond));
        new_cond_column->_conds.emplace_back(new_cond.release());
    }
    *output = std::move(new_cond_column);
    return Status::OK();
}

OLAPStatus Conditions::append_condition(const TCondition& tcond) {
    int32_t index = _schema->field_index(tcond.column_name);
    if (index < 0) {
        LOG(WARNING) << "invalid field name=" << tcond.column_name;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Skip column whose type is string or float
    const TabletColumn& column = _schema->column(index);
    if (column.type() == OLAP_FIELD_TYPE_DOUBLE || column.type() == OLAP_FIELD_TYPE_FLOAT) {
        return OLAP_SUCCESS;
    }

    auto it = _columns.find(index);
    if (it == _columns.end()) {
        auto cond_col = new CondColumn(*_schema, index);
        _columns[index] = cond_col;
        return cond_col->add_cond(tcond, column);
    } else {
        return it->second->add_cond(tcond, column);
    }
}

bool Conditions::delete_conditions_eval(const RowCursor& row) const {
    // rowcurser is filtered by delete criteria on all columns
    if (_columns.empty()) {
        return false;
    }

    for (const auto& each_cond : _columns) {
        if (_cond_column_is_key_or_duplicate(each_cond.second) && !each_cond.second->eval(row)) {
            return false;
        }
    }

    VLOG(3) << "Row meets the delete conditions. "
            << "condition_count=" << _columns.size() << ", row=" << row.to_string();
    return true;
}

CondColumn* Conditions::get_column(int32_t cid) const {
    auto iter = _columns.find(cid);
    if (iter != _columns.end()) {
        return iter->second;
    }
    return nullptr;
}

Status Conditions::convert_to(const Conditions** output, const std::vector<FieldType>& new_types,
                              ObjectPool* obj_pool) const {
    bool needs_convert = false;
    for (const auto& iter : _columns) {
        auto cid = iter.first;
        if (_schema->column(cid).type() != new_types[cid]) {
            needs_convert = true;
            break;
        }
    }
    if (!needs_convert) {
        *output = this;
        return Status::OK();
    }

    Conditions* new_cond = obj_pool->add(new Conditions());
    new_cond->_schema = _schema;
    for (const auto& iter : _columns) {
        auto cid = iter.first;
        std::unique_ptr<CondColumn> new_cond_column;
        RETURN_IF_ERROR(iter.second->convert_to(new_types[cid], &new_cond_column));
        new_cond->_columns.emplace(cid, new_cond_column.release());
    }
    *output = new_cond;
    return Status::OK();
}

} // namespace starrocks
