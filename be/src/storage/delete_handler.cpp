// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/delete_handler.cpp

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

#include "storage/delete_handler.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <boost/regex.hpp>
#include <cerrno>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_common.h"
#include "storage/olap_cond.h"
#include "storage/utils.h"

using apache::thrift::ThriftDebugString;
using std::numeric_limits;
using std::vector;
using std::string;
using std::stringstream;

using boost::regex;
using boost::regex_error;
using boost::regex_match;
using boost::smatch;

using google::protobuf::RepeatedPtrField;

namespace starrocks {

OLAPStatus DeleteConditionHandler::generate_delete_predicate(const TabletSchema& schema,
                                                             const std::vector<TCondition>& conditions,
                                                             DeletePredicatePB* del_pred) {
    if (conditions.empty()) {
        LOG(WARNING) << "invalid parameters for store_cond."
                     << " condition_size=" << conditions.size();
        return OLAP_ERR_DELETE_INVALID_PARAMETERS;
    }

    // check delete condition meet the requirements
    for (const TCondition& condition : conditions) {
        if (check_condition_valid(schema, condition) != OLAP_SUCCESS) {
            LOG(WARNING) << "invalid condition. condition=" << ThriftDebugString(condition);
            return OLAP_ERR_DELETE_INVALID_CONDITION;
        }
    }

    // storage delete condition
    for (const TCondition& condition : conditions) {
        // condition.condition_op eq !*= or *=
        if (condition.condition_values.size() > 1) {
            InPredicatePB* in_pred = del_pred->add_in_predicates();
            in_pred->set_column_name(condition.column_name);
            bool is_not_in = condition.condition_op == "!*=";
            in_pred->set_is_not_in(is_not_in);
            for (const auto& condition_value : condition.condition_values) {
                in_pred->add_values(condition_value);
            }

            LOG(INFO) << "store one sub-delete condition. condition name=" << in_pred->column_name()
                      << ",condition size=" << in_pred->values().size();
        } else {
            string condition_str = construct_sub_predicates(condition);
            del_pred->add_sub_predicates(condition_str);
            LOG(INFO) << "store one sub-delete condition. condition=" << condition_str;
        }
    }
    del_pred->set_version(-1);

    return OLAP_SUCCESS;
}

std::string DeleteConditionHandler::construct_sub_predicates(const TCondition& condition) {
    string op = condition.condition_op;
    if (op == "<") {
        op += "<";
    } else if (op == ">") {
        op += ">";
    }
    string condition_str;
    if ("IS" == op) {
        condition_str = condition.column_name + " " + op + " " + condition.condition_values[0];
    } else {
        if (op == "*=") {
            op = "=";
        } else if (op == "!*=") {
            op = "!=";
        }
        condition_str = condition.column_name + op + condition.condition_values[0];
    }
    return condition_str;
}

bool DeleteConditionHandler::is_condition_value_valid(const TabletColumn& column, const TCondition& cond,
                                                      const string& value_str) {
    FieldType field_type = column.type();
    bool valid_condition = false;

    if ("IS" == cond.condition_op && ("NULL" == value_str || "NOT NULL" == value_str)) {
        valid_condition = true;
    } else if (field_type == OLAP_FIELD_TYPE_TINYINT) {
        valid_condition = valid_signed_number<int8_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_SMALLINT) {
        valid_condition = valid_signed_number<int16_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_INT) {
        valid_condition = valid_signed_number<int32_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_BIGINT) {
        valid_condition = valid_signed_number<int64_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_LARGEINT) {
        valid_condition = valid_signed_number<int128_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_TINYINT) {
        valid_condition = valid_unsigned_number<uint8_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_SMALLINT) {
        valid_condition = valid_unsigned_number<uint16_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_INT) {
        valid_condition = valid_unsigned_number<uint32_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_BIGINT) {
        valid_condition = valid_unsigned_number<uint64_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL || field_type == OLAP_FIELD_TYPE_DECIMAL_V2 ||
               is_decimalv3_field_type(field_type)) {
        valid_condition = valid_decimal(value_str, column.precision(), column.scale());
    } else if (field_type == OLAP_FIELD_TYPE_CHAR || field_type == OLAP_FIELD_TYPE_VARCHAR) {
        if (value_str.size() <= column.length()) {
            valid_condition = true;
        }
    } else if (field_type == OLAP_FIELD_TYPE_DATE || field_type == OLAP_FIELD_TYPE_DATE_V2 ||
               field_type == OLAP_FIELD_TYPE_DATETIME || field_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        valid_condition = valid_datetime(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_BOOL) {
        valid_condition = valid_bool(value_str);
    } else {
        LOG(WARNING) << "unknown field type " << field_type;
    }

    return valid_condition;
}

OLAPStatus DeleteConditionHandler::check_condition_valid(const TabletSchema& schema, const TCondition& cond) {
    // Checks for the existence of the specified column name
    int field_index = _get_field_index(schema, cond.column_name);

    if (field_index < 0) {
        LOG(WARNING) << "field is not existent. field_index=" << field_index;
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // Checks that the specified column is a key, is it type float or double.
    const TabletColumn& column = schema.column(field_index);

    if ((!column.is_key() && schema.keys_type() != KeysType::DUP_KEYS) || column.type() == OLAP_FIELD_TYPE_DOUBLE ||
        column.type() == OLAP_FIELD_TYPE_FLOAT) {
        LOG(WARNING) << "field is not key column, or storage model is not duplicate, or data type "
                        "is float or double.";
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // Check that the filter values specified in the delete condition
    // meet the requirements of each type itself
    // 1. For integer types (int8,int16,in32,int64,uint8,uint16,uint32,uint64), check for overflow
    // 2. For decimal type, checks whether the accuracy and scale specified
    //    when the table was created are exceeded
    // 3. For date and datetime types, checks that the specified filter value conforms to
    //    the date format and that an incorrect value is specified
    // 4. For string and varchar types, checks whether the specified filter value exceeds
    //    the length specified when creating the table
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op && cond.condition_values.size() != 1) {
        LOG(WARNING) << "invalid condition value size " << cond.condition_values.size();
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    for (int i = 0; i < cond.condition_values.size(); i++) {
        const string& value_str = cond.condition_values[i];
        if (!is_condition_value_valid(column, cond, value_str)) {
            LOG(WARNING) << "invalid condition value. [value=" << value_str << "]";
            return OLAP_ERR_DELETE_INVALID_CONDITION;
        }
    }

    return OLAP_SUCCESS;
}

bool DeleteHandler::parse_condition(const std::string& condition_str, TCondition* condition) {
    bool matched = true;
    smatch what;

    try {
        // Condition string format
        // eg:  condition_str="c1 = 1597751948193618247  and length(source)<1;\n;\n"
        //  group1:  ([^\0=<>!\*]{1,64}) matches "c1"
        //  group2:  ((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?:IS)) matches  "="
        //  group3:  ((?:[\s\S]+)?) matches "1597751948193618247  and length(source)<1;\n;\n"
        const char* const CONDITION_STR_PATTERN =
                R"(([^\0=<>!\*]{1,64})\s*((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?: IS ))\s*((?:[\s\S]+)?))";
        regex ex(CONDITION_STR_PATTERN);
        if (regex_match(condition_str, what, ex)) {
            if (condition_str.size() != what[0].str().size()) {
                matched = false;
            }
        } else {
            matched = false;
        }
    } catch (regex_error& e) {
        VLOG(3) << "fail to parse expr. [expr=" << condition_str << "; error=" << e.what() << "]";
        matched = false;
    }

    if (!matched) {
        return false;
    }

    condition->column_name = what[1].str();
    // 'is' predicate will be parsed as ' is ' which has extra space
    // remove extra space and set op as 'is'
    if (what[2].str().size() == 4 && strcasecmp(what[2].str().c_str(), " is ") == 0) {
        condition->condition_op = "IS";
    } else {
        condition->condition_op = what[2].str();
    }
    condition->condition_values.push_back(what[3].str());

    return true;
}

OLAPStatus DeleteHandler::init(const TabletSchema& schema, const DelPredicateArray& delete_conditions,
                               int32_t version) {
    if (_is_inited) {
        LOG(WARNING) << "reinitialize delete handler.";
        return OLAP_ERR_INIT_FAILED;
    }

    if (version < 0) {
        LOG(WARNING) << "invalid parameters. version=" << version;
        return OLAP_ERR_DELETE_INVALID_PARAMETERS;
    }

    DelPredicateArray::const_iterator it = delete_conditions.begin();

    for (; it != delete_conditions.end(); ++it) {
        // skip larger version
        if (it->version() > version) {
            continue;
        }

        DeleteConditions temp;
        temp.filter_version = it->version();

        temp.del_cond = new (std::nothrow) Conditions();

        if (temp.del_cond == nullptr) {
            LOG(FATAL) << "fail to malloc Conditions. size=" << sizeof(Conditions);
            return OLAP_ERR_MALLOC_ERROR;
        }

        temp.del_cond->set_tablet_schema(&schema);

        for (int i = 0; i != it->sub_predicates_size(); ++i) {
            TCondition condition;
            if (!parse_condition(it->sub_predicates(i), &condition)) {
                LOG(WARNING) << "fail to parse condition. condition=" << it->sub_predicates(i).c_str();
                return OLAP_ERR_DELETE_INVALID_PARAMETERS;
            }

            OLAPStatus res = temp.del_cond->append_condition(condition);
            if (OLAP_SUCCESS != res) {
                LOG(WARNING) << "fail to append condition. res=" << res;
                return res;
            }
        }

        for (int i = 0; i != it->in_predicates_size(); ++i) {
            TCondition condition;
            const InPredicatePB& in_predicate = it->in_predicates(i);
            condition.__set_column_name(in_predicate.column_name());
            if (in_predicate.is_not_in()) {
                condition.__set_condition_op("!*=");
            } else {
                condition.__set_condition_op("*=");
            }
            for (const auto& value : in_predicate.values()) {
                condition.condition_values.push_back(value);
            }
            OLAPStatus res = temp.del_cond->append_condition(condition);
            if (OLAP_SUCCESS != res) {
                LOG(WARNING) << "fail to append condition. res=" << res;
                return res;
            }
        }

        _del_conds.push_back(temp);
    }

    _is_inited = true;

    return OLAP_SUCCESS;
}

bool DeleteHandler::is_filter_data(const int32_t data_version, const RowCursor& row) const {
    if (_del_conds.empty()) {
        return false;
    }

    // Condition in _del_conds are OR,
    // If one version hit, return true
    for (const auto& _del_cond : _del_conds) {
        if (data_version <= _del_cond.filter_version && _del_cond.del_cond->delete_conditions_eval(row)) {
            return true;
        }
    }

    return false;
}

std::vector<int32_t> DeleteHandler::get_conds_version() {
    std::vector<int32_t> conds_version;

    for (auto& _del_cond : _del_conds) {
        conds_version.push_back(_del_cond.filter_version);
    }

    return conds_version;
}

void DeleteHandler::finalize() {
    if (!_is_inited) {
        return;
    }

    for (auto& _del_cond : _del_conds) {
        _del_cond.del_cond->finalize();
        delete _del_cond.del_cond;
    }

    _del_conds.clear();
    _is_inited = false;
}

void DeleteHandler::get_delete_conditions_after_version(int32_t version,
                                                        std::vector<const Conditions*>* delete_conditions) const {
    for (const auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            delete_conditions->emplace_back(del_cond.del_cond);
        }
    }
}

} // namespace starrocks
