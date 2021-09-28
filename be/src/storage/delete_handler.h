// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/delete_handler.h

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

#ifndef STARROCKS_BE_SRC_OLAP_DELETE_HANDLER_H
#define STARROCKS_BE_SRC_OLAP_DELETE_HANDLER_H

#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_define.h"
#include "storage/tablet_schema.h"

namespace starrocks {

typedef google::protobuf::RepeatedPtrField<DeletePredicatePB> DelPredicateArray;
class Conditions;
class RowCursor;

class DeleteConditionHandler {
public:
    DeleteConditionHandler() = default;
    ~DeleteConditionHandler() = default;

    // generated DeletePredicatePB by TCondition
    OLAPStatus generate_delete_predicate(const TabletSchema& schema, const std::vector<TCondition>& conditions,
                                         DeletePredicatePB* del_pred);

    // Check if cond is a valid delete condition
    // If ok return OLAP_SUCCESS, otherwise return OLAP_ERR_DELETE_INVALID_CONDITION
    OLAPStatus check_condition_valid(const TabletSchema& tablet_schema, const TCondition& cond);

    // construct sub condition from TCondition
    std::string construct_sub_predicates(const TCondition& condition);

private:
    int32_t _get_field_index(const TabletSchema& schema, const std::string& field_name) const {
        for (int i = 0; i < schema.num_columns(); i++) {
            if (schema.column(i).name() == field_name) {
                return i;
            }
        }
        LOG(WARNING) << "invalid field name. name='" << field_name;
        return -1;
    }
    bool is_condition_value_valid(const TabletColumn& column, const TCondition& cond, const string& value_str);
};

// Represents a delete condition
struct DeleteConditions {
    DeleteConditions() {}
    ~DeleteConditions() = default;

    int32_t filter_version{0};     // delete condition version
    Conditions* del_cond{nullptr}; // delete condition
};

// Used to check if one row is deleted
// 1. Initialize with a version
//    OLAPStatus res;
//    DeleteHandler delete_handler;
//    res = delete_handler.init(tablet, condition_version);
// 2. check if data is deleted
//    bool filter_data;
//    filter_data = delete_handler.is_filter_data(data_version, row_cursor);
// 3. If there are many rows to check, call is_filter_data() repeatly
// 4. destory
//    delete_handler.finalize();
//
// NOTE:
//    * Should hold header lock before calling init()
class DeleteHandler {
public:
    typedef std::vector<DeleteConditions>::size_type cond_num_t;

    DeleteHandler() {}
    ~DeleteHandler() = default;

    // Use regular expression to extract 'column_name', 'op' and 'operands'
    static bool parse_condition(const std::string& condition_str, TCondition* condition);

    OLAPStatus init(const TabletSchema& schema, const DelPredicateArray& delete_conditions, int32_t version);

    // Check if input row is deleted
    bool is_filter_data(const int32_t data_version, const RowCursor& row) const;

    cond_num_t conditions_num() const { return _del_conds.size(); }

    bool empty() const { return _del_conds.empty(); }

    std::vector<int32_t> get_conds_version();

    void finalize();

    const std::vector<DeleteConditions>& get_delete_conditions() const { return _del_conds; }

    void get_delete_conditions_after_version(int32_t version, std::vector<const Conditions*>* delete_conditions) const;

private:
    bool _is_inited{false};
    std::vector<DeleteConditions> _del_conds;
};

} // namespace starrocks
#endif // STARROCKS_BE_SRC_OLAP_DELETE_HANDLER_H
