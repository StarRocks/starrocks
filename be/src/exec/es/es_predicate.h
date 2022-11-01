// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/es/es_predicate.h

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

#pragma once
#include <string>
#include <utility>
#include <vector>

#include "cctz/time_zone.h"
#include "column/column.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "types/date_value.h"
#include "util/timezone_utils.h"

namespace starrocks {

class Status;
class ExprContext;
class EsPredicate;

class ExtLiteral {
public:
    virtual const std::string& to_string() const = 0;
    virtual ~ExtLiteral() = default;
};

// for vectorized call
class VExtLiteral : public ExtLiteral {
public:
    VExtLiteral(PrimitiveType type, vectorized::ColumnPtr column,
                const std::string& timezone = TimezoneUtils::default_time_zone) {
        DCHECK(!column->empty());
        // We need to convert the predicate column into the corresponding string.
        // Some types require special handling, because the default behavior of Datum may not match the behavior of ES.
        if (type == TYPE_DATE) {
            vectorized::ColumnViewer<TYPE_DATE> viewer(column);
            DCHECK(!viewer.is_null(0));
            _value = viewer.value(0).to_string();
        } else if (type == TYPE_DATETIME) {
            vectorized::ColumnViewer<TYPE_DATETIME> viewer(column);
            DCHECK(!viewer.is_null(0));
            vectorized::TimestampValue datetime_value = viewer.value(0);
            // Use timezone variable from FE
            cctz::time_zone timezone_obj;
            if (!TimezoneUtils::find_cctz_time_zone(timezone, timezone_obj)) {
                // Use default +8 timezone instead.
                TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, timezone_obj);
            }
            int64_t offsets = TimezoneUtils::to_utc_offset(timezone_obj);
            _value = std::to_string((datetime_value.to_unix_second() - offsets) * 1000);
        } else if (type == TYPE_BOOLEAN) {
            vectorized::ColumnViewer<TYPE_BOOLEAN> viewer(column);
            if (viewer.value(0)) {
                _value = "true";
            } else {
                _value = "false";
            }
        } else {
            _value = _value_to_string(column);
        }
    }
    VExtLiteral() = default;
    const std::string& to_string() const override { return _value; }

private:
    static std::string _value_to_string(vectorized::ColumnPtr& column);
    std::string _value;
};

struct ExtColumnDesc {
    ExtColumnDesc(std::string name, TypeDescriptor type) : name(std::move(name)), type(std::move(type)) {}

    std::string name;
    TypeDescriptor type;
};

struct ExtPredicate {
    ExtPredicate(TExprNodeType::type node_type) : node_type(node_type) {}
    virtual ~ExtPredicate() = default;

    TExprNodeType::type node_type;
};

// this used for placeholder for compound_predicate
// reserved for compound_not
struct ExtCompPredicates : public ExtPredicate {
    ExtCompPredicates(TExprOpcode::type expr_op, std::vector<EsPredicate*> es_predicates)
            : ExtPredicate(TExprNodeType::COMPOUND_PRED), op(expr_op), conjuncts(std::move(es_predicates)) {}

    TExprOpcode::type op;
    std::vector<EsPredicate*> conjuncts;
};

struct ExtBinaryPredicate : public ExtPredicate {
    ExtBinaryPredicate(TExprNodeType::type node_type, const std::string& name, const TypeDescriptor& type,
                       TExprOpcode::type op, const ExtLiteral* value)
            : ExtPredicate(node_type), col(name, type), op(op), value(value) {}

    ExtColumnDesc col;
    TExprOpcode::type op;
    const ExtLiteral* value;
};

struct ExtInPredicate : public ExtPredicate {
    ExtInPredicate(TExprNodeType::type node_type, bool is_not_in, const std::string& name, const TypeDescriptor& type,
                   std::vector<ExtLiteral*> values)
            : ExtPredicate(node_type), is_not_in(is_not_in), col(name, type), values(std::move(values)) {}

    bool is_not_in;
    ExtColumnDesc col;
    std::vector<ExtLiteral*> values;
};

struct ExtLikePredicate : public ExtPredicate {
    ExtLikePredicate(TExprNodeType::type node_type, const std::string& name, const TypeDescriptor& type,
                     const ExtLiteral* value)
            : ExtPredicate(node_type), col(name, type), value(value) {}

    ExtColumnDesc col;
    const ExtLiteral* value;
};

struct ExtIsNullPredicate : public ExtPredicate {
    ExtIsNullPredicate(TExprNodeType::type node_type, const std::string& name, const TypeDescriptor& type,
                       bool is_not_null)
            : ExtPredicate(node_type), col(name, type), is_not_null(is_not_null) {}

    ExtColumnDesc col;
    bool is_not_null;
};

struct ExtFunction : public ExtPredicate {
    ExtFunction(TExprNodeType::type node_type, std::string func_name, std::vector<ExtColumnDesc> cols,
                std::vector<ExtLiteral*> values)
            : ExtPredicate(node_type),
              func_name(std::move(func_name)),
              cols(std::move(cols)),
              values(std::move(values)) {}

    const std::string func_name;
    std::vector<ExtColumnDesc> cols;
    std::vector<ExtLiteral*> values;
};

class EsPredicate {
public:
    EsPredicate(ExprContext* context, const TupleDescriptor* tuple_desc, const std::string& timezone, ObjectPool* pool);
    ~EsPredicate();
    const std::vector<ExtPredicate*>& get_predicate_list();
    Status build_disjuncts_list(bool use_vectorized = true);
    // public for tests
    EsPredicate(const std::vector<ExtPredicate*>& all_predicates) { _disjuncts = all_predicates; };

    Status get_es_query_status() { return _es_query_status; }

    void set_field_context(const std::map<std::string, std::string>& field_context) { _field_context = field_context; }

private:
    Status _vec_build_disjuncts_list(const Expr* conjunct);
    // used in vectorized mode
    Status _build_binary_predicate(const Expr* conjunct, bool* handled);
    Status _build_functioncall_predicate(const Expr* conjunct, bool* handled);
    Status _build_in_predicate(const Expr* conjunct, bool* handled);
    Status _build_compound_predicate(const Expr* conjunct, bool* handled);

    const SlotDescriptor* get_slot_desc(SlotId slot_id);

    ExprContext* _context = nullptr;
    int _disjuncts_num = 0;
    const TupleDescriptor* _tuple_desc = nullptr;
    std::vector<ExtPredicate*> _disjuncts;
    Status _es_query_status;
    const std::string _timezone;
    ObjectPool* _pool = nullptr;
    std::map<std::string, std::string> _field_context;
};

} // namespace starrocks
