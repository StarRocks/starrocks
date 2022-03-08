// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/slot_ref.cpp

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

#include "exprs/slot_ref.h"

#include <sstream>

#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

SlotRef::SlotRef(const TExprNode& node)
        : Expr(node, true),
          _slot_offset(-1), // invalid
          _null_indicator_offset(0, 0),
          _slot_id(node.slot_ref.slot_id),
          _tuple_id(node.slot_ref.tuple_id) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc)
        : Expr(desc->type(), true), _slot_offset(-1), _null_indicator_offset(0, 0), _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const TypeDescriptor& type)
        : Expr(type, true), _slot_offset(-1), _null_indicator_offset(0, 0), _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const TypeDescriptor& type, int offset, SlotId slot)
        : Expr(type, true), _slot_offset(offset), _null_indicator_offset(0, -1), _slot_id(slot) {}

Status SlotRef::prepare(const SlotDescriptor* slot_desc) {
    return Status::OK();
}

Status SlotRef::prepare(RuntimeState* state, ExprContext* ctx) {
    return Status::OK();
}

int SlotRef::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    slot_ids->push_back(_slot_id);
    return 1;
}

bool SlotRef::is_bound(const std::vector<TupleId>& tuple_ids) const {
    for (int tuple_id : tuple_ids) {
        if (_tuple_id == tuple_id) {
            return true;
        }
    }
    return false;
}

std::string SlotRef::debug_string() const {
    std::stringstream out;
    out << "SlotRef(slot_id=" << _slot_id << " tuple_idx=" << _tuple_idx << " slot_offset=" << _slot_offset
        << " null_indicator=" << _null_indicator_offset << " " << Expr::debug_string() << ")";
    return out.str();
}

ColumnPtr SlotRef::evaluate(ExprContext* context, vectorized::Chunk* ptr) {
    return get_column(this, ptr);
}

} // namespace starrocks
