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

#include "runtime/agg_state_desc.h"

#include <memory>

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"

namespace starrocks {

// Create a new AggStateDesc from a thrift TTypeDesc.
AggStateDesc AggStateDesc::from_thrift(const TAggStateDesc& desc) {
    VLOG(2) << "TAggStateDesc:" << apache::thrift::ThriftDebugString(desc);
    std::string agg_func_name = desc.agg_func_name;
    // return type
    auto return_type = TypeDescriptor::from_thrift(desc.ret_type);
    // arg types
    std::vector<TypeDescriptor> arg_types;
    for (auto& arg_type : desc.arg_types) {
        arg_types.emplace_back(TypeDescriptor::from_thrift(arg_type));
    }
    bool result_nullable = desc.result_nullable;
    int func_version = desc.func_version;
    return AggStateDesc{agg_func_name, std::move(return_type), std::move(arg_types), result_nullable, func_version};
}

// Transform this AggStateDesc to a thrift TTypeDesc.
void AggStateDesc::to_thrift(TAggStateDesc* t) {
    t->agg_func_name = _func_name;
    t->result_nullable = _is_result_nullable;
    t->func_version = _func_version;
    // return type
    t->ret_type = _return_type.to_thrift();
    // arg types
    for (auto& arg_type : _arg_types) {
        t->arg_types.push_back(arg_type.to_thrift());
    }
}

AggStateDesc AggStateDesc::from_protobuf(const AggStateDescPB& desc) {
    auto& agg_func_name = desc.agg_func_name();
    bool is_result_nullable = desc.is_result_nullable();
    int func_version = desc.func_version();
    std::vector<TypeDescriptor> arg_types;
    // arg types
    for (auto& arg_type : desc.arg_types()) {
        arg_types.emplace_back(TypeDescriptor::from_protobuf(arg_type));
    }
    // ret type
    auto ret_type = TypeDescriptor::from_protobuf(desc.ret_type());
    return AggStateDesc{agg_func_name, std::move(ret_type), std::move(arg_types), is_result_nullable, func_version};
}

void AggStateDesc::to_protobuf(AggStateDescPB* desc) {
    desc->set_agg_func_name(this->get_func_name());
    desc->set_is_result_nullable(this->is_result_nullable());
    desc->set_func_version(this->get_func_version());
    // arg types
    for (auto& arg_type : this->get_arg_types()) {
        auto* arg_type_pb = desc->add_arg_types();
        *arg_type_pb = arg_type.to_protobuf();
    }
    // ret type
    auto ret_type_desc = this->get_return_type();
    auto* ret_type_pb = desc->mutable_ret_type();
    *ret_type_pb = ret_type_desc.to_protobuf();
}

void AggStateDesc::thrift_to_protobuf(const TAggStateDesc& desc, AggStateDescPB* pb) {
    pb->set_agg_func_name(desc.agg_func_name);
    pb->set_is_result_nullable(desc.result_nullable);
    pb->set_func_version(desc.func_version);
    // arg types
    for (auto& arg_type : desc.arg_types) {
        auto arg_type_desc = TypeDescriptor::from_thrift(arg_type);
        auto* arg_type_pb = pb->add_arg_types();
        *arg_type_pb = arg_type_desc.to_protobuf();
    }
    // ret type
    auto ret_type_desc = TypeDescriptor::from_thrift(desc.ret_type);
    auto* ret_type_pb = pb->mutable_ret_type();
    *ret_type_pb = ret_type_desc.to_protobuf();
}

std::string AggStateDesc::debug_string() const {
    std::stringstream ss;
    ss << "[" << _func_name << ", args:<";
    for (size_t i = 0; i < _arg_types.size(); i++) {
        if (i != _arg_types.size() - 1) {
            ss << _arg_types[i] << ", ";
        } else {
            ss << _arg_types[i] << ">";
        }
    }
    ss << ", ret:" << _return_type << ", result_nullable:" << _is_result_nullable << ", func_version:" << _func_version
       << "]";
    return ss.str();
}

const AggregateFunction* AggStateDesc::get_agg_state_func(AggStateDesc* agg_state_desc) {
    DCHECK(agg_state_desc);
    auto* agg_function = get_aggregate_function(agg_state_desc->get_func_name(), agg_state_desc->get_return_type(),
                                                agg_state_desc->get_arg_types(), agg_state_desc->is_result_nullable(),
                                                TFunctionBinaryType::BUILTIN, agg_state_desc->get_func_version());
    if (agg_function == nullptr) {
        LOG(WARNING) << "Failed to get aggregate function for " << agg_state_desc->debug_string();
    }
    return agg_function;
}

} // namespace starrocks