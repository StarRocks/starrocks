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

#include "exec/pipeline/hashjoin/hash_joiner_factory.h"

namespace starrocks::pipeline {

Status HashJoinerFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_param._conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._other_join_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_param._conjunct_ctxs, state));
    return Status::OK();
}

void HashJoinerFactory::close(RuntimeState* state) {
    Expr::close(_param._conjunct_ctxs, state);
    Expr::close(_param._other_join_conjunct_ctxs, state);
    Expr::close(_param._probe_expr_ctxs, state);
    Expr::close(_param._build_expr_ctxs, state);
}

HashJoinerPtr HashJoinerFactory::create_builder(int32_t builder_dop, int32_t builder_driver_seq) {
    _builder_dop = builder_dop;
    return _create_joiner(_builder_map, builder_driver_seq);
}

HashJoinerPtr HashJoinerFactory::create_prober(int32_t prober_dop, int32_t prober_driver_seq) {
    _prober_dop = prober_dop;
    DCHECK_GT(_builder_dop, 0);
    DCHECK_GE(_prober_dop, _builder_dop);
    // There is one-to-one correspondence between the prober and builder,
    // so the prober can use builder directly.
    if (_prober_dop == _builder_dop) {
        return get_builder(prober_dop, prober_driver_seq);
    }
    return _create_joiner(_prober_map, prober_driver_seq);
}

HashJoinerPtr HashJoinerFactory::get_builder(int32_t prober_dop, int32_t prober_driver_seq) {
    _prober_dop = prober_dop;
    return _builder_map[prober_driver_seq % _builder_dop];
}

HashJoinerPtr HashJoinerFactory::_create_joiner(HashJoinerMap& joiner_map, int32_t driver_sequence) {
    if (auto it = joiner_map.find(driver_sequence); it != joiner_map.end()) {
        return it->second;
    }

    auto joiner = std::make_shared<HashJoiner>(_param);
    joiner_map.emplace(driver_sequence, joiner);
    return joiner;
}

} // namespace starrocks::pipeline
