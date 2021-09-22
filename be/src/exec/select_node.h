// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/select_node.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_SELECT_NODE_H
#define STARROCKS_BE_SRC_QUERY_EXEC_SELECT_NODE_H

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"

namespace starrocks {

class Tuple;
class TupleRow;

// Node that evaluates conjuncts and enforces a limit but otherwise passes along
// the rows pulled from its child unchanged.

class SelectNode : public ExecNode {
public:
    SelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    virtual Status close(RuntimeState* state);

private:
    // true if last get_next() call on child signalled eos
    bool _child_eos;

    RuntimeProfile::Counter* _conjunct_evaluate_timer = nullptr;
};

} // namespace starrocks

#endif
