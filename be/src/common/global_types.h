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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/global_types.h

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
namespace starrocks {

// For now, these are simply ints; if we find we need to generate ids in the
// backend, we can also introduce separate classes for these to make them
// assignment-incompatible.
typedef int TupleId;
typedef int SlotId;
typedef int TableId;
typedef int PlanNodeId;

// Mapping from input slot to output slot of an ExecNode.
// It is used for pipeline to rewrite runtime in filters.
struct TupleSlotMapping {
    TupleId from_tuple_id;
    SlotId from_slot_id;
    TupleId to_tuple_id;
    SlotId to_slot_id;

    TupleSlotMapping() = default;
    ~TupleSlotMapping() = default;

    TupleSlotMapping(const TupleSlotMapping&) = default;
    TupleSlotMapping(TupleSlotMapping&&) = default;
    TupleSlotMapping& operator=(TupleSlotMapping&&) = default;
    TupleSlotMapping& operator=(const TupleSlotMapping&) = default;

    TupleSlotMapping(TupleId from_tuple_id, SlotId from_slot_id, TupleId to_tuple_id, SlotId to_slot_id)
            : from_tuple_id(from_tuple_id),
              from_slot_id(from_slot_id),
              to_tuple_id(to_tuple_id),
              to_slot_id(to_slot_id) {}
};

}; // namespace starrocks
