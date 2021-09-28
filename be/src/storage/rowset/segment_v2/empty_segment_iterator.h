// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/empty_segment_iterator.h

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

#include "common/status.h"
#include "storage/iterators.h"
#include "storage/rowset/segment_v2/segment.h"
#include "storage/schema.h"

namespace starrocks {
namespace segment_v2 {

class EmptySegmentIterator : public RowwiseIterator {
public:
    explicit EmptySegmentIterator(const Schema& schema);
    ~EmptySegmentIterator() override = default;
    Status init(const StorageReadOptions& opts) override { return Status::OK(); }
    const Schema& schema() const override { return _schema; }
    Status next_batch(RowBlockV2* row_block) override;

private:
    Schema _schema;
};

} // namespace segment_v2
} // namespace starrocks