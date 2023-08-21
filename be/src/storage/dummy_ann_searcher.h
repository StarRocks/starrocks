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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet.h

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

#include "tenann/common/seq_view.h"
#include "tenann/searcher/ann_searcher.h"
#include "tenann/store/index_meta.h"

namespace starrocks {
class DummyAnnSearcher : public tenann::AnnSearcher {
public:
  DummyAnnSearcher(const tenann::IndexMeta& meta):tenann::AnnSearcher(meta) {};

  T_FORBID_MOVE(DummyAnnSearcher);
  T_FORBID_COPY_AND_ASSIGN(DummyAnnSearcher);

    /// mock
  void dummyAnnSearch(int64_t* result_ids, uint8_t* result_distances) {
    result_ids[0] = 0;
    result_ids[1] = 1;
    result_ids[2] = 2;

    result_distances[0] = 2;
    result_distances[1] = 1;
    result_distances[2] = 3;
  };

  /// ANN搜索接口，只返回k近邻的id
  void AnnSearch(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_id) {
  };

  /// ANN搜索接口，同时返回k近邻的id和距离
  void AnnSearch(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances) {
  };
};

} // namespace starrocks
