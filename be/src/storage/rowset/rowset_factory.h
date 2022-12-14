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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset_factory.h

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

#include "gen_cpp/olap_file.pb.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class RowsetWriter;
class RowsetWriterContext;

class RowsetFactory {
public:
    // return OK on success and set inited rowset in `*rowset`.
    // return error if failed to create or init rowset.
    static Status create_rowset(const TabletSchema* schema, const std::string& rowset_path,
                                const RowsetMetaSharedPtr& rowset_meta, RowsetSharedPtr* rowset);

    // create and init rowset writer.
    // return OK on success and set `*output` to inited rowset writer.
    // return error if failed
    static Status create_rowset_writer(const RowsetWriterContext& context, std::unique_ptr<RowsetWriter>* output);
};

} // namespace starrocks
