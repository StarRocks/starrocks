// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/merger.h

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

#ifndef STARROCKS_BE_SRC_OLAP_MERGER_H
#define STARROCKS_BE_SRC_OLAP_MERGER_H

#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/tablet.h"

namespace starrocks {

class Merger {
public:
    struct Statistics {
        // number of rows written to the destination rowset after merge
        int64_t output_rows = 0;
        int64_t merged_rows = 0;
        int64_t filtered_rows = 0;
    };

    // merge rows from `src_rowset_readers` and write into `dst_rowset_writer`.
    // return OLAP_SUCCESS and set statistics into `*stats_output`.
    // return others on error
    static OLAPStatus merge_rowsets(int64_t mem_limit, const TabletSharedPtr& tablet, ReaderType reader_type,
                                    const std::vector<RowsetReaderSharedPtr>& src_rowset_readers,
                                    RowsetWriter* dst_rowset_writer, Statistics* stats_output);
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_MERGER_H
