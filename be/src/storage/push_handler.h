// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/push_handler.h

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

#ifndef STARROCKS_BE_SRC_OLAP_PUSH_HANDLER_H
#define STARROCKS_BE_SRC_OLAP_PUSH_HANDLER_H

#include <map>
#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "runtime/tuple.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class BinaryFile;

struct TabletVars {
    TabletSharedPtr tablet;
    RowsetSharedPtr rowset_to_add;
};

class PushHandler {
public:
    PushHandler() = default;
    ~PushHandler() = default;

    // Load local data file into specified tablet.
    OLAPStatus process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request, PushType push_type,
                                           std::vector<TTabletInfo>* tablet_info_vec);

    int64_t write_bytes() const { return _write_bytes; }
    int64_t write_rows() const { return _write_rows; }

private:
    OLAPStatus _convert(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset);
    OLAPStatus _convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset);

    void _get_tablet_infos(const std::vector<TabletVars>& tablet_infos, std::vector<TTabletInfo>* tablet_info_vec);

    OLAPStatus _do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request, PushType push_type,
                                       vector<TabletVars>* tablet_vars, std::vector<TTabletInfo>* tablet_info_vec);

private:
    // mainly tablet_id, version and delta file path
    TPushReq _request;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
    DISALLOW_COPY_AND_ASSIGN(PushHandler);
};

class PushBrokerReader {
public:
    PushBrokerReader() = default;
    ~PushBrokerReader() {
        _mem_pool.reset();
        if (_runtime_state != nullptr && _runtime_state->instance_mem_tracker() != nullptr) {
            _runtime_state->instance_mem_tracker()->release(_runtime_state->instance_mem_tracker()->consumption());
        }
    }

    OLAPStatus init(const Schema* schema, const TBrokerScanRange& t_scan_range, const TDescriptorTable& t_desc_tbl);
    OLAPStatus next(ContiguousRow* row);
    void print_profile();

    OLAPStatus close() {
        _ready = false;
        return OLAP_SUCCESS;
    }
    bool eof() const { return _eof; }
    MemPool* mem_pool() { return _mem_pool.get(); }

private:
    bool _ready = false;
    bool _eof = false;
    TupleDescriptor* _tuple_desc = nullptr;
    Tuple* _tuple = nullptr;
    const Schema* _schema = nullptr;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile = nullptr;
    std::unique_ptr<MemPool> _mem_pool;
};

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_PUSH_HANDLER_H
