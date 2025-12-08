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

#include "storage/lake/tablet_writer.h"

#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

void TabletWriter::check_global_dict(SegmentWriter* segment_writer) {
    const auto& seg_global_dict_columns_valid_info = segment_writer->global_dict_columns_valid_info();
    for (const auto& it : seg_global_dict_columns_valid_info) {
        if (!it.second) {
            _global_dict_columns_valid_info[it.first] = false;
        } else {
            if (const auto& iter = _global_dict_columns_valid_info.find(it.first);
                iter == _global_dict_columns_valid_info.end()) {
                _global_dict_columns_valid_info[it.first] = true;
            }
        }
    }
}

} // namespace starrocks::lake