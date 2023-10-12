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

#pragma once

#include <string>
#include <vector>

#include "gen_cpp/lake_service.pb.h"

namespace starrocks {
class FileSystem;
class Status;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

void vacuum(TabletManager* tablet_mgr, const VacuumRequest& request, VacuumResponse* response);

void vacuum_full(TabletManager* tablet_mgr, const VacuumFullRequest& request, VacuumFullResponse* response);

// REQUIRES:
//  - tablet_mgr != NULL
//  - request.tablet_ids_size() > 0
//  - response != NULL
void delete_tablets(TabletManager* tablet_mgr, const DeleteTabletRequest& request, DeleteTabletResponse* response);

// Batch deletion with bvar statistics.
//
// REQUIRES:
//  - All path in |paths| must have the same filesystem scheme and have a common parent directory.
//  This usually means that all files should be in the same table partition directory on the object store.
Status delete_files(const std::vector<std::string>& paths);

} // namespace starrocks::lake
