# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/data_workflows")

set(DATA_WORKFLOWS_FILES
    clone/engine_clone_task.cpp
    compaction/engine_compaction_control_task.cpp
    compaction/engine_manual_compaction_task.cpp
    consistency/engine_checksum_task.cpp
    load/engine_batch_load_task.cpp
    load/lake_spark_load_handler.cpp
    load/push_broker_reader.cpp
    load/push_handler.cpp
    load/tablet_writer/lake_tablets_channel.cpp
    load/tablet_writer/load_channel.cpp
    load/tablet_writer/load_channel_mgr.cpp
    load/tablet_writer/local_tablets_channel.cpp
    load/tablet_writer/tablets_channel.cpp
    migration/engine_storage_migration_task.cpp
    schema_change/engine_alter_tablet_task.cpp
)

ADD_BE_LIB(DataWorkflows
    ${DATA_WORKFLOWS_FILES}
)

target_link_libraries(DataWorkflows PUBLIC
    Storage
    Exec
    Runtime
    StoragePrimitive
    Common
    Base
    Gutil
    StarRocksGen
    PRIVATE
    HttpCore
)
