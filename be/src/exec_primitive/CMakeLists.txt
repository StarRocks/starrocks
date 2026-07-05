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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exec_primitive")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exec_primitive")

set(EXEC_PRIMITIVE_FILES
    arrow/result_to_arrow_converter.cpp
    data_sink.cpp
    exec_node.cpp
    runtime_filter/runtime_filter_descriptor.cpp
    runtime_filter/runtime_filter_probe.cpp
    runtime_filter/runtime_filter_probe_wait.cpp
    runtime_filter/runtime_filter_registry.cpp
    runtime_filter/runtime_filter_helper.cpp
    pipeline/operator.cpp
    pipeline/operator_factory.cpp
    pipeline/source_operator.cpp
    pipeline/primitives/event.cpp
    pipeline/primitives/pipeline_metrics.cpp
    pipeline/primitives/pipeline_observer.cpp
    pipeline/scan/dynamic_morsel_queue.cpp
    pipeline/scan/fixed_morsel_queue.cpp
    pipeline/scan/morsel_queue.cpp
    pipeline/scan/morsel_queue_factory_base.cpp
    pipeline/scan/scan_morsel.cpp
    pipeline/scan/split_morsel_ticket_checker.cpp
)

ADD_BE_LIB(ExecPrimitive
    ${EXEC_PRIMITIVE_FILES}
)

target_link_libraries(ExecPrimitive PUBLIC
    Expr
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)
target_link_libraries(ExecPrimitive PRIVATE arrow)
