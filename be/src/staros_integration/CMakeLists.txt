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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/staros_integration")

set(STAROS_INTEGRATION_FILES
    staros_status.cpp
    staros_starcache.cpp
    staros_worker.cpp
    staros_worker_metrics.cpp
    staros_worker_runtime.cpp
)

ADD_BE_LIB(StarOSIntegration
    ${STAROS_INTEGRATION_FILES}
)
