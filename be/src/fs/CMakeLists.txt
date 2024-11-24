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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/fs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/fs")

set(EXEC_FILES
    fd_cache.cpp
    fs.cpp
    encrypt_file.cpp
    key_cache.cpp
    fs_posix.cpp
    fs_broker.cpp
    fs_memory.cpp
    fs_s3.cpp
    hdfs/fs_hdfs.cpp
    hdfs/hdfs_fs_cache.cpp
    s3/poco_http_client_factory.cpp
    s3/poco_http_client.cpp
    s3/poco_common.cpp
    fs_util.cpp
    )

if (NOT BUILD_FORMAT_LIB)
    list(APPEND EXEC_FILES fs_starlet.cpp)
endif()

add_library(FileSystem STATIC
    ${EXEC_FILES}
)
