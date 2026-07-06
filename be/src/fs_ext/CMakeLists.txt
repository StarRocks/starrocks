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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/fs_ext")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/fs_ext")

ADD_BE_LIB(HdfsFileSystem
    hdfs/fs_hdfs.cpp
    hdfs/hdfs_fs_cache.cpp
    hdfs/hdfs_util.cpp
)

target_link_libraries(HdfsFileSystem PUBLIC FileSystem Runtime IO Common Base Gutil StarRocksGen hdfs jvm)
