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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/types")

ADD_BE_LIB(TypesCore
    logical_type.cpp
)

ADD_BE_LIB(Types
    array_type_info.cpp
    bitmap_value.cpp
    date_value.cpp
    hll.cpp
    hll_sketch.cpp
    logical_type_size.cpp
    large_int_value.cpp
    map_type_info.cpp
    struct_type_info.cpp
    timestamp_value.cpp
    checker/type_checker.cpp
    checker/type_checker_xml_loader.cpp
    type_checker_manager.cpp
    variant_value.cpp
)
