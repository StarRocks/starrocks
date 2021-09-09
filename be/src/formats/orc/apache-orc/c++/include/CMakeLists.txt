# This file is made available under Elastic License 2.0.
# This file is based on code available under the Apache license here:
#   https://github.com/apache/orc/tree/main/c++/include/CMakeLists.txt

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

configure_file (
  "orc/orc-config.hh.in"
  "orc/orc-config.hh"
  )

if (NOT DEFINED ENV{STARROCKS_HOME}) # starrocks: no need to install header files.

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/orc/orc-config.hh"
  DESTINATION "include/orc"
  )

install(DIRECTORY
        "orc/"
        DESTINATION "include/orc"
        FILES_MATCHING PATTERN "*.hh")

endif()
