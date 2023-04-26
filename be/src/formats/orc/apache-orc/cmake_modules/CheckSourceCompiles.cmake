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

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

INCLUDE(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
    #include <initializer_list>
    struct A {
      A(std::initializer_list<int> list);
    };
    int main(int,char*[]){
    }"
  ORC_CXX_HAS_INITIALIZER_LIST
)

CHECK_CXX_SOURCE_COMPILES("
    int main(int,char*[]) noexcept {
      return 0;
    }"
  ORC_CXX_HAS_NOEXCEPT
)

CHECK_CXX_SOURCE_COMPILES("
    int main(int,char* argv[]){
      return argv[0] != nullptr;
    }"
  ORC_CXX_HAS_NULLPTR
)

CHECK_CXX_SOURCE_COMPILES("
    struct A {
      virtual ~A();
      virtual void foo();
    };
    struct B: public A {
      virtual void foo() override;
    };
    int main(int,char*[]){
    }"
  ORC_CXX_HAS_OVERRIDE
)

CHECK_CXX_SOURCE_COMPILES("
    #include<memory>
    int main(int,char* []){
      std::unique_ptr<int> ptr(new int);
    }"
  ORC_CXX_HAS_UNIQUE_PTR
)

CHECK_CXX_SOURCE_COMPILES("
    #include <cstdint>
    int main(int, char*[]) { }"
  ORC_CXX_HAS_CSTDINT
)

CHECK_CXX_SOURCE_COMPILES("
    #include <thread>
    int main(void) {
      thread_local int s;
      return s;
    }"
  ORC_CXX_HAS_THREAD_LOCAL
)
