/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Adaptor.hh"
#include "ToolTest.hh"
#include "orc/OrcFile.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

TEST(TestFileContents, testRaw) {
    const std::string pgm = findProgram("tools/src/orc-contents");
    const std::string file = findExample("TestOrcFile.test1.orc");
    const std::string expected =
            "{\"boolean1\": false, \"byte1\": 1, \"short1\": 1024, \"int1\": 65536, "
            "\"long1\": 9223372036854775807, \"float1\": 1, \"double1\": -15,"
            " \"bytes1\": [0, 1, 2, 3, 4], \"string1\": \"hi\", \"middle\": "
            "{\"list\": [{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, "
            "\"string1\": \"sigh\"}]}, \"list\": [{\"int1\": 3, \"string1\": "
            "\"good\"}, {\"int1\": 4, \"string1\": \"bad\"}], \"map\": []}\n"
            "{\"boolean1\": true, \"byte1\": 100, \"short1\": 2048, \"int1\": 65536,"
            " \"long1\": 9223372036854775807, \"float1\": 2, \"double1\": -5, "
            "\"bytes1\": [], \"string1\": \"bye\", \"middle\": {\"list\": "
            "[{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, \"string1\":"
            " \"sigh\"}]}, \"list\": [{\"int1\": 100000000, \"string1\": \"cat\"},"
            " {\"int1\": -100000, \"string1\": \"in\"}, {\"int1\": 1234, "
            "\"string1\": \"hat\"}], \"map\": [{\"key\": \"chani\", \"value\": "
            "{\"int1\": 5, \"string1\": \"chani\"}}, {\"key\": \"mauddib\", "
            "\"value\": {\"int1\": 1, \"string1\": \"mauddib\"}}]}\n";

    std::string output;
    std::string error;

    EXPECT_EQ(0, runProgram({pgm, file}, output, error));
    EXPECT_EQ(expected, output);
    EXPECT_EQ("", error);
}

TEST(TestFileContents, testSelectedColumns) {
    const std::string pgm = findProgram("tools/src/orc-contents");
    const std::string file = findExample("TestOrcFile.test1.orc");
    const std::string options = "--columns=1,3,5,7";
    const std::string expected =
            "{\"byte1\": 1, \"int1\": 65536, \"float1\": 1, \"bytes1\": [0, 1, 2, 3, 4]}\n"
            "{\"byte1\": 100, \"int1\": 65536, \"float1\": 2, \"bytes1\": []}\n";

    std::string output;
    std::string error;

    EXPECT_EQ(0, runProgram({pgm, options, file}, output, error));
    EXPECT_EQ(expected, output);
    EXPECT_EQ("", error);
}
