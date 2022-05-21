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

TEST(TestCSVFileImport, test10rows) {
    // create an ORC file from importing the CSV file
    const std::string pgm1 = findProgram("tools/src/csv-import");
    const std::string csvFile = findExample("TestCSVFileImport.test10rows.csv");
    const std::string orcFile = "/tmp/test_csv_import_test_10_rows.orc";
    const std::string schema = "'struct<_a:bigint,b_:string,c_col:double>'";
    std::string output;
    std::string error;

    EXPECT_EQ(0, runProgram({pgm1, schema, csvFile, orcFile}, output, error));
    EXPECT_EQ("", error);

    // verify the ORC file content
    const std::string pgm2 = findProgram("tools/src/orc-contents");
    const std::string expected =
            "{\"_a\": 0, \"b_\": \"a\", \"c_col\": 0}\n"
            "{\"_a\": 1, \"b_\": \"b\", \"c_col\": 1.1}\n"
            "{\"_a\": 2, \"b_\": \"c\", \"c_col\": 2.2}\n"
            "{\"_a\": 3, \"b_\": \"d\", \"c_col\": null}\n"
            "{\"_a\": 4, \"b_\": null, \"c_col\": 4.4}\n"
            "{\"_a\": null, \"b_\": \"f\", \"c_col\": 5.5}\n"
            "{\"_a\": null, \"b_\": null, \"c_col\": null}\n"
            "{\"_a\": 7, \"b_\": \"h\", \"c_col\": 7.7}\n"
            "{\"_a\": 8, \"b_\": \"i\", \"c_col\": 8.8}\n"
            "{\"_a\": 9, \"b_\": \"j\", \"c_col\": 9.9}\n";
    EXPECT_EQ(0, runProgram({pgm2, orcFile}, output, error));
    EXPECT_EQ(expected, output);
    EXPECT_EQ("", error);
}
