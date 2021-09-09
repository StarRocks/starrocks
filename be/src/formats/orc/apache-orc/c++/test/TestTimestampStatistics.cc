// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/test/TestTimestampStatistics.cc

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
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

TEST(TestTimestampStatistics, testOldFile) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
        ss << example_dir;
    } else {
        ss << "../../../examples";
    }
    ss << "/TestOrcFile.testTimestamp.orc";
    std::unique_ptr<orc::Reader> reader = createReader(readLocalFile(ss.str().c_str()), ReaderOptions());

    std::unique_ptr<orc::ColumnStatistics> footerStats = reader->getColumnStatistics(0);
    const orc::TimestampColumnStatistics* footerColStats =
            reinterpret_cast<const orc::TimestampColumnStatistics*>(footerStats.get());

    std::unique_ptr<orc::StripeStatistics> stripeStats = reader->getStripeStatistics(0);
    const orc::TimestampColumnStatistics* stripeColStats =
            reinterpret_cast<const orc::TimestampColumnStatistics*>(stripeStats->getColumnStatistics(0));

    EXPECT_FALSE(footerColStats->hasMinimum());
    EXPECT_FALSE(footerColStats->hasMaximum());
    EXPECT_TRUE(footerColStats->hasLowerBound());
    EXPECT_TRUE(footerColStats->hasUpperBound());
    EXPECT_EQ(
            "Data type: Timestamp\nValues: 12\nHas null: no\nMinimum is not defined\nLowerBound: "
            "1994-12-31 07:00:00.688\nMaximum is not defined\nUpperBound: 2037-01-02 09:00:00.1\n",
            footerColStats->toString());

    EXPECT_TRUE(stripeColStats->hasMinimum());
    EXPECT_TRUE(stripeColStats->hasMaximum());
    EXPECT_EQ(
            "Data type: Timestamp\nValues: 12\nHas null: no\nMinimum: 1995-01-01 "
            "00:00:00.688\nLowerBound: 1995-01-01 00:00:00.688\nMaximum: 2037-01-01 "
            "00:00:00.0\nUpperBound: 2037-01-01 00:00:00.1\n",
            stripeColStats->toString());
}

} // namespace orc
