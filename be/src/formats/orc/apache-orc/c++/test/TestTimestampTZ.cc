// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/test/TestTimestampTZ.cc

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
#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

static void readTimestampWithTimezone(std::unique_ptr<InputStream> inputStream, const std::string& readerTimezone,
                                      int64_t* second, int64_t* ns, bool useWriterTimezone = false) {
    ReaderOptions ro;
    std::unique_ptr<Reader> reader = createReader(std::move(inputStream), ro);
    RowReaderOptions rro;
    if (!readerTimezone.empty()) {
        rro.setTimezoneName(readerTimezone);
    }
    if (useWriterTimezone) {
        rro.useWriterTimezone();
    }
    std::unique_ptr<RowReader> rowReader = reader->createRowReader(rro);
    auto cvb = rowReader->createRowBatch(1);
    rowReader->next(*cvb);

    auto struct_cvb = dynamic_cast<StructVectorBatch*>(cvb.get());
    auto ts_cvb = dynamic_cast<TimestampVectorBatch*>(struct_cvb->fields[0]);
    *second = ts_cvb->data[0];
    *ns = ts_cvb->nanoseconds[0];
    return;
}

static void writeTimestampWithTimezone(OutputStream* outputStream, const std::string& writerTimezone, int64_t second,
                                       int64_t ns) {
    ORC_UNIQUE_PTR<Type> type(Type::buildTypeFromString("struct<col1:timestamp>"));
    WriterOptions wo;
    wo.setTimezoneName(writerTimezone);
    auto writer = createWriter(*(type.get()), outputStream, wo);
    auto cvb = writer->createRowBatch(1);
    cvb->numElements = 1;
    auto struct_cvb = dynamic_cast<StructVectorBatch*>(cvb.get());
    auto ts_cvb = dynamic_cast<TimestampVectorBatch*>(struct_cvb->fields[0]);
    ts_cvb->data[0] = second;
    ts_cvb->nanoseconds[0] = ns;
    writer->add(*cvb);
    writer->close();
}

TEST(TestTimestampTZ, testTimestampTZConversion) {
    // MemoryOutputStream memStream(1024*1024);
    const std::string outputFile = "/tmp/test.orc";
    auto os = writeLocalFile(outputFile);
    int64_t second = 1622390017;
    int64_t ns = 123456;
    writeTimestampWithTimezone(os.get(), "Asia/Shanghai", second, ns);

    auto inputStream = readLocalFile(outputFile);
    int64_t output_second = 0;
    int64_t output_ns = 0;
    readTimestampWithTimezone(std::move(inputStream), "Asia/Shanghai", &output_second, &output_ns);
    EXPECT_EQ(output_second, second);
    EXPECT_EQ(output_ns, ns);

    inputStream = readLocalFile(outputFile);
    readTimestampWithTimezone(std::move(inputStream), "UTC", &output_second, &output_ns, true);
    std::cout << "read in writer timezone. sec = " << output_second << std::endl;
    EXPECT_EQ(output_second, second);
    EXPECT_EQ(output_ns, ns);

    // read as UTC.
    inputStream = readLocalFile(outputFile);
    readTimestampWithTimezone(std::move(inputStream), "UTC", &output_second, &output_ns);
    std::cout << "read in utc. sec = " << output_second << std::endl;
    EXPECT_NE(output_second, second);
    EXPECT_EQ(output_second - 28800, second);
    EXPECT_EQ(output_ns, ns);

    // read as UTC.
    inputStream = readLocalFile(outputFile);
    readTimestampWithTimezone(std::move(inputStream), "", &output_second, &output_ns);
    std::cout << "read in null. sec = " << output_second << std::endl;
    EXPECT_NE(output_second, second);
    EXPECT_EQ(output_second - 28800, second);
    EXPECT_EQ(output_ns, ns);

    inputStream = readLocalFile(outputFile);
    readTimestampWithTimezone(std::move(inputStream), "America/Los_Angeles", &output_second, &output_ns);
    std::cout << "read in LA. sec = " << output_second << std::endl;
    EXPECT_NE(output_second, second);
    EXPECT_EQ(output_ns, ns);
}

TEST(TestTimestampTZ, testReaderWriterAtShanghai) {
    struct tm tmValue;
    char timeBuffer[256];
    std::string ts;

    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
        ss << example_dir;
    } else {
        ss << "../../../examples";
    }
    // writer timezone Asia/Shanghai
    // second value = 1622283640
    // which is 2021-05-29 10:20:40 UTC
    // so I guess orc stores two part
    // datetime literal(in UTC) as seconds
    // and timezone info

    // {"c0":"2021-05-29 18:20:40.861"}
    // 2021-05-29 18:20:40.861 +GMT0
    // 2021-05-29 10:20:40.861 +GMT8
    ss << "/writer_at_shanghai.orc";
    auto inputStream = readLocalFile(ss.str());
    int64_t output_second = 0;
    int64_t output_ns = 0;
    readTimestampWithTimezone(std::move(inputStream), "Asia/Shanghai", &output_second, &output_ns);
    EXPECT_EQ(output_second, 1622283640);
    gmtime_r(&output_second, &tmValue);
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
    ts = timeBuffer;
    EXPECT_EQ(ts, "2021-05-29 10:20:40");
    EXPECT_EQ(output_ns, 861 * 1000000L);

    inputStream = readLocalFile(ss.str());
    readTimestampWithTimezone(std::move(inputStream), "UTC", &output_second, &output_ns);
    EXPECT_EQ(output_second - 28800, 1622283640);
    gmtime_r(&output_second, &tmValue);
    strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
    ts = timeBuffer;
    EXPECT_EQ(ts, "2021-05-29 18:20:40");
    EXPECT_EQ(output_ns, 861 * 1000000L);
}

} // namespace orc
