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

#include <iostream>
#include <memory>
#include <string>

#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"
#include "orc/orc-config.hh"

void printContents(const char* filename, const orc::RowReaderOptions& rowReaderOpts) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<orc::RowReader> rowReader;
    reader = orc::createReader(orc::readFile(std::string(filename)), readerOpts);
    rowReader = reader->createRowReader(rowReaderOpts);

    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(1000);
    std::string line;
    std::unique_ptr<orc::ColumnPrinter> printer = createColumnPrinter(line, &rowReader->getSelectedType());

    while (rowReader->next(*batch)) {
        printer->reset(*batch);
        for (unsigned long i = 0; i < batch->numElements; ++i) {
            line.clear();
            printer->printRow(i);
            line += "\n";
            const char* str = line.c_str();
            fwrite(str, 1, strlen(str), stdout);
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: orc-contents <filename> [--columns=1,2,...]\n"
                  << "Print contents of <filename>.\n"
                  << "If columns are specified, only these top-level (logical) columns are "
                     "printed.\n";
        return 1;
    }
    try {
        const std::string COLUMNS_PREFIX = "--columns=";
        std::list<uint64_t> cols;
        char* filename = ORC_NULLPTR;

        // Read command-line options
        char *param, *value;
        for (int i = 1; i < argc; i++) {
            if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
                value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
                while (value) {
                    cols.push_back(static_cast<uint64_t>(std::atoi(value)));
                    value = std::strtok(ORC_NULLPTR, ",");
                }
            } else {
                filename = argv[i];
            }
        }
        orc::RowReaderOptions rowReaderOpts;
        if (cols.size() > 0) {
            rowReaderOpts.include(cols);
        }
        if (filename != ORC_NULLPTR) {
            printContents(filename, rowReaderOpts);
        }
    } catch (std::exception& ex) {
        std::cerr << "Caught exception: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
