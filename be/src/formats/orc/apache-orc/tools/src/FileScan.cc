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

#include <getopt.h>

#include <iostream>
#include <memory>
#include <string>

#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"

void scanFile(std::ostream& out, const char* filename, uint64_t batchSize) {
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readFile(filename), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(batchSize);

    unsigned long rows = 0;
    unsigned long batches = 0;
    while (rowReader->next(*batch)) {
        batches += 1;
        rows += batch->numElements;
    }
    out << "Rows: " << rows << std::endl;
    out << "Batches: " << batches << std::endl;
}

int main(int argc, char* argv[]) {
    static struct option longOptions[] = {{"help", no_argument, ORC_NULLPTR, 'h'},
                                          {"batch", required_argument, ORC_NULLPTR, 'b'},
                                          {ORC_NULLPTR, 0, ORC_NULLPTR, 0}};
    bool helpFlag = false;
    uint64_t batchSize = 1024;
    int opt;
    char* tail;
    do {
        opt = getopt_long(argc, argv, "hb:", longOptions, ORC_NULLPTR);
        switch (opt) {
        case '?':
        case 'h':
            helpFlag = true;
            opt = -1;
            break;
        case 'b':
            batchSize = strtoul(optarg, &tail, 10);
            if (*tail != '\0') {
                fprintf(stderr, "The --batch parameter requires an integer option.\n");
                return 1;
            }
            break;
        }
    } while (opt != -1);
    argc -= optind;
    argv += optind;

    if (argc < 1 || helpFlag) {
        std::cerr << "Usage: orc-scan [-h] [--help]\n"
                  << "                [-b<size>] [--batch=<size>] <filename>\n";
        return 1;
    } else {
        for (int i = 0; i < argc; ++i) {
            try {
                scanFile(std::cout, argv[i], batchSize);
            } catch (std::exception& ex) {
                std::cerr << "Caught exception in " << argv[i] << ": " << ex.what() << "\n";
                return 1;
            }
        }
    }
    return 0;
}
