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

#include <exception>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"
#include "orc/orc-config.hh"

class TestMemoryPool : public orc::MemoryPool {
private:
    std::map<char*, uint64_t> blocks;
    uint64_t totalMemory;
    uint64_t maxMemory;

public:
    char* malloc(uint64_t size) ORC_OVERRIDE {
        char* p = static_cast<char*>(std::malloc(size));
        blocks[p] = size;
        totalMemory += size;
        if (maxMemory < totalMemory) {
            maxMemory = totalMemory;
        }
        return p;
    }

    void free(char* p) ORC_OVERRIDE {
        std::free(p);
        totalMemory -= blocks[p];
        blocks.erase(p);
    }

    uint64_t getMaxMemory() { return maxMemory; }

    TestMemoryPool() : totalMemory(0), maxMemory(0) {}
    ~TestMemoryPool() ORC_OVERRIDE;
};

TestMemoryPool::~TestMemoryPool() {}

void processFile(const char* filename, const std::list<uint64_t>& cols, uint32_t batchSize) {
    orc::ReaderOptions readerOpts;
    orc::RowReaderOptions rowReaderOpts;
    if (cols.size() > 0) {
        rowReaderOpts.include(cols);
    }
    std::unique_ptr<orc::MemoryPool> pool(new TestMemoryPool());
    readerOpts.setMemoryPool(*(pool.get()));

    std::unique_ptr<orc::Reader> reader = orc::createReader(orc::readFile(std::string(filename)), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);

    std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(batchSize);
    uint64_t readerMemory = reader->getMemoryUseByFieldId(cols);
    uint64_t batchMemory = batch->getMemoryUsage();
    while (rowReader->next(*batch)) {
    }
    uint64_t actualMemory = static_cast<TestMemoryPool*>(pool.get())->getMaxMemory();
    std::cout << "Reader memory estimate: " << readerMemory << "\nBatch memory estimate:  ";
    if (batch->hasVariableLength()) {
        std::cout << "Cannot estimate because reading ARRAY or MAP columns";
    } else {
        std::cout << batchMemory << "\nTotal memory estimate:  " << readerMemory + batchMemory;
    }
    std::cout << "\nActual max memory used: " << actualMemory << "\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "Usage: orc-memory [--columns=column1,column2,...] "
                  << "[--batch=rows_in_batch] <filename> \n";
        return 1;
    }

    const std::string COLUMNS_PREFIX = "--columns=";
    const std::string BATCH_PREFIX = "--batch=";
    char* filename = ORC_NULLPTR;

    // Default parameters
    std::list<uint64_t> cols;
    uint32_t batchSize = 1000;

    // Read command-line options
    char *param, *value;
    for (int i = 1; i < argc; i++) {
        if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
            value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
            while (value) {
                cols.push_back(static_cast<uint64_t>(std::atoi(value)));
                value = std::strtok(ORC_NULLPTR, ",");
            }
        } else if ((param = strstr(argv[i], BATCH_PREFIX.c_str()))) {
            batchSize = static_cast<uint32_t>(std::atoi(param + BATCH_PREFIX.length()));
        } else {
            filename = argv[i];
        }
    }

    if (filename == ORC_NULLPTR) {
        std::cout << "Error: Filename not provided.\n";
        return 1;
    }

    try {
        processFile(filename, cols, batchSize);
        return 0;
    } catch (std::exception& ex) {
        std::cerr << "Caught exception: " << ex.what() << "\n";
        return 1;
    }
}
