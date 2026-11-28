// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <lance_rs_ffi.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

struct ArrowArray {
    int64_t length, null_count, offset, n_buffers, n_children;
    const void** buffers;
    ArrowArray** children;
    ArrowArray* dictionary;
    void (*release)(ArrowArray*);
    void* private_data;
};
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags, n_children;
    ArrowSchema** children;
    ArrowSchema* dictionary;
    void (*release)(ArrowSchema*);
    void* private_data;
};

int main(int argc, char** argv) {
    assert(argc == 2);
    SrLanceString uri{argv[1], std::strlen(argv[1])};
    SrLanceString id{"id", 2};
    for (uint64_t version : {uint64_t(1), uint64_t(999999)}) {
        SrLanceReader* reader = nullptr;
        char* error = nullptr;
        int result = sr_lance_reader_open(uri, version, &id, 1, 2, 0, nullptr, 0, {nullptr, nullptr}, &reader, &error);
        if (version == 999999) {
            assert(result == SR_LANCE_ERROR && reader == nullptr && error != nullptr);
            sr_lance_free_error(error);
            continue;
        }
        if (result != SR_LANCE_NEXT_BATCH) {
            std::cerr << (error ? error : "open failed") << '\n';
            return 1;
        }
        assert(sr_lance_reader_version(reader) == version);
        int64_t rows = 0, sum = 0;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        for (;;) {
            assert(std::chrono::steady_clock::now() < deadline);
            ArrowArray array{};
            ArrowSchema schema{};
            result = sr_lance_reader_next(reader, &array, &schema, &error);
            if (result == SR_LANCE_NEXT_PENDING) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            if (result == SR_LANCE_NEXT_EOF) break;
            assert(result == SR_LANCE_NEXT_BATCH && array.n_children == 1);
            assert(std::strcmp(schema.children[0]->format, "i") == 0);
            auto* values = static_cast<const int32_t*>(array.children[0]->buffers[1]);
            for (int64_t row = 0; row < array.length; ++row) sum += values[array.children[0]->offset + row];
            rows += array.length;
            array.release(&array);
            schema.release(&schema);
        }
        assert(rows == 7 && sum == 21);
        sr_lance_reader_close(reader);
    }
    std::cout << "Pinned snapshot and missing-version rejection passed through the published C ABI\n";
}
