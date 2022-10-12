// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <ostream>

#include "fs/fs.h"

namespace starrocks {

class WritableFileAsStreamBuf final : public std::streambuf {
    static_assert(sizeof(std::streambuf::char_type) == 1, "only support char");

public:
    explicit WritableFileAsStreamBuf(WritableFile* file) : _file(file) {}

    ~WritableFileAsStreamBuf() override = default;

protected:
    // put an element to stream
    int_type overflow(int_type ch) override {
        if (ch == std::streambuf::traits_type::eof()) {
            return std::streambuf::traits_type::not_eof(ch); // EOF, return success code
        }
        char_type c = std::streambuf::traits_type::to_char_type(ch);
        auto st = _file->append(Slice(&c, 1));
        return st.ok() ? ch : std::streambuf::traits_type ::eof();
    }

    // put |count| characters to stream
    std::streamsize xsputn(const char_type* s, std::streamsize count) override {
        auto st = _file->append(Slice(s, count));
        return st.ok() ? count : std::streambuf::traits_type::eof();
    }

    // synchronize buffer with external file
    int sync() override {
        // data are already in WritableFile.
        return 0;
    }

private:
    WritableFile* _file;
};

} // namespace starrocks
