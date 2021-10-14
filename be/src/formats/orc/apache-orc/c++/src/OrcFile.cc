// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/OrcFile.cc

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

#include "orc/OrcFile.hh"

#include <fcntl.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "Adaptor.hh"
#include "orc/Exceptions.hh"

#ifdef _MSC_VER
#include <io.h>
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define stat _stat64
#define fstat _fstat64
#else
#include <unistd.h>

#include <utility>
#define O_BINARY 0
#endif

namespace orc {

class FileInputStream : public InputStream {
private:
    std::string filename;
    int file;
    uint64_t totalLength;

public:
    FileInputStream(std::string _filename) {
        filename = std::move(_filename);
        file = open(filename.c_str(), O_BINARY | O_RDONLY);
        if (file == -1) {
            throw ParseError("Can't open " + filename);
        }
        struct stat fileStat;
        if (fstat(file, &fileStat) == -1) {
            throw ParseError("Can't stat " + filename);
        }
        totalLength = static_cast<uint64_t>(fileStat.st_size);
    }

    ~FileInputStream() override;

    uint64_t getLength() const override { return totalLength; }

    uint64_t getNaturalReadSize() const override { return 128 * 1024; }

    void read(void* buf, uint64_t length, uint64_t offset) override {
        if (!buf) {
            throw ParseError("Buffer is null");
        }
        ssize_t bytesRead = pread(file, buf, length, static_cast<off_t>(offset));

        if (bytesRead == -1) {
            throw ParseError("Bad read of " + filename);
        }
        if (static_cast<uint64_t>(bytesRead) != length) {
            throw ParseError("Short read of " + filename);
        }
    }

    const std::string& getName() const override { return filename; }
};

FileInputStream::~FileInputStream() {
    close(file);
}

std::unique_ptr<InputStream> readFile(const std::string& path) {
#ifdef BUILD_LIBHDFSPP
    if (strncmp(path.c_str(), "hdfs://", 7) == 0) {
        return orc::readHdfsFile(std::string(path));
    } else {
#endif
        return orc::readLocalFile(std::string(path));
#ifdef BUILD_LIBHDFSPP
    }
#endif
}

std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
}

OutputStream::~OutputStream(){
        // PASS
};

class FileOutputStream : public OutputStream {
private:
    std::string filename;
    int file;
    uint64_t bytesWritten;
    bool closed;

public:
    FileOutputStream(std::string _filename) {
        bytesWritten = 0;
        filename = std::move(_filename);
        closed = false;
        file = open(filename.c_str(), O_BINARY | O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);
        if (file == -1) {
            throw ParseError("Can't open " + filename);
        }
    }

    ~FileOutputStream() override;

    uint64_t getLength() const override { return bytesWritten; }

    uint64_t getNaturalWriteSize() const override { return 128 * 1024; }

    void write(const void* buf, size_t length) override {
        if (closed) {
            throw std::logic_error("Cannot write to closed stream.");
        }
        ssize_t bytesWrite = ::write(file, buf, length);
        if (bytesWrite == -1) {
            throw ParseError("Bad write of " + filename);
        }
        if (static_cast<uint64_t>(bytesWrite) != length) {
            throw ParseError("Short write of " + filename);
        }
        bytesWritten += static_cast<uint64_t>(bytesWrite);
    }

    const std::string& getName() const override { return filename; }

    void close() override {
        if (!closed) {
            ::close(file);
            closed = true;
        }
    }
};

FileOutputStream::~FileOutputStream() {
    if (!closed) {
        ::close(file);
        closed = true;
    }
}

std::unique_ptr<OutputStream> writeLocalFile(const std::string& path) {
    return std::unique_ptr<OutputStream>(new FileOutputStream(path));
}
} // namespace orc
