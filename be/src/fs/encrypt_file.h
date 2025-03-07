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

#pragma once

#include "fs/fs.h"
#include "fs/writable_file_wrapper.h"
#include "io/input_stream.h"

namespace starrocks {

class EVPCipher;

bool openssl_supports_aesni();
std::string get_openssl_errors();

class EncryptWritableFile : public WritableFileWrapper {
public:
    EncryptWritableFile(WritableFile* file, Ownership ownership, FileEncryptionInfo encryption_info);

    ~EncryptWritableFile();

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t cnt) override;

private:
    FileEncryptionInfo _encryption_info;
};

std::unique_ptr<WritableFile> wrap_encrypted(std::unique_ptr<WritableFile> file,
                                             const FileEncryptionInfo& encryption_info);

class EncryptSeekableInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit EncryptSeekableInputStream(std::unique_ptr<SeekableInputStream> stream,
                                        FileEncryptionInfo encryption_info);

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status read_fully(void* data, int64_t count) override;

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count);

    Status read_at_fully(int64_t offset, void* out, int64_t count);

    StatusOr<std::string> read_all() override;

    bool is_encrypted() const override { return true; };

private:
    std::unique_ptr<io::SeekableInputStream> _stream;
    FileEncryptionInfo _encryption_info;
};

void ssl_random_bytes(void* buf, int size);

StatusOr<std::string> wrap_key(EncryptionAlgorithmPB algorithm, const std::string& plain_kek,
                               const std::string& plain_key);

StatusOr<std::string> unwrap_key(EncryptionAlgorithmPB algorithm, const std::string& plain_kek,
                                 const std::string& encrypted_key);

} // namespace starrocks
