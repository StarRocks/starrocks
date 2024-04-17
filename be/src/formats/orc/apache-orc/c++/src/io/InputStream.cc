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
//   https://github.com/apache/orc/tree/main/c++/src/io/InputStream.cc

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

#include "InputStream.hh"

#include <algorithm>
#include <iomanip>

#include "orc/Exceptions.hh"

namespace orc {

void printBuffer(std::ostream& out, const char* buffer, uint64_t length) {
    const uint64_t width = 24;
    out << std::hex;
    for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
        out << std::setfill('0') << std::setw(7) << (line * width);
        for (uint64_t byte = 0; byte < width && line * width + byte < length; ++byte) {
            out << " " << std::setfill('0') << std::setw(2)
                << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
        }
        out << "\n";
    }
    out << std::dec;
}

PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
    position = posns.begin();
}

uint64_t PositionProvider::next() {
    uint64_t result = *position;
    ++position;
    return result;
}

uint64_t PositionProvider::current() {
    return *position;
}

PositionProvider& PositionProviderMap::at(uint64_t columnId) {
    return providers.at(columnId);
}

SeekableInputStream::~SeekableInputStream() {
    // PASS
}

SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
}

SeekableArrayInputStream::SeekableArrayInputStream(const unsigned char* values, uint64_t size, uint64_t blkSize)
        : data(reinterpret_cast<const char*>(values)) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

SeekableArrayInputStream::SeekableArrayInputStream(const char* values, uint64_t size, uint64_t blkSize) : data(values) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
    uint64_t currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
        *buffer = data + position;
        *size = static_cast<int>(currentSize);
        position += currentSize;
        return true;
    }
    *size = 0;
    return false;
}

void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
        auto unsignedCount = static_cast<uint64_t>(count);
        if (unsignedCount <= blockSize && unsignedCount <= position) {
            position -= unsignedCount;
        } else {
            throw std::logic_error("Can't backup that much!");
        }
    }
}

bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
        auto unsignedCount = static_cast<uint64_t>(count);
        if (unsignedCount + position <= length) {
            position += unsignedCount;
            return true;
        } else {
            position = length;
        }
    }
    return false;
}

google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
}

void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
}

std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position << " of " << length;
    return result.str();
}

static const uint64_t DEFAULT_FILE_BLOCK_SIZE = 256 * 1024;
static uint64_t computeBlock(uint64_t request, uint64_t length) {
    return std::min(length, request == 0 ? DEFAULT_FILE_BLOCK_SIZE : request);
}

SeekableFileInputStream::SeekableFileInputStream(InputStream* stream, uint64_t offset, uint64_t byteCount,
                                                 MemoryPool& _pool, uint64_t _blockSize)
        : pool(_pool), input(stream), start(offset), length(byteCount), blockSize(computeBlock(_blockSize, length)) {
    position = 0;
    buffer = nullptr;
    pushBack = 0;
    hasSeek = false;
}

SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
}

bool SeekableFileInputStream::Next(const void** data, int* size) {
    if (buffer == nullptr) {
        buffer.reset(new DataBuffer<char>(pool, blockSize));
    }
    uint64_t bytesRead;
    if (pushBack != 0) {
        *data = buffer->data() + (buffer->size() - pushBack);
        bytesRead = pushBack;
    } else {
        bytesRead = std::min(length - position, blockSize);
        if (hasSeek) {
            hasSeek = false;
            bytesRead = std::min(bytesRead, input->getNaturalReadSizeAfterSeek());
        }
        if (bytesRead > 0) {
            input->read(buffer->data(), bytesRead, start + position);
            *data = static_cast<void*>(buffer->data());
        }
    }
    position += bytesRead;
    pushBack = 0;
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
}

void SeekableFileInputStream::BackUp(int signedCount) {
    if (signedCount < 0) {
        throw std::logic_error("can't backup negative distances");
    }
    auto count = static_cast<uint64_t>(signedCount);
    if (pushBack > 0) {
        throw std::logic_error("can't backup unless we just called Next");
    }
    if (count > blockSize || count > position) {
        throw std::logic_error("can't backup that far");
    }
    pushBack = static_cast<uint64_t>(count);
    position -= pushBack;
}

bool SeekableFileInputStream::Skip(int signedCount) {
    if (signedCount < 0) {
        return false;
    }
    auto count = static_cast<uint64_t>(signedCount);
    position = std::min(position + count, length);
    pushBack = 0;
    return position < length;
}

int64_t SeekableFileInputStream::ByteCount() const {
    return static_cast<int64_t>(position);
}

void SeekableFileInputStream::seek(PositionProvider& location) {
    position = location.next();
    if (position > length) {
        position = length;
        throw std::logic_error("seek too far");
    }
    pushBack = 0;
    hasSeek = true;
}

std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input->getName() << " from " << start << " for " << length;
    return result.str();
}

  DecryptionInputStream::DecryptionInputStream(std::unique_ptr<SeekableInputStream> input,
                                               std::vector<unsigned char> key,
                                               std::vector<unsigned char> iv,
                                               const EVP_CIPHER* cipher,MemoryPool& pool)
      : input_(std::move(input)),
        key_(key),
        iv_(iv),
        cipher(cipher),
        pool(pool){
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == nullptr) {
      throw std::runtime_error("Failed to create EVP cipher context");
    }
    int ret = EVP_DecryptInit_ex(ctx, cipher, NULL, key_.data(), iv_.data());
    if (ret != 1) {
      EVP_CIPHER_CTX_free(ctx);
      //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("Failed to initialize EVP cipher context");
    }
    ctx_ = ctx;
    outputBuffer_.reset(new DataBuffer<unsigned char>(pool));
    inputBuffer_.reset(new DataBuffer<unsigned char>(pool));
  }

  DecryptionInputStream::~DecryptionInputStream() {
    EVP_CIPHER_CTX_free(ctx_);
    //EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
  }

  bool DecryptionInputStream::Next(const void** data, int* size) {
    int bytesRead = 0;
    //const void* ptr;
    const void* inptr = static_cast<void*>(inputBuffer_->data());
    input_->Next(&inptr, &bytesRead);
    if (bytesRead == 0) {
      return false;
    }
    // decrypt data
    const unsigned char* result = static_cast<const unsigned char*>(inptr);
    int outlen = 0;
    //int blockSize = EVP_CIPHER_block_size(this->cipher);
    outputBuffer_->resize(bytesRead);
    int ret = EVP_DecryptUpdate(ctx_, outputBuffer_->data(), &outlen, result, bytesRead);
    if (ret != 1) {
      throw std::runtime_error("Failed to decrypt data");
    }
    outputBuffer_->resize(outlen);
    *data = outputBuffer_->data();
    *size = outputBuffer_->size();
    return true;
  }
  void DecryptionInputStream::BackUp(int count) {
    this->input_->BackUp(count);
  }

  bool DecryptionInputStream::Skip(int count) {
    return this->input_->Skip(count);
  }

  google::protobuf::int64 DecryptionInputStream::ByteCount() const {
    return input_->ByteCount();
  }

  void DecryptionInputStream::seek(PositionProvider& position) {
    //std::cout<<"PPP:DecryptionInputStream::seek:"<<position.current()<<std::endl;
    changeIv(position.current());
    input_->seek(position);
  }
  void DecryptionInputStream::changeIv(long offset) {
    int blockSize = EVP_CIPHER_key_length(cipher);
    long encryptionBlocks = offset / blockSize;
    long extra = offset % blockSize;
    std::fill(iv_.end() - 8, iv_.end(), 0);
    if (encryptionBlocks != 0) {
      // Add the encryption blocks into the initial iv, to compensate for
      // skipping over decrypting those bytes.
      int posn = iv_.size() - 1;
      while (encryptionBlocks > 0) {
        long sum = (iv_[posn] & 0xff) + encryptionBlocks;
        iv_[posn--] = (unsigned char) sum;
        encryptionBlocks = sum / 0x100;
      }
    }
    EVP_DecryptInit_ex(ctx_, cipher, NULL, key_.data(), iv_.data());
    // If the range starts at an offset that doesn't match the encryption
    // block, we need to advance some bytes within an encryption block.
    if (extra > 0) {
      std::vector<unsigned char> decrypted(extra);
      int decrypted_len;
      EVP_DecryptUpdate(ctx_, decrypted.data(), &decrypted_len, decrypted.data(), extra);
    }
  }
  std::string DecryptionInputStream::getName() const {
    return "DecryptionInputStream("+input_->getName()+")";
  }
}  // namespace orc
