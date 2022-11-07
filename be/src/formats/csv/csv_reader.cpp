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

#include "formats/csv/csv_reader.h"

namespace starrocks::vectorized {

using Field = Slice;

static Field trim(const char* value, int len) {
    int32_t begin = 0;

    while (begin < len && value[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && value[end] == ' ') {
        --end;
    }
    return Field(value + begin, end - begin + 1);
}

bool CSVReader::isDelimiter() {
    // 1. 如果是delimiter，那么一切很好
    // 2. 如果不是delimiter，那么要将指针回退
    // 3. 如果比较的时候buff长度不够，怎么处理
}

bool CSVReader::isNewline() {

}

bool CSVReader::isEscape() {

}

// 新的问题：
// 1. 比较最后一个字符往前回溯的方式看来行不通。那么如何处理多字节DELIMITER和NEWLINE
//  ordinary状态下需要
// 



// 这个函数我们要从状态START开始，读取下一条记录。
// 重载。
Status CSVReader::next_record(Fields* fields) {
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    fields->clear();
    // TODO: 每次读一条记录时先做一次compact，这里性能会有问题
    _buff.compact();
    ParseState curState = START;
    ParseState preState = curState;
    while (true) {
        if(_buff.available() < 1) {
            if (_buff.free_space() == 0) {
                RETURN_IF_ERROR(_expand_buffer());
            }
            RETURN_IF_ERROR(_fill_buffer());
        }
        char* filed_start = nullptr;
        // 每一次根据当前的状态+当前的字符，来推进到下一个状态
        switch (curState)
        {
        case START:
            // 我们记录字段开始的位置
            filed_start = _buff.position();
            // newline
            if (_row_delimiter_length == 1) {
                if (*(_buff.position()) == _row_delimiter[0]) {
                    curState = NEWLINE;
                    _buff.skip(1);
                    break;
                }
            } else {
                // 当行分隔符的长度大于1时，我们需要比较已经读到的字节和该分隔符是否一致。
                if (*(_buff.position()) == _row_delimiter.back() && _buff.have_read() >= _row_delimiter_length) {
                    bool equal = true;
                    char* pos = _buff.position();
                    for (auto it = _row_delimiter.rbegin(); it != _row_delimiter.rend(); ++it) {
                        if (*pos != *it) {
                            equal = false;
                            break;
                        }
                        pos--;
                    }
                    if (equal) {
                        curState = NEWLINE;
                        _buff.skip(1);
                        break;
                    }
                }
            }
            // delimiter
            if (_column_separator_length == 1) {
                if (*(_buff.position()) == _column_separator[0]) {
                    curState = DELIMITER;
                    _buff.skip(1);
                    break;
                }
            } else {
                if (*(_buff.position()) == _column_separator.back() && _buff.have_read() >= _column_separator_length) {
                    bool equal = true;
                    char* pos = _buff.position();
                    for (auto it = _column_separator.rbegin(); it != _column_separator.rend(); ++it) {
                        if (*pos != *it) {
                            equal = false;
                            break;
                        }
                        pos--;
                    }
                    if (equal) {
                        curState = DELIMITER;
                        _buff.skip(1);
                        break;
                    }
                }
            }
            // escape
            if (*(_buff.position()) == _escape) {
                curState = ESCAPE;
                _buff.skip(1);
                break;
            }
            // enclose
            if (*(_buff.position()) == _enclose) {
                curState = ENCLOSE;
                _buff.skip(1);
                break;
            }
            curState = ORDINARY;
            _buff.skip(1);
            break;

        case ENCLOSE:
            if (*(_buff.position()) == _enclose) {
                if (*(_buff.position() - 1) == _enclose) {
                    // TODO：两个连续ENCLOSE符号是转义状态，那么如何处理
                    _buff.skip(1);
                    break;
                } else {                    
                    curState = ORDINARY;
                    _buff.skip(1);
                    break;
                }
            }
            // escape
            if (*(_buff.position()) == _escape) {
                preState = curState;
                curState = ESCAPE;
                _buff.skip(1);
                break;
            }
            // other character
            _buff.skip(1);
            break;

        case ESCAPE:






        
        default:
            break;
        }
        // 每一次switch之后去判断是否要读新的数据
        // 这块确认好了，再往下演进：
        // 1. 如何读，何时读数据
        // 当available没有剩余数据时，那么读数据。
        // 每一次进行next record时，都做一次compact，当然这里会有性能开销，考虑如何优化
        // 2. 读新的数据的边界
        // 3. 这种方式处理多字节的delimiter和newline符号是否可行，或者对其他的状态流转是否可行
        // 4. 对于转义在某种情况下会破坏我们感兴趣内存的连续性，如何处理？
        // 到后面再做转义。需要多做一次扫描。这个方案不行，因为还有enclose转义。后面再考虑这个问题。
        // 4.5 循环何时退出
        // a. 拿到了一行完整记录. b. 超过缓冲区大小，但是还没有找到相关记录
        // 5. 如何处理空行
        // 6. 如何处理end of file

    }

}

Status CSVReader::next_record(Record* record) {
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    char* d;
    size_t pos = 0;
    while ((d = _buff.find(_row_delimiter, pos)) == nullptr) {
        pos = _buff.available();
        _buff.compact();
        if (_buff.free_space() == 0) {
            RETURN_IF_ERROR(_expand_buffer());
        }
        //  fill_buffer 进行了一次内存拷贝：将数据从文件中读取，写入到_buff中
        RETURN_IF_ERROR(_fill_buffer());
    }
    size_t l = d - _buff.position();
    *record = Record(_buff.position(), l);
    _buff.skip(l + _row_delimiter_length);
    //               ^^ skip record delimiter.
    _parsed_bytes += l + _row_delimiter_length;
    return Status::OK();
}

// 将底层的storage(vector)扩大
Status CSVReader::_expand_buffer() {
    if (UNLIKELY(_storage.size() >= kMaxBufferSize)) {
        return Status::InternalError("CSV line length exceed limit " + std::to_string(kMaxBufferSize));
    }
    size_t new_capacity = std::min(_storage.size() * 2, kMaxBufferSize);
    DCHECK_EQ(_storage.data(), _buff.position()) << "should compact buffer before expand";
    _storage.resize(new_capacity);
    CSVBuffer new_buff(_storage.data(), _storage.size());
    new_buff.add_limit(_buff.available());
    DCHECK_EQ(_storage.data(), new_buff.position());
    DCHECK_EQ(_buff.available(), new_buff.available());
    _buff = new_buff;
    return Status::OK();
}

void CSVReader::split_record(const Record& record, Fields* fields) const {
    const char* value = record.data;
    const char* ptr = record.data;
    const size_t size = record.size;

    if (_column_separator_length == 1) {
        for (size_t i = 0; i < size; ++i, ++ptr) {
            if (*ptr == _column_separator[0]) {
                if (_trim_space) {
                    fields->emplace_back(trim(value, ptr - value));
                } else {
                    fields->emplace_back(value, ptr - value);
                }
                value = ptr + 1;
            }
        }
    } else {
        const auto* const base = ptr;

        do {
            ptr = static_cast<char*>(
                    memmem(value, size - (value - base), _column_separator.data(), _column_separator_length));
            if (ptr != nullptr) {
                if (_trim_space) {
                    fields->emplace_back(trim(value, ptr - value));
                } else {
                    fields->emplace_back(value, ptr - value);
                }
                value = ptr + _column_separator_length;
            }
        } while (ptr != nullptr);

        ptr = record.data + size;
    }
    if (_trim_space) {
        fields->emplace_back(trim(value, ptr - value));
    } else {
        fields->emplace_back(value, ptr - value);
    }
}

} // namespace starrocks::vectorized
