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

#include "storage/sstable/key_dict.h"

#include <algorithm>
#include <cstring>
#include <string_view>

#include "storage/sstable/coding.h"
#include "storage/sstable/iterator.h"

namespace starrocks::sstable {

namespace {

constexpr uint8_t kKeyDictFormatVersion = 1;

// Length of the column segment that starts at |p|, with |n| bytes left in the key. When the key ends inside the
// segment, |*complete| is set to false and the rest of the key is returned.
size_t segment_length(const char* p, size_t n, const KeyColumnLayout& column, bool is_last, bool* complete) {
    if (column.fixed_width > 0) {
        *complete = n >= column.fixed_width;
        return *complete ? column.fixed_width : n;
    }
    if (is_last) {
        *complete = true;
        return n;
    }
    // A VARCHAR in the middle ends with 0x00 0x00; a 0x00 inside it is escaped as 0x00 0x01.
    size_t i = 0;
    while (i < n) {
        const void* zero = memchr(p + i, 0, n - i);
        if (zero == nullptr) {
            break;
        }
        const size_t pos = static_cast<const char*>(zero) - p;
        if (pos + 1 >= n) {
            break;
        }
        if (p[pos + 1] == 0) {
            *complete = true;
            return pos + 2;
        }
        i = pos + 2;
    }
    *complete = false;
    return n;
}

void put_code(std::string* dst, uint32_t code, uint32_t width) {
    if (width == 1) {
        dst->push_back(static_cast<char>(code));
    } else {
        dst->push_back(static_cast<char>(code >> 8));
        dst->push_back(static_cast<char>(code & 0xff));
    }
}

bool same_layout(const KeyLayout& a, const KeyLayout& b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](const auto& x, const auto& y) {
               return x.fixed_width == y.fixed_width;
           });
}

} // namespace

StatusOr<std::shared_ptr<const KeyDict>> KeyDict::create(KeyLayout layout, std::vector<ColumnDict> columns) {
    std::shared_ptr<KeyDict> dict(new KeyDict());
    dict->_layout = std::move(layout);
    dict->_columns = std::move(columns);
    RETURN_IF_ERROR(dict->_init());
    return std::shared_ptr<const KeyDict>(std::move(dict));
}

Status KeyDict::_init() {
    if (_columns.empty()) {
        return Status::InvalidArgument("key dictionary without any column");
    }
    _dict_of_column.assign(_layout.size(), -1);
    _memory_usage = sizeof(KeyDict) + _layout.size() * (sizeof(KeyColumnLayout) + sizeof(int32_t));
    for (size_t i = 0; i < _columns.size(); ++i) {
        const auto& column = _columns[i];
        if (column.column >= _layout.size() || (i > 0 && column.column <= _columns[i - 1].column)) {
            return Status::Corruption("bad key dictionary column");
        }
        if (column.values.empty() || column.values.size() > kMaxCardinality) {
            return Status::Corruption("bad key dictionary size");
        }
        const bool is_last = column.column + 1 == _layout.size();
        for (size_t j = 0; j < column.values.size(); ++j) {
            const auto& value = column.values[j];
            bool complete = false;
            if (value.empty() ||
                segment_length(value.data(), value.size(), _layout[column.column], is_last, &complete) !=
                        value.size() ||
                !complete) {
                return Status::Corruption("bad key dictionary value");
            }
            if (j > 0 && Slice(column.values[j - 1]).compare(Slice(value)) >= 0) {
                return Status::Corruption("key dictionary values are not sorted");
            }
            _memory_usage += sizeof(std::string) + value.capacity();
        }
        _dict_of_column[column.column] = static_cast<int32_t>(i);
        _parsed_columns = column.column + 1;
    }
    _memory_usage += _columns.size() * sizeof(ColumnDict);
    return Status::OK();
}

void KeyDict::serialize(std::string* dst) const {
    dst->push_back(static_cast<char>(kKeyDictFormatVersion));
    PutVarint32(dst, _layout.size());
    for (const auto& column : _layout) {
        PutVarint32(dst, column.fixed_width);
    }
    PutVarint32(dst, _columns.size());
    for (const auto& column : _columns) {
        PutVarint32(dst, column.column);
        PutVarint32(dst, column.values.size());
        for (const auto& value : column.values) {
            PutLengthPrefixedSlice(dst, Slice(value));
        }
    }
}

StatusOr<std::shared_ptr<const KeyDict>> KeyDict::deserialize(const Slice& data) {
    Slice input = data;
    if (input.size < 1 || static_cast<uint8_t>(input.data[0]) != kKeyDictFormatVersion) {
        return Status::Corruption("unknown key dictionary format");
    }
    input.remove_prefix(1);
    uint32_t num_layout = 0;
    if (!GetVarint32(&input, &num_layout) || num_layout > input.size) {
        return Status::Corruption("bad key dictionary layout");
    }
    KeyLayout layout(num_layout);
    for (auto& column : layout) {
        if (!GetVarint32(&input, &column.fixed_width)) {
            return Status::Corruption("bad key dictionary layout");
        }
    }
    uint32_t num_columns = 0;
    if (!GetVarint32(&input, &num_columns) || num_columns > num_layout) {
        return Status::Corruption("bad key dictionary columns");
    }
    std::vector<ColumnDict> columns(num_columns);
    for (auto& column : columns) {
        uint32_t num_values = 0;
        if (!GetVarint32(&input, &column.column) || !GetVarint32(&input, &num_values) || num_values > kMaxCardinality) {
            return Status::Corruption("bad key dictionary column");
        }
        column.values.reserve(num_values);
        for (uint32_t i = 0; i < num_values; ++i) {
            Slice value;
            if (!GetLengthPrefixedSlice(&input, &value)) {
                return Status::Corruption("bad key dictionary value");
            }
            column.values.emplace_back(value.data, value.size);
        }
    }
    if (input.size != 0) {
        return Status::Corruption("trailing bytes after key dictionary");
    }
    return create(std::move(layout), std::move(columns));
}

void KeyDict::encode(const Slice& key, std::string* dst) const {
    dst->clear();
    const char* p = key.data;
    size_t n = key.size;
    for (size_t c = 0; c < _parsed_columns && n > 0; ++c) {
        bool complete = false;
        const size_t len = segment_length(p, n, _layout[c], c + 1 == _layout.size(), &complete);
        const int32_t d = _dict_of_column[c];
        if (d >= 0) {
            const auto& values = _columns[d].values;
            const uint32_t width = code_width(values.size());
            const Slice segment(p, len);
            auto it = std::lower_bound(values.begin(), values.end(), segment,
                                       [](const std::string& a, const Slice& b) { return Slice(a).compare(b) < 0; });
            const auto index = static_cast<uint32_t>(it - values.begin());
            if (complete && it != values.end() && Slice(*it) == segment) {
                put_code(dst, 2 * index + 1, width);
            } else {
                // Not in the dictionary, or cut short: escape, then the raw bytes. All the values between
                // values[index - 1] and values[index] share this code and are ordered by their raw bytes.
                put_code(dst, 2 * index, width);
                dst->append(p, len);
            }
        } else {
            dst->append(p, len);
        }
        p += len;
        n -= len;
        if (!complete) {
            return;
        }
    }
    dst->append(p, n);
}

Status KeyDict::decode(const Slice& stored, std::string* dst) const {
    dst->clear();
    const char* p = stored.data;
    size_t n = stored.size;
    for (size_t c = 0; c < _parsed_columns && n > 0; ++c) {
        const int32_t d = _dict_of_column[c];
        if (d >= 0) {
            const auto& values = _columns[d].values;
            const uint32_t width = code_width(values.size());
            if (n < width) {
                return Status::Corruption("bad dictionary encoded key: truncated code");
            }
            const uint32_t code =
                    width == 1 ? static_cast<uint8_t>(p[0])
                               : (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 8) | static_cast<uint8_t>(p[1]);
            p += width;
            n -= width;
            if ((code & 1) != 0) {
                if ((code >> 1) >= values.size()) {
                    return Status::Corruption("bad dictionary encoded key: code out of range");
                }
                dst->append(values[code >> 1]);
                continue;
            }
            if ((code >> 1) > values.size()) {
                return Status::Corruption("bad dictionary encoded key: escape out of range");
            }
            // An escape: the raw bytes of the column follow.
        }
        bool complete = false;
        const size_t len = segment_length(p, n, _layout[c], c + 1 == _layout.size(), &complete);
        dst->append(p, len);
        p += len;
        n -= len;
        if (!complete) {
            return Status::OK();
        }
    }
    dst->append(p, n);
    return Status::OK();
}

KeyDictCollector::KeyDictCollector(KeyLayout layout, uint32_t max_cardinality)
        : _layout(std::move(layout)),
          _max_cardinality(std::min(max_cardinality, KeyDict::kMaxCardinality)),
          _states(_layout.size()) {
    for (size_t c = 1; c < _layout.size(); ++c) {
        if (_layout[c].fixed_width == 1 || _max_cardinality == 0) {
            continue;
        }
        _states[c].candidate = true;
        _parsed_columns = c + 1;
    }
}

void KeyDictCollector::_insert(ColumnState* state, const Slice& value) {
    const std::string_view view(value.data, value.size);
    if (state->values.find(view) != state->values.end()) {
        return;
    }
    if (state->values.size() >= _max_cardinality) {
        state->overflow = true;
        state->values.clear();
        state->last.clear();
        return;
    }
    state->values.emplace(view);
}

void KeyDictCollector::add_seed(const KeyDict& dict) {
    if (!same_layout(dict.layout(), _layout)) {
        return;
    }
    for (const auto& column : dict.columns()) {
        auto& state = _states[column.column];
        if (!state.candidate || state.overflow) {
            continue;
        }
        state.seeded = true;
        for (const auto& value : column.values) {
            _insert(&state, Slice(value));
            if (state.overflow) {
                break;
            }
        }
    }
}

void KeyDictCollector::add(const Slice& key) {
    const char* p = key.data;
    size_t n = key.size;
    for (size_t c = 0; c < _parsed_columns && n > 0; ++c) {
        bool complete = false;
        const size_t len = segment_length(p, n, _layout[c], c + 1 == _layout.size(), &complete);
        if (!complete) {
            return;
        }
        auto& state = _states[c];
        if (state.candidate && !state.overflow) {
            const Slice segment(p, len);
            state.bytes += len;
            if (++state.count > 1 && !state.decreased && segment.compare(Slice(state.last)) < 0) {
                state.decreased = true;
            }
            if (!state.decreased) {
                state.last.assign(p, len);
            }
            _insert(&state, segment);
        }
        p += len;
        n -= len;
    }
}

bool KeyDictCollector::exhausted() const {
    return std::none_of(_states.begin(), _states.end(), [](const auto& s) { return s.candidate && !s.overflow; });
}

StatusOr<std::shared_ptr<const KeyDict>> KeyDictCollector::finish(bool complete) {
    std::vector<KeyDict::ColumnDict> columns;
    for (size_t c = 0; c < _states.size(); ++c) {
        auto& state = _states[c];
        if (!state.candidate || state.overflow || state.values.empty() || state.count == 0) {
            continue;
        }
        if (!complete && !state.seeded && !state.decreased) {
            continue;
        }
        // The codes must save at least a byte per key, and, when every key is known, more than the dictionary costs.
        const uint64_t width = KeyDict::code_width(state.values.size());
        if (state.bytes < state.count * (width + 1)) {
            continue;
        }
        if (complete) {
            uint64_t dict_bytes = 0;
            for (const auto& value : state.values) {
                dict_bytes += value.size() + 1;
            }
            if (state.bytes - state.count * width <= dict_bytes) {
                continue;
            }
        }
        columns.push_back(
                {static_cast<uint32_t>(c), std::vector<std::string>(state.values.begin(), state.values.end())});
    }
    if (columns.empty()) {
        return std::shared_ptr<const KeyDict>();
    }
    return KeyDict::create(_layout, std::move(columns));
}

namespace {

class KeyDictIterator final : public Iterator {
public:
    KeyDictIterator(Iterator* iter, std::shared_ptr<const KeyDict> dict) : _iter(iter), _dict(std::move(dict)) {}
    ~KeyDictIterator() override { delete _iter; }

    bool Valid() const override { return _status.ok() && _iter->Valid(); }
    void SeekToFirst() override {
        _iter->SeekToFirst();
        _decode();
    }
    void SeekToLast() override {
        _iter->SeekToLast();
        _decode();
    }
    void Seek(const Slice& target) override {
        _dict->encode(target, &_target);
        _iter->Seek(Slice(_target));
        _decode();
    }
    void Next() override {
        _iter->Next();
        _decode();
    }
    void Prev() override {
        _iter->Prev();
        _decode();
    }
    Slice key() const override { return Slice(_key); }
    Slice value() const override { return _iter->value(); }
    Status status() const override { return _status.ok() ? _iter->status() : _status; }
    uint64_t max_rss_rowid() const override { return _iter->max_rss_rowid(); }
    uint32_t shared_rssid() const override { return _iter->shared_rssid(); }
    int64_t shared_version() const override { return _iter->shared_version(); }
    int32_t rssid_offset() const override { return _iter->rssid_offset(); }
    DelVectorPtr delvec() const override { return _iter->delvec(); }

private:
    void _decode() {
        if (_iter->Valid()) {
            auto st = _dict->decode(_iter->key(), &_key);
            if (!st.ok()) {
                _status = std::move(st);
            }
        }
    }

    Iterator* _iter;
    std::shared_ptr<const KeyDict> _dict;
    std::string _key;
    std::string _target;
    Status _status;
};

} // namespace

Iterator* NewKeyDictIterator(Iterator* iter, std::shared_ptr<const KeyDict> dict) {
    return new KeyDictIterator(iter, std::move(dict));
}

} // namespace starrocks::sstable
