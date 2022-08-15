// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/del_vector.h"

#include <memory>

#include "gutil/strings/substitute.h"
#include "util/raw_container.h"

namespace starrocks {

DelVector::DelVector() = default;

DelVector::~DelVector() = default;

void DelVector::set_empty() {
    _version = 1;
    _loaded = true;
    _cardinality = 0;
    _memory_usage = 0;
    _roaring.reset();
}

void DelVector::_add_dels(const std::vector<uint32_t>& dels) {
    if (!_roaring) {
        _roaring = std::make_unique<Roaring>(dels.size(), dels.data());
    } else {
        _roaring->addMany(dels.size(), dels.data());
    }
    _update_stats();
}

void DelVector::add_dels_as_new_version(const std::vector<uint32_t>& dels, int64_t version,
                                        std::shared_ptr<DelVector>* pdelvec) const {
    CHECK(this != pdelvec->get());
    DelVectorPtr tmp(new DelVector());
    if (_roaring) {
        tmp->_roaring = std::make_unique<Roaring>(*_roaring);
    }
    tmp->_version = version;
    tmp->_loaded = true;
    tmp->_add_dels(dels);
    tmp.swap(*pdelvec);
}

Status DelVector::load(int64_t version, const char* data, size_t length) {
    if (length < 1) {
        return Status::Corruption("zero length");
    }
    if (*data != 0x01) {
        return Status::Corruption("invalid flag");
    }
    data += 1;
    length -= 1;
    _loaded = true;
    _version = version;
    if (length > 0) {
        _roaring = std::make_unique<Roaring>(Roaring::readSafe(data, length));
    }
    _update_stats();
    return Status::OK();
}

void DelVector::init(int64_t version, const uint32_t* data, size_t length) {
    _loaded = true;
    _version = version;
    if (length > 0) {
        _roaring = std::make_unique<Roaring>(length, data);
    }
    _update_stats();
}

string DelVector::save() const {
    string ret;
    auto roaring_size = _roaring ? _roaring->getSizeInBytes() : 0;
    ret.resize(roaring_size + 1);
    ret[0] = 0x01; // one byte flag.
    if (roaring_size > 0) {
        _roaring->write(ret.data() + 1);
    }
    return ret;
}

string DelVector::to_string() const {
    return strings::Substitute("version:$0 $1", _version, _roaring ? _roaring->toString() : string("null"));
}

void DelVector::_update_stats() {
    // TODO(cbl): optimization
    if (_roaring) {
        roaring_statistics_t st;
        roaring_bitmap_statistics(&_roaring->roaring, &st);
        _memory_usage = st.n_bytes_array_containers + st.n_bytes_bitset_containers + st.n_bytes_run_containers;
        //_memory_usage = _roaring->getSizeInBytes(false);
        _cardinality = st.cardinality;
    } else {
        _memory_usage = 0;
        _cardinality = 0;
    }
}

} // namespace starrocks
