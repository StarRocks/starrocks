#pragma once

#include "roaring/roaring.hh"
#include "storage/rowset/common.h"
#include "types/bitmap_value_detail.h"

namespace starrocks {

class BitmapUpdateContext {
    static constexpr size_t estimate_size_threshold = 1024;
    static constexpr size_t _ADD_BATCH_SIZE = 512;

public:
    explicit BitmapUpdateContext(uint32_t rid) : _roaring(roaring::Roaring::bitmapOf(1, rid)) {
        _pending_adds.reserve(_ADD_BATCH_SIZE);
    };
    explicit BitmapUpdateContext(uint32_t rid0, uint32_t rid1)
            : _roaring(roaring::Roaring::bitmapOfList({rid0, rid1})) {
        _pending_adds.reserve(_ADD_BATCH_SIZE);
    };

    roaring::Roaring* roaring() { return &_roaring; }

    static uint64_t estimate_size(int element_count) {
        // When _element_count is less than estimate_size_threshold, we use
        // (1 + _element_count + 1) * (sizeof(uint32_t)) to approximately estimate true size of roaring bitmap:
        // one bit pre    4 bytes         4 bytes *  _element_count
        // [ 1            cardinality      data ]
        return (1 + sizeof(uint32_t) * (element_count + 1));
    }

    static void init_estimate_size(uint64_t* reverted_index_size) {
        *reverted_index_size += BitmapUpdateContext::estimate_size(1);
    }

    void add_and_flush_if_needed(uint32_t rid) {
        _pending_adds.push_back(rid);
        if (_pending_adds.size() >= _ADD_BATCH_SIZE) {
            flush_pending_adds();
        }
    }

    void flush_pending_adds() {
        if (!_pending_adds.empty()) {
            _roaring.addMany(_pending_adds.size(), _pending_adds.data());
            _pending_adds.clear();
        }
    }

    // When _element_count is less than estimate_size_threshold, update the estimate size
    // When _element_count equals to estimate_size_threshold, clear previous estimate size, disable estimation.
    // When _element_count is larger than estimate_size_threshold, use `getSizeInBytes(false)` to get
    // the exact size of roaring bitmap. For efficiency, we will not update the roaring's size each time when size changed.
    // We will save the sized changed roaring bitmap in _late_update_context_vector, and delay calculation of update size
    // each time when `size()` of bitmap is called.
    // Return value in this function indicates whether this BitmapUpdateContext needs to be added to the _late_update_context_vector
    bool update_estimate_size(uint64_t* reverted_index_size) {
        bool need_add = false;
        _element_count++;
        if (_element_count < estimate_size_threshold) {
            *reverted_index_size += sizeof(uint32_t);
        } else if (_element_count == estimate_size_threshold) {
            *reverted_index_size -= BitmapUpdateContext::estimate_size(_element_count);
            _size_changed = true;
            need_add = true;
        } else {
            // Add BitmapUpdateContext to _late_update_context_vector iff
            // it hash not been added to _late_update_context_vector before.
            if (!_size_changed) {
                need_add = true;
            }
            _size_changed = true;
        }
        return need_add;
    }

    void late_update_size(uint64_t* reverted_index_size) {
        uint64_t current_size = _roaring.getSizeInBytes(false);
        *reverted_index_size += (current_size - _previous_size);
        _previous_size = current_size;
        _size_changed = false;
    }

private:
    roaring::Roaring _roaring;
    uint64_t _previous_size{0};
    uint32_t _element_count{1};
    bool _size_changed{false};
    std::vector<uint32_t> _pending_adds;
};

// if last bit is 0 it is std::unique_ptr<BitmapUpdateContext>
// else it is a single value
class BitmapUpdateContextRefOrSingleValue {
public:
    BitmapUpdateContextRefOrSingleValue(const BitmapUpdateContextRefOrSingleValue& rhs) = delete;
    BitmapUpdateContextRefOrSingleValue& operator=(const BitmapUpdateContextRefOrSingleValue& rhs) = delete;
    BitmapUpdateContextRefOrSingleValue(BitmapUpdateContextRefOrSingleValue&& rhs) noexcept {
        _value = rhs._value;
        rhs._value = 1; // make sure not delete when rhs is destroyed
    }
    BitmapUpdateContextRefOrSingleValue& operator=(BitmapUpdateContextRefOrSingleValue&& rhs) noexcept {
        this->_value = rhs._value;
        rhs._value = 1; // make sure not delete when rhs is destroyed
        return *this;
    }
    BitmapUpdateContextRefOrSingleValue(uint32_t value) { _value = (value << 1) | 1; }
    ~BitmapUpdateContextRefOrSingleValue() {
        if (is_context()) {
            delete context();
        }
    }
    bool is_context() const { return (_value & 1) == 0; }
    uint32_t value() const { return _value >> 1; }
    BitmapUpdateContext* context() const {
        return reinterpret_cast<BitmapUpdateContext*>(_value); // NOLINT
    }
    void add(uint32_t rid) {
        if (is_context()) {
            context()->add_and_flush_if_needed(rid);
        } else {
            auto* context = new BitmapUpdateContext(value(), rid);
            _value = reinterpret_cast<uint64_t>(context); // NOLINT
        }
    }
    roaring::Roaring* roaring() { return context()->roaring(); }

    static uint64_t estimate_size(int element_count) { return BitmapUpdateContext::estimate_size(element_count); }

    static void init_estimate_size(uint64_t* reverted_index_size) {
        return BitmapUpdateContext::init_estimate_size(reverted_index_size);
    }

    bool update_estimate_size(uint64_t* reverted_index_size) {
        if (is_context()) {
            return context()->update_estimate_size(reverted_index_size);
        } else {
            return false;
        }
    }

    void late_update_size(uint64_t* reverted_index_size) {
        if (is_context()) {
            context()->late_update_size(reverted_index_size);
        }
    }

    void flush_pending_adds() {
        if (is_context()) {
            context()->flush_pending_adds();
        }
    }

private:
    uint64_t _value;
};

} // namespace starrocks