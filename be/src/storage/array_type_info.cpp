// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/array_type_info.h"

#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "util/mem_util.hpp"

namespace starrocks {

class ArrayTypeInfo final : public TypeInfo {
public:
    explicit ArrayTypeInfo(const TypeInfoPtr& item_type_info)
            : _item_type_info(item_type_info), _item_size(item_type_info->size()) {}

    bool equal(const void* left, const void* right) const override {
        auto l_value = unaligned_load<Collection>(left);
        auto r_value = unaligned_load<Collection>(right);
        if (l_value.length != r_value.length) {
            return false;
        }
        size_t len = l_value.length;

        if (!l_value.has_null && !r_value.has_null) {
            for (size_t i = 0; i < len; ++i) {
                if (!_item_type_info->equal((uint8_t*)(l_value.data) + i * _item_size,
                                            (uint8_t*)(r_value.data) + i * _item_size)) {
                    return false;
                }
            }
        } else {
            for (size_t i = 0; i < len; ++i) {
                if (l_value.null_signs[i]) {
                    if (r_value.null_signs[i]) { // both are null
                        continue;
                    } else { // left is null & right is not null
                        return false;
                    }
                } else if (r_value.null_signs[i]) { // left is not null & right is null
                    return false;
                }
                if (!_item_type_info->equal((uint8_t*)(l_value.data) + i * _item_size,
                                            (uint8_t*)(r_value.data) + i * _item_size)) {
                    return false;
                }
            }
        }
        return true;
    }

    int cmp(const void* left, const void* right) const override {
        auto l_value = unaligned_load<Collection>(left);
        auto r_value = unaligned_load<Collection>(right);
        size_t l_length = l_value.length;
        size_t r_length = r_value.length;
        size_t cur = 0;

        if (!l_value.has_null && !r_value.has_null) {
            while (cur < l_length && cur < r_length) {
                int result = _item_type_info->cmp((uint8_t*)(l_value.data) + cur * _item_size,
                                                  (uint8_t*)(r_value.data) + cur * _item_size);
                if (result != 0) {
                    return result;
                }
                ++cur;
            }
        } else {
            while (cur < l_length && cur < r_length) {
                if (l_value.null_signs[cur]) {
                    if (!r_value.null_signs[cur]) { // left is null & right is not null
                        return -1;
                    }
                } else if (r_value.null_signs[cur]) { // left is not null & right is null
                    return 1;
                } else { // both are not null
                    int result = _item_type_info->cmp((uint8_t*)(l_value.data) + cur * _item_size,
                                                      (uint8_t*)(r_value.data) + cur * _item_size);
                    if (result != 0) {
                        return result;
                    }
                }
                ++cur;
            }
        }

        if (l_length < r_length) {
            return -1;
        } else if (l_length > r_length) {
            return 1;
        } else {
            return 0;
        }
    }

    void shallow_copy(void* dest, const void* src) const override {
        unaligned_store<Collection>(dest, unaligned_load<Collection>(src));
    }

    void deep_copy(void* dest, const void* src, MemPool* mem_pool) const override {
        Collection dest_value;
        auto src_value = unaligned_load<Collection>(src);

        dest_value.length = src_value.length;

        size_t item_size = src_value.length * _item_size;
        size_t nulls_size = src_value.has_null ? src_value.length : 0;
        dest_value.data = mem_pool->allocate(item_size + nulls_size);
        assert(dest_value.data != nullptr);
        dest_value.has_null = src_value.has_null;
        dest_value.null_signs = src_value.has_null ? reinterpret_cast<uint8_t*>(dest_value.data) + item_size : nullptr;

        // copy null_signs
        if (src_value.has_null) {
            memory_copy(dest_value.null_signs, src_value.null_signs, sizeof(uint8_t) * src_value.length);
        }

        // copy item
        for (uint32_t i = 0; i < src_value.length; ++i) {
            if (dest_value.is_null_at(i)) {
                auto* item = reinterpret_cast<Collection*>((uint8_t*)dest_value.data + i * _item_size);
                item->data = nullptr;
                item->length = 0;
                item->has_null = false;
                item->null_signs = nullptr;
            } else {
                _item_type_info->deep_copy((uint8_t*)(dest_value.data) + i * _item_size,
                                           (uint8_t*)(src_value.data) + i * _item_size, mem_pool);
            }
        }
        unaligned_store<Collection>(dest, dest_value);
    }

    void copy_object(void* dest, const void* src, MemPool* mem_pool) const override { deep_copy(dest, src, mem_pool); }

    void direct_copy(void* dest, const void* src, MemPool* mem_pool) const override { deep_copy(dest, src, mem_pool); }

    Status convert_from(void* dest, const void* src, const TypeInfoPtr& src_type, MemPool* mem_pool) const override {
        return Status::NotSupported("Not supported function");
    }

    Status from_string(void* buf, const std::string& scan_key) const override {
        return Status::NotSupported("Not supported function");
    }

    std::string to_string(const void* src) const override {
        auto src_value = unaligned_load<Collection>(src);
        std::string result = "[";

        for (size_t i = 0; i < src_value.length; ++i) {
            if (src_value.has_null && src_value.null_signs[i]) {
                result += "NULL";
            } else {
                result += _item_type_info->to_string((uint8_t*)(src_value.data) + i * _item_size);
            }
            if (i != src_value.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    void set_to_max(void* buf) const override { DCHECK(false) << "set_to_max of list is not implemented."; }

    void set_to_min(void* buf) const override { DCHECK(false) << "set_to_min of list is not implemented."; }

    uint32_t hash_code(const void* data, uint32_t seed) const override {
        auto value = unaligned_load<Collection>(data);
        uint32_t result = HashUtil::hash(&(value.length), sizeof(size_t), seed);
        for (size_t i = 0; i < value.length; ++i) {
            if (value.null_signs[i]) {
                result = seed * result;
            } else {
                result = seed * result + _item_type_info->hash_code((uint8_t*)(value.data) + i * _item_size, seed);
            }
        }
        return result;
    }

    size_t size() const override { return sizeof(Collection); }

    FieldType type() const override { return OLAP_FIELD_TYPE_ARRAY; }

    const TypeInfoPtr& item_type_info() const { return _item_type_info; }

protected:
    int _datum_cmp_impl(const Datum& left, const Datum& right) const override {
        CHECK(false) << "not implemented";
        return -1;
    }

private:
    TypeInfoPtr _item_type_info;
    const size_t _item_size;
};

TypeInfoPtr get_array_type_info(const TypeInfoPtr& item_type) {
    return std::make_shared<ArrayTypeInfo>(item_type);
}

const TypeInfoPtr& get_item_type_info(const TypeInfo* type_info) {
    auto array_type_info = down_cast<const ArrayTypeInfo*>(type_info);
    return array_type_info->item_type_info();
}

} // namespace starrocks
