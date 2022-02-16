// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include "gutil/strings/substitute.h"
#include "storage/fs/fs_util.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "util/coding.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/murmur_hash3.h"

namespace starrocks {

MutableIndex::MutableIndex() {}

MutableIndex::~MutableIndex() {}

constexpr uint64_t seed0 = 12980785309524476958ULL;
constexpr uint64_t seed1 = 9110941936030554525ULL;

template <size_t KeySize>
struct FixedKeyHash {
    uint64_t operator()(const FixedKey<KeySize>& k) const {
        uint64_t ret;
        murmur_hash3_x64_64(k.data, KeySize, seed0, &ret);
        return ret;
    }
};

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
private:
    phmap::flat_hash_map<FixedKey<KeySize>, IndexValue, FixedKeyHash<KeySize>> _map;

public:
    FixedMutableIndex() {}
    ~FixedMutableIndex() override {}

    Status get(size_t n, const void* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found) const override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto iter = _map.find(key, hash);
            if (iter == _map.end()) {
                values[i] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                values[i] = iter->second;
                nfound += (iter->second != NullIndexValue);
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (p.second) {
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                auto old_value = p.first->second;
                old_values[i] = old_value;
                nfound += (old_value != NullIndexValue);
                p.first->second = v;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(size_t n, const void* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found) {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (p.second) {
                // key not exist previously
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                // key exist
                auto old_value = p.first->second;
                nfound += (old_value != NullIndexValue);
                p.first->second = v;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status insert(size_t n, const void* keys, const IndexValue* values) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (!p.second) {
                std::string msg = strings::Substitute("FixedMutableIndex<$0> insert found duplicate key $1", KeySize,
                                                      hexdump((const char*)key.data, KeySize));
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    Status erase(size_t n, const void* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, NullIndexValue);
            if (p.second) {
                // key not exist previously
                old_values[i] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                // key exist
                old_values[i] = p.first->second;
                nfound += (p.first->second != NullIndexValue);
                p.first->second = NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status append_wal(size_t n, const void* keys, const IndexValue* values, fs::WritableBlock* wblock,
                      uint32_t* page_size) {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        faststring fixed_buf;
        fixed_buf.reserve(n * (KeySize + sizeof(IndexValue)));
        for (size_t i = 0; i < n; i++) {
            const auto v = (values != nullptr) ? values[i] : NullIndexValue;
            fixed_buf.append(fkeys[i].data, KeySize);
            put_fixed64_le(&fixed_buf, v);
        }
        RETURN_IF_ERROR(wblock->append(fixed_buf));
        *page_size += fixed_buf.size();
        return Status::OK();
    }
};

StatusOr<std::unique_ptr<MutableIndex>> MutableIndex::create(size_t key_size) {
    if (key_size == 0) {
        return Status::NotSupported("varlen key size IndexL0 not supported");
    }

#define CASE_SIZE(s) \
    case s:          \
        return std::make_unique<FixedMutableIndex<s>>();

#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)

    switch (key_size) {
        CASE_SIZE_8(1)
        CASE_SIZE_8(9)
        CASE_SIZE_8(17)
        CASE_SIZE_8(25)
        CASE_SIZE_8(33)
        CASE_SIZE_8(41)
        CASE_SIZE_8(49)
        CASE_SIZE_8(57)
    default:
        return Status::NotSupported("large key size IndexL0 not supported");
    }

#undef CASE_SIZE_8
#undef CASE_SIZE
}

Status ImmutableIndex::get(size_t n, const void* keys, const KeysInfo& keys_info, IndexValue* values,
                           size_t* num_found) const {
    *num_found = 0;
    return Status::OK();
}

Status ImmutableIndex::check_not_exist(size_t n, const void* keys) {
    return Status::OK();
}

PersistentIndex::PersistentIndex(const std::string& path) : _path(path) {}

PersistentIndex::~PersistentIndex() {
    if (_index_block) {
        _index_block->close();
    }
}

Status PersistentIndex::create(size_t key_size, const EditVersion& version) {
    if (loaded()) {
        return Status::InternalError("PersistentIndex already loaded");
    }
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    fs::CreateBlockOptions wblock_opts({_path});
    wblock_opts.mode = Env::MUST_EXIST;
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &_index_block));

    _index_meta.set_key_size(key_size);
    version.to_pb(_index_meta.mutable_version());
    _index_meta.set_size(0);
    _index_meta.mutable_l0_meta();
    // TODO: write to index file
    _key_size = key_size;
    _size = 0;
    _version = version;
    auto st = MutableIndex::create(key_size);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    return Status::OK();
}

Status PersistentIndex::load(Tablet* tablet) {
    RETURN_IF_ERROR(
            TabletMetaManager::get_persistent_index_meta(tablet->data_dir(), tablet->tablet_id(), &_index_meta));
    size_t key_size = _index_meta.key_size();
    _size = _index_meta.size();
    DCHECK_EQ(key_size, _key_size);
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::ReadableBlock> rblock;
    DeferOp close_block([&rblock] { rblock->close(); });
    RETURN_IF_ERROR(block_mgr->open_block(_path, &rblock));
    // TODO: load snapshot first
    if (!_index_meta.has_l0_meta()) {
        return Status::OK();
    }
    MutableIndexMetaPB* l0_meta = _index_meta.mutable_l0_meta();
    int n = l0_meta->wals_size();
    // read wals and build l0
    for (int i = 0; i < n; i++) {
        auto* wal_pb = l0_meta->mutable_wals(i);
        auto* page_pb = wal_pb->mutable_data();
        size_t offset = page_pb->offset();
        size_t size = page_pb->size();
        _offset = offset + size;
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, size);
        RETURN_IF_ERROR(rblock->read(offset, buff));

        size_t nums = (size / (key_size + sizeof(IndexValue)));
        uint8_t keys[key_size * nums];
        std::vector<IndexValue> values;
        values.reserve(nums);
        size_t buf_offset = 0;
        for (size_t j = 0; j < nums; ++j) {
            memcpy(keys + j * key_size, buff.data() + buf_offset, key_size);
            IndexValue val = UNALIGNED_LOAD64(buff.data() + buf_offset + key_size);
            values.emplace_back(val);
            buf_offset += key_size + sizeof(IndexValue);
        }
        DCHECK_EQ(buf_offset, size);
        std::vector<IndexValue> old_values(nums);
        KeysInfo upsert_not_found;
        size_t upsert_num_found = 0;
        RETURN_IF_ERROR(
                _l0->upsert(nums, keys, values.data(), old_values.data(), &upsert_not_found, &upsert_num_found));
    }
    // the data in the end maybe invalid
    // so we need to reopen the index file after truncated
    RETURN_IF_ERROR(_index_block->close());
    RETURN_IF_ERROR(FileSystemUtil::resize_file(_path, _offset));
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({_path});
    wblock_opts.mode = Env::MUST_EXIST;
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));
    _index_block = std::move(wblock);

    return Status::OK();
}

Status PersistentIndex::prepare(const EditVersion& version) {
    _version = version;
    return Status::OK();
}

Status PersistentIndex::abort() {
    return Status::NotSupported("TODO");
}

Status PersistentIndex::commit() {
    // TODO: l0 may be need to flush
    MutableIndexMetaPB* l0_meta = _index_meta.mutable_l0_meta();
    IndexWalMetaPB* wal_pb = l0_meta->add_wals();
    _version.to_pb(wal_pb->mutable_version());

    PagePointerPB* data = wal_pb->mutable_data();
    data->set_offset(_offset);
    data->set_size(_page_size);

    _version.to_pb(_index_meta.mutable_version());
    _index_meta.set_size(_size);

    _offset += _page_size;
    _page_size = 0;
    return Status::OK();
}

Status PersistentIndex::get(size_t n, const void* keys, IndexValue* values) {
    KeysInfo l1_checks;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->get(n, keys, values, &l1_checks, &num_found));
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, values, &num_found));
    }
    return Status::OK();
}

Status PersistentIndex::upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values) {
    KeysInfo l1_checks;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->append_wal(n, keys, values, _index_block.get(), &_page_size));
    RETURN_IF_ERROR(_l0->upsert(n, keys, values, old_values, &l1_checks, &num_found));
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, old_values, &num_found));
    }
    _size += (n - num_found);
    return Status::OK();
}

Status PersistentIndex::insert(size_t n, const void* keys, const IndexValue* values, bool check_l1) {
    RETURN_IF_ERROR(_l0->append_wal(n, keys, values, _index_block.get(), &_page_size));
    RETURN_IF_ERROR(_l0->insert(n, keys, values));
    if (_l1 && check_l1) {
        RETURN_IF_ERROR(_l1->check_not_exist(n, keys));
    }
    _size += n;
    return Status::OK();
}

Status PersistentIndex::erase(size_t n, const void* keys, IndexValue* old_values) {
    KeysInfo l1_checks;
    size_t num_erased = 0;
    RETURN_IF_ERROR(_l0->append_wal(n, keys, nullptr, _index_block.get(), &_page_size));
    RETURN_IF_ERROR(_l0->erase(n, keys, old_values, &l1_checks, &num_erased));
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, old_values, &num_erased));
    }
    CHECK(_size >= num_erased) << strings::Substitute("_size($0) < num_erased($1)", _size, num_erased);
    _size -= num_erased;
    return Status::OK();
}

} // namespace starrocks
