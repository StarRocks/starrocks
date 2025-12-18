#include "posting.h"

#include "storage/posting/encoder.h"

namespace starrocks {

PostingList::PostingList(PostingList&& rhs) noexcept {
    _postings = std::move(rhs._postings);
}

PostingList& PostingList::operator=(PostingList&& rhs) noexcept {
    _postings = std::move(rhs._postings);
    return *this;
}

PostingList::PostingList() = default;

PostingList::~PostingList() = default;

void PostingList::add_posting(rowid_t doc_id, rowid_t pos) {
    const uint64_t val = static_cast<uint64_t>(doc_id) << 32 | static_cast<uint64_t>(pos);
    if (_postings == nullptr) {
        _postings = std::make_unique<BitmapUpdateContextRefOrSingleValue<uint64_t>>(val);
    } else {
        _postings->add(val);
    }
}

void PostingList::finalize() const {
    if (_postings == nullptr || !_postings->is_context()) {
        return;
    }

    _postings->flush_pending_adds();
    _postings->roaring()->runOptimize();
}

uint32_t PostingList::get_num_doc_ids() const {
    if (_postings == nullptr) {
        return 0;
    }

    if (_postings->is_context()) {
        return _postings->roaring()->getHighBitsCount();
    }
    return 1;
}

roaring::Roaring PostingList::get_all_doc_ids() const {
    if (_postings == nullptr) {
        return roaring::Roaring();
    }

    if (_postings->is_context()) {
        return _postings->roaring()->getAllHighBits();
    }

    auto val = _postings->value();
    rowid_t high = static_cast<rowid_t>(val >> 32);
    return roaring::Roaring::bitmapOf(high);
}

roaring::Roaring PostingList::get_positions(rowid_t doc_id) const {
    if (_postings == nullptr) {
        return roaring::Roaring();
    }

    if (_postings->is_context()) {
        return _postings->roaring()->getLowBitsRoaring(doc_id);
    }
    auto val = _postings->value();
    rowid_t high = static_cast<rowid_t>(val >> 32);
    if (high == doc_id) {
        return roaring::Roaring::bitmapOf(static_cast<uint32_t>(val));
    }
    return roaring::Roaring();
}

} // namespace starrocks