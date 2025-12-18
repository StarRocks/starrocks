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

uint32_t PostingList::get_num_doc_ids() const {
    const auto postings = _internal_get_all_postings();
    return postings.getHighBitsCount();
}

roaring::Roaring PostingList::get_all_doc_ids() const {
    const auto postings = _internal_get_all_postings();
    return postings.getAllHighBits();
}

roaring::Roaring PostingList::get_positions(rowid_t doc_id) const {
    const auto postings = _internal_get_all_postings();
    return postings.getLowBitsRoaring(doc_id);
}

detail::Roaring64Map PostingList::_internal_get_all_postings() const {
    if (_postings == nullptr) {
        return detail::Roaring64Map();
    }

    _postings->flush_pending_adds();

    detail::Roaring64Map roaring;
    if (_postings->is_context()) {
        roaring = *_postings->roaring();
    } else {
        roaring.add(_postings->value());
    }
    return roaring;
}

} // namespace starrocks