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

std::vector<uint8_t> PostingList::encode(Encoder* encoder) const {
    if (_postings == nullptr) {
        return {};
    }

    _postings->flush_pending_adds();

    detail::Roaring64Map roaring;
    if (_postings->is_context()) {
        roaring.add(_postings->value());
    } else {
        roaring = *(_postings->roaring());
    }

    auto doc_ids = roaring.getAllHighBits();
    auto result = encoder->encode(doc_ids);
    for (const auto doc_id : doc_ids) {
        auto positions = roaring.getLowBitsRoaring(doc_id);
        auto encoded_positions = encoder->encode(positions);
        result.insert(result.end(), encoded_positions.begin(), encoded_positions.end());
    }
    return result;
}

} // namespace starrocks