#include "varint_encoder.h"

#include <stdexcept>

#include "util/bitmap_update_context.h"

namespace starrocks {

VarIntEncoder::VarIntEncoder() = default;

VarIntEncoder::~VarIntEncoder() = default;

void VarIntEncoder::encodeValue(uint32_t value, std::vector<uint8_t>* output) {
    while (value >= 0x80) {
        output->push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    output->push_back(static_cast<uint8_t>(value & 0x7F));
}

uint32_t VarIntEncoder::decodeValue(const std::vector<uint8_t>& data, size_t& offset) {
    if (offset >= data.size()) {
        throw std::runtime_error("Invalid offset in decodeVarInt");
    }

    uint32_t result = 0;
    int shift = 0;

    while (offset < data.size()) {
        const uint8_t byte = data[offset++];
        result |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
        if (shift > 28) {
            throw std::runtime_error("VarInt too large");
        }
    }
    return result;
}

Status VarIntEncoder::encode(const roaring::Roaring& roaring, std::vector<uint8_t>* result) {
    result->clear();
    if (roaring.cardinality() <= 0) {
        return Status::OK();
    }

    if (roaring.maximum() > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument("too large roaring for VarIntEncoder, should not bigger that 2147483647.");
    }

    uint32_t last = 0;
    for (const auto pos : roaring) {
        const uint32_t delta = pos - last;
        encodeValue(delta, result);
        last = pos;
    }
    return Status::OK();
}

Status VarIntEncoder::decode(const std::vector<uint8_t>& data, roaring::Roaring* result) {
    if (data.empty()) {
        result->clear();
        return Status::OK();
    }

    size_t offset = 0;
    uint32_t current_position = decodeValue(data, offset);
    BitmapUpdateContextRefOrSingleValue positions(current_position);
    while (offset < data.size()) {
        const uint32_t delta = decodeValue(data, offset);
        current_position += delta;
        positions.add(current_position);
    }
    positions.flush_pending_adds();
    if (positions.is_context()) {
        result = positions.roaring();
    } else {
        result->add(positions.value());
    }
    return Status::OK();
}

} // namespace starrocks
