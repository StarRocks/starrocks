#include "encoder.h"

#include "varint_encoder.h"

namespace starrocks {

std::shared_ptr<Encoder> EncoderFactory::createEncoder(const EncodingType type) {
    switch (type) {
    case EncodingType::VARINT:
        return std::make_shared<VarIntEncoder>();
    default:
        return nullptr;
    }
}

} // namespace starrocks