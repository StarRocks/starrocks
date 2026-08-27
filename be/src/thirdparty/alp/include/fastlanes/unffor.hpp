#ifndef FASTLANES_UNFFOR_HPP
#define FASTLANES_UNFFOR_HPP

#include <cstdint>

namespace fastlanes { namespace generated { namespace unffor { namespace fallback { namespace scalar {
void unffor(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw, const uint64_t* __restrict a_base_p);
void unffor(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw, const uint32_t* __restrict a_base_p);
void unffor(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw, const uint16_t* __restrict a_base_p);
void unffor(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw, const uint8_t* __restrict a_base_p);

void unffor(const int64_t* __restrict in, int64_t* __restrict out, uint8_t bw, const int64_t* __restrict a_base_p);
void unffor(const int32_t* __restrict in, int32_t* __restrict out, uint8_t bw, const int32_t* __restrict a_base_p);
void unffor(const int16_t* __restrict in, int16_t* __restrict out, uint8_t bw, const int16_t* __restrict a_base_p);
void unffor(const int8_t* __restrict in, int8_t* __restrict out, uint8_t bw, const int8_t* __restrict a_base_p);
}}}}} // namespace fastlanes::generated::unffor::fallback::scalar

namespace unffor = fastlanes::generated::unffor::fallback::scalar;

#endif
