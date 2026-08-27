#include "fastlanes/ffor.hpp"

namespace fastlanes::generated::ffor::fallback::scalar {

void ffor(const int64_t* __restrict in, int64_t* __restrict out, uint8_t bw, const int64_t* __restrict a_base_p) {
	auto const* in_u   = reinterpret_cast<const uint64_t*>(in);
	auto*       out_u  = reinterpret_cast<uint64_t*>(out);
	auto const* base_u = reinterpret_cast<const uint64_t*>(a_base_p);

	ffor(in_u, out_u, bw, base_u);
}

void ffor(const int32_t* __restrict in, int32_t* __restrict out, uint8_t bw, const int32_t* __restrict a_base_p) {
	auto const* in_u   = reinterpret_cast<const uint32_t*>(in);
	auto*       out_u  = reinterpret_cast<uint32_t*>(out);
	auto const* base_u = reinterpret_cast<const uint32_t*>(a_base_p);

	ffor(in_u, out_u, bw, base_u);
}

void ffor(const int16_t* __restrict in, int16_t* __restrict out, uint8_t bw, const int16_t* __restrict a_base_p) {
	auto const* in_u   = reinterpret_cast<const uint16_t*>(in);
	auto*       out_u  = reinterpret_cast<uint16_t*>(out);
	auto const* base_u = reinterpret_cast<const uint16_t*>(a_base_p);

	ffor(in_u, out_u, bw, base_u);
}

void ffor(const int8_t* __restrict in, int8_t* __restrict out, uint8_t bw, const int8_t* __restrict a_base_p) {
	auto const* in_u   = reinterpret_cast<const uint8_t*>(in);
	auto*       out_u  = reinterpret_cast<uint8_t*>(out);
	auto const* base_u = reinterpret_cast<const uint8_t*>(a_base_p);

	ffor(in_u, out_u, bw, base_u);
}
} // namespace fastlanes::generated::ffor::fallback::scalar