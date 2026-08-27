#ifndef ALP_FALP_HPP
#define ALP_FALP_HPP

#include <cstdint>

namespace generated { namespace falp {
namespace fallback {
namespace scalar {

void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);

inline void falp(const int64_t* __restrict in,
                 double* __restrict out,
                 uint8_t bw,
                 const int64_t* __restrict base,
                 uint8_t factor,
                 uint8_t exponent) {
	const auto* in_p   = reinterpret_cast<const uint64_t*>(in);
	const auto* base_p = reinterpret_cast<const uint64_t*>(base);
	falp(in_p, out, bw, base_p, factor, exponent);
}

void falp(const uint32_t* __restrict in,
          float* __restrict out,
          uint8_t bw,
          const uint32_t* __restrict base,
          uint8_t factor,
          uint8_t exponent);

inline void falp(const int32_t* __restrict in,
                 float* __restrict out,
                 uint8_t bw,
                 const int32_t* __restrict base,
                 uint8_t factor,
                 uint8_t exponent) {
	const auto* in_p   = reinterpret_cast<const uint32_t*>(in);
	const auto* base_p = reinterpret_cast<const uint32_t*>(base);
	falp(in_p, out, bw, base_p, factor, exponent);
}
} // namespace scalar
namespace unit64 {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);

} // namespace unit64
} // namespace fallback

namespace helper { namespace scalar {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
}} // namespace helper::scalar

namespace x86_64 {
namespace sse {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
} // namespace sse

namespace avx2 {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
} // namespace avx2

namespace avx512f {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
}

namespace avx512bw {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
} // namespace avx512bw

} // namespace x86_64
namespace wasm { namespace simd128 {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
}} // namespace wasm::simd128

namespace arm64v8 {
namespace neon {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
} // namespace neon

namespace sve {
void falp(const uint64_t* __restrict in,
          double* __restrict out,
          uint8_t bw,
          const uint64_t* __restrict a_base_p,
          uint8_t factor,
          uint8_t exponent);
} // namespace sve
} // namespace arm64v8
}} // namespace generated::falp

#endif // ALP_FALP_HPP
