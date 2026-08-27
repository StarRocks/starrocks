#ifndef FASTLANES_MACROS_HPP
#define FASTLANES_MACROS_HPP

#define _mm256_set1_epi64   _mm256_set1_epi64x

#define _mm128_loadu_si128  _mm_loadu_si128
#define _mm128_storeu_si128 _mm_storeu_si128
#define _mm128_and_si128    _mm_and_si128
#define _mm128_or_si128     _mm_or_si128
#define _mm128_srli_epi64   _mm_srli_epi64
#define _mm128_slli_epi64   _mm_slli_epi64
#define _mm128_set1_epi8    _mm_set1_epi8
#define _mm128_set1_epi16   _mm_set1_epi16
#define _mm128_set1_epi32   _mm_set1_epi32
#define _mm128_set1_epi64   _mm_set1_epi64x

#endif // FASTLANES_MACROS_HPP
