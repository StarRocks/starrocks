#ifndef ALP_RD_HPP
#define ALP_RD_HPP

#include "alp/common.hpp"
#include "alp/constants.hpp"
#include "alp/encoder.hpp"
#include "alp/sampler.hpp"
#include <algorithm>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wfloat-conversion"
#pragma GCC diagnostic ignored "-Wimplicit-int-conversion"

namespace alp {

template <class PT>
struct rd_encoder {
	using UT                                     = typename inner_t<PT>::ut;
	static constexpr uint8_t EXACT_TYPE_BIT_SIZE = sizeof(UT) * 8;

	//! Estimate the bits per value of ALPRD within a sample
	static inline double estimate_compression_size(const bw_t     right_bit_width,
	                                               const bw_t     left_bit_width,
	                                               const exp_c_t  exceptions_count,
	                                               const uint64_t sample_count) {
		const double exceptions_size = exceptions_count * (RD_EXCEPTION_POSITION_SIZE + RD_EXCEPTION_SIZE);
		const double estimated_size =
		    right_bit_width + left_bit_width + (exceptions_size / static_cast<double>(sample_count));
		return estimated_size;
	}

	template <bool PERSIST_DICT>
	static double build_left_parts_dictionary(const PT* in_p, bw_t right_bit_width, state<PT>& stt) {
		std::unordered_map<UT, int32_t>       left_parts_hash;
		std::vector<std::pair<int, uint64_t>> left_parts_sorted_repetitions;

		auto* in = reinterpret_cast<const UT*>(in_p);
		// Building a hash for all the left parts and how many times they appear
		for (size_t i = 0; i < stt.sampled_values_n; i++) {
			auto left_tmp = in[i] >> right_bit_width;
			left_parts_hash[left_tmp]++;
		}

		// We build a vector from the hash to be able to sort it by repetition count
		left_parts_sorted_repetitions.reserve(left_parts_hash.size());
		for (auto& pair : left_parts_hash) {
			left_parts_sorted_repetitions.emplace_back(pair.second, pair.first);
		}
		std::sort(left_parts_sorted_repetitions.begin(),
		          left_parts_sorted_repetitions.end(),
		          [](const std::pair<uint16_t, uint64_t>& a, const std::pair<uint16_t, uint64_t>& b) {
			          return a.first > b.first;
		          });

		// Exceptions are left parts which do not fit in the fixed dictionary size
		uint32_t exceptions_count {0};
		for (size_t i {config::MAX_RD_DICTIONARY_SIZE}; i < left_parts_sorted_repetitions.size(); i++) {
			exceptions_count += left_parts_sorted_repetitions[i].first;
		}

		// The left parts bit width after compression is determined by how many elements are in the dictionary
		uint8_t actual_dictionary_size =
		    std::min<uint64_t>(config::MAX_RD_DICTIONARY_SIZE, left_parts_sorted_repetitions.size());
		bw_t left_bit_width = std::max<bw_t>(1, std::ceil(std::log2(actual_dictionary_size)));

		if (PERSIST_DICT) {
			stt.left_parts_dict_map.clear();
			for (size_t dict_idx = 0; dict_idx < actual_dictionary_size; dict_idx++) {
				//! The left_parts_dict keys are mapped to the left part themselves
				stt.left_parts_dict[dict_idx] = left_parts_sorted_repetitions[dict_idx].second; // .hash
				stt.left_parts_dict_map.insert({stt.left_parts_dict[dict_idx], dict_idx});
			}
			//! Pararelly we store a map of the dictionary to quickly resolve exceptions during encoding
			for (size_t i = actual_dictionary_size + 1; i < left_parts_sorted_repetitions.size(); i++) {
				stt.left_parts_dict_map.insert({left_parts_sorted_repetitions[i].second, i}); // .hash
			}
			stt.left_bit_width               = left_bit_width;
			stt.right_bit_width              = right_bit_width;
			stt.actual_dictionary_size       = actual_dictionary_size;
			stt.actual_dictionary_size_bytes = actual_dictionary_size * DICTIONARY_ELEMENT_SIZE_BYTES;
		}

		double estimated_size =
		    estimate_compression_size(right_bit_width, left_bit_width, exceptions_count, stt.sampled_values_n);
		return estimated_size;
	}

	static inline void find_best_dictionary(const PT* smp_arr, state<PT>& stt) {
		bw_t   right_bit_width {0};
		double best_dict_size = std::numeric_limits<double>::max();

		// Finding the best position to CUT the values
		for (size_t i {1}; i <= config::CUTTING_LIMIT; i++) {
			bw_t         candidate_right_bit_width = EXACT_TYPE_BIT_SIZE - i;
			const double estimated_size = build_left_parts_dictionary<false>(smp_arr, candidate_right_bit_width, stt);
			if (estimated_size < best_dict_size) {
				right_bit_width = candidate_right_bit_width;
				best_dict_size  = estimated_size;
			}
			// TODO: We can implement an early exit mechanism similar to normal ALP
		}
		build_left_parts_dictionary<true>(smp_arr, right_bit_width, stt);
	}

	/*
	 * ALP RD Encode
	 */
	static inline void encode(const PT*  dbl_arr,
	                          uint16_t*  exceptions,
	                          uint16_t*  exception_positions,
	                          uint16_t*  exceptions_count_p,
	                          UT*        right_parts,
	                          uint16_t*  left_parts,
	                          state<PT>& stt) {
		const auto* in = reinterpret_cast<const UT*>(dbl_arr);

		// Cutting the floating point values
		for (size_t i {0}; i < config::VECTOR_SIZE; ++i) {
			UT tmp         = in[i];
			right_parts[i] = tmp & ((1ULL << stt.right_bit_width) - 1);
			left_parts[i]  = (tmp >> stt.right_bit_width);
		}

		uint16_t exceptions_count {0};
		// Dictionary encoding for left parts
		for (size_t i {0}; i < config::VECTOR_SIZE; i++) {
			uint16_t dictionary_index;
			auto     dictionary_key = left_parts[i];
			if (stt.left_parts_dict_map.find(dictionary_key) == stt.left_parts_dict_map.end()) {
				// If not found on the dictionary we store the smallest non-key index as exception (the dict size)
				dictionary_index = stt.actual_dictionary_size;
			} else {
				dictionary_index = stt.left_parts_dict_map[dictionary_key];
			}
			left_parts[i] = dictionary_index;

			//! Left parts not found in the dictionary are stored as exceptions
			if (dictionary_index >= stt.actual_dictionary_size) {
				exceptions[exceptions_count]          = dictionary_key;
				exception_positions[exceptions_count] = i;
				exceptions_count++;
			}
		}
		stt.exceptions_count  = exceptions_count;
		exceptions_count_p[0] = exceptions_count;
	}

	/*
	 * ALP RD Decode
	 */
	static inline void decode(PT*        a_out,
	                          UT*        unffor_right_arr,
	                          uint16_t*  unffor_left_arr,
	                          uint16_t*  exceptions,
	                          uint16_t*  exceptions_positions,
	                          uint16_t*  exceptions_count,
	                          state<PT>& stt) {

		auto* out         = reinterpret_cast<UT*>(a_out);
		auto* right_parts = unffor_right_arr;
		auto* left_parts  = unffor_left_arr;

		// Decoding
		for (size_t i = 0; i < config::VECTOR_SIZE; i++) {
			uint16_t left  = stt.left_parts_dict[left_parts[i]];
			UT       right = right_parts[i];
			out[i]         = (static_cast<UT>(left) << stt.right_bit_width) | right;
		}

		// Exceptions Patching (exceptions only occur in left parts)
		auto exp_c = exceptions_count[0];
		for (size_t i = 0; i < exp_c; i++) {
			UT       right               = right_parts[exceptions_positions[i]];
			uint16_t left                = exceptions[i];
			out[exceptions_positions[i]] = (static_cast<UT>(left) << stt.right_bit_width) | right;
		}
	}

	static inline void
	init(const PT* data_column, size_t column_offset, size_t tuples_count, PT* sample_arr, state<PT>& stt) {
		stt.scheme           = Scheme::ALP_RD;
		stt.sampled_values_n = sampler::first_level_sample<PT>(data_column, column_offset, tuples_count, sample_arr);
		find_best_dictionary(sample_arr, stt);
	}
};

} // namespace alp

#pragma GCC diagnostic pop

#endif // ALP_RD_HPP
