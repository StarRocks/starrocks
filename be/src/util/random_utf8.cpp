#include "random_utf8.h"

namespace simdutf {
namespace tests {
namespace helpers {

random_utf8::random_utf8(uint32_t seed, int prob_1byte, int prob_2bytes,
                       int prob_3bytes, int prob_4bytes)
    : gen(seed), bytes_count({double(prob_1byte), double(prob_2bytes),
                              double(prob_3bytes), double(prob_4bytes)}) {}

std::pair<std::vector<uint8_t>,size_t> random_utf8::generate_counted(size_t output_bytes) {
  std::vector<uint8_t> result;
  result.reserve(output_bytes);
  uint8_t candidate, head;
  size_t count{0};
  while (result.size() < output_bytes) {
    count++;
    switch (bytes_count(gen)) {
    case 0: // 1 byte
      candidate = uint8_t(val_7bit(gen));
      while (candidate == 0) { // though strictly speaking, a stream of nulls is
                               // UTF8, it tends to break some code
        candidate = uint8_t(val_7bit(gen));
      }
      result.push_back(candidate);
      break;
    case 1: // 2 bytes
      candidate = 0xc0 | uint8_t(val_5bit(gen));
      while (candidate < 0xC2) {
        candidate = 0xc0 | uint8_t(val_5bit(gen));
      }
      result.push_back(candidate);
      result.push_back(0x80 | uint8_t(val_6bit(gen)));
      break;
    case 2: // 3 bytes
      head = 0xe0 | uint8_t(val_4bit(gen));
      result.push_back(head);
      candidate = 0x80 | uint8_t(val_6bit(gen));
      if (head == 0xE0) {
        while (candidate < 0xA0) {
          candidate = 0x80 | uint8_t(val_6bit(gen));
        }
      } else if (head == 0xED) {
        while (candidate > 0x9F) {
          candidate = 0x80 | uint8_t(val_6bit(gen));
        }
      }
      result.push_back(candidate);
      result.push_back(0x80 | uint8_t(val_6bit(gen)));
      break;
    case 3: // 4 bytes
      head = 0xf0 | uint8_t(val_3bit(gen));
      while (head > 0xF4) {
        head = 0xf0 | uint8_t(val_3bit(gen));
      }
      result.push_back(head);
      candidate = 0x80 | uint8_t(val_6bit(gen));
      if (head == 0xF0) {
        while (candidate < 0x90) {
          candidate = 0x80 | uint8_t(val_6bit(gen));
        }
      } else if (head == 0xF4) {
        while (candidate > 0x8F) {
          candidate = 0x80 | uint8_t(val_6bit(gen));
        }
      }
      result.push_back(candidate);
      result.push_back(0x80 | uint8_t(val_6bit(gen)));
      result.push_back(0x80 | uint8_t(val_6bit(gen)));
      break;
    }
  }
  result.push_back(0); // EOS for scalar code

  return make_pair(result,count);
}

std::vector<uint8_t> random_utf8::generate(size_t output_bytes) {
  return generate_counted(output_bytes).first;
}

std::vector<uint8_t> random_utf8::generate(size_t output_bytes, long seed) {
  gen.seed(uint32_t(seed));
  return generate(output_bytes);
}

} // namespace helpers
} // namespace tests
} // namespace simdutf
