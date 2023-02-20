// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/sm3.h"

namespace starrocks {
// IS LITTLE ENDIAN?
bool Sm3::is_little_endian = (*(char*)&Sm3::ENDIAN_TEST_VALUE == 1) ? true : false;

// Cyclic shift left k-bit operation
unsigned int Sm3::left_rotate(unsigned int word, int k) {
    k = k % WORD_BITS;
    if (k == 0) {
        return word;
    } else {
        return (word) << (k) | (word) >> (WORD_BITS - (k));
    }
}

// is_little_endian:true, reverse word because the word should stored in big endian format.
unsigned int* Sm3::reverse_word(unsigned int* word) {
    unsigned char *byte, temp;

    byte = (unsigned char*)word;
    temp = byte[0];
    byte[0] = byte[3];
    byte[3] = temp;

    temp = byte[1];
    byte[1] = byte[2];
    byte[2] = temp;
    return word;
}

// is_little_endian:true, reverse words(8 bytes) because the word should stored in big endian format.
unsigned long* Sm3::reverse_double_word(unsigned long* word) {
    unsigned char *byte, temp;

    byte = (unsigned char*)word;
    temp = byte[0];
    byte[0] = byte[7];
    byte[7] = temp;

    temp = byte[1];
    byte[1] = byte[6];
    byte[6] = temp;

    temp = byte[2];
    byte[2] = byte[5];
    byte[5] = temp;

    temp = byte[3];
    byte[3] = byte[4];
    byte[4] = temp;

    return word;
}

// It's defined by 4.2 of Sm3 algorithm
unsigned int Sm3::T(int i) {
    if (i >= 0 && i <= 15)
        return 0x79CC4519;
    else if (i >= 16 && i <= 63)
        return 0x7A879D8A;
    else
        return 0;
}

// It's defined by 4.3 of Sm3 algorithm
unsigned int Sm3::FF(unsigned int X, unsigned int Y, unsigned int Z, int i) {
    if (i >= 0 && i <= 15)
        return X ^ Y ^ Z;
    else if (i >= 16 && i <= 63)
        return (X & Y) | (X & Z) | (Y & Z);
    else
        return 0;
}

// It's defined by 4.3 of Sm3 algorithm
unsigned int Sm3::GG(unsigned int X, unsigned int Y, unsigned int Z, int i) {
    if (i >= 0 && i <= 15)
        return X ^ Y ^ Z;
    else if (i >= 16 && i <= 63)
        return (X & Y) | (~X & Z);
    else
        return 0;
}

// It's defined by 4.4 of Sm3 algorithm
unsigned int Sm3::P0(unsigned int X) {
    return X ^ Sm3::left_rotate(X, 9) ^ Sm3::left_rotate(X, 17);
}

// It's defined by 4.4 of Sm3 algorithm
unsigned int Sm3::P1(unsigned int X) {
    return X ^ Sm3::left_rotate(X, 15) ^ Sm3::left_rotate(X, 23);
}

void Sm3::init(Sm3Context* context) {
    // 0x7380166F - 0xB0FB0E4E is used as initial value from Sm3's definition.
    context->intermediate_hash[0] = 0x7380166F;
    context->intermediate_hash[1] = 0x4914B2B9;
    context->intermediate_hash[2] = 0x172442D7;
    context->intermediate_hash[3] = 0xDA8A0600;
    context->intermediate_hash[4] = 0xA96F30BC;
    context->intermediate_hash[5] = 0x163138AA;
    context->intermediate_hash[6] = 0xE38DEE4D;
    context->intermediate_hash[7] = 0xB0FB0E4E;
}

void Sm3::process_message_block(Sm3Context* context) {
    int i;
    unsigned int W[68];
    unsigned int W_[64];
    unsigned int A, B, C, D, E, F, G, H, SS1, SS2, TT1, TT2;

    // for every message_block(512 bits),
    // we copy values in message_block and use it as W[0-15],
    // W[0-15] is 16 words.
    for (i = 0; i < 16; i++) {
        W[i] = *(unsigned int*)(context->message_block + i * 4);
        // should modify input from little endian to big endian.
        if (Sm3::is_little_endian) {
            Sm3::reverse_word(W + i);
        }
    }

    // for W[16-67],
    // It is generated from W[0-15] with bit shift operation.
    // It's defined from 5.3.2 of sm3.
    for (i = 16; i < 68; i++) {
        W[i] = Sm3::P1(W[i - 16] ^ W[i - 9] ^ Sm3::left_rotate(W[i - 3], 15)) ^ Sm3::left_rotate(W[i - 13], 7) ^
               W[i - 6];
    }

    // for W_[0-63],
    // It is generated from W[0-67] with bit shift operation.
    // It's defined from 5.3.2 of sm3.
    for (i = 0; i < 64; i++) {
        W_[i] = W[i] ^ W[i + 4];
    }

    // get A-H from intermediate_hash.
    A = context->intermediate_hash[0];
    B = context->intermediate_hash[1];
    C = context->intermediate_hash[2];
    D = context->intermediate_hash[3];
    E = context->intermediate_hash[4];
    F = context->intermediate_hash[5];
    G = context->intermediate_hash[6];
    H = context->intermediate_hash[7];

    // We use W[0-67] and W_[0-63] to generate temporary value of (A-H).
    // It's defined at 5.3.3 of sm3.
    for (i = 0; i < 64; i++) {
        SS1 = Sm3::left_rotate((Sm3::left_rotate(A, 12) + E + Sm3::left_rotate(T(i), i)), 7);
        SS2 = SS1 ^ Sm3::left_rotate(A, 12);
        TT1 = Sm3::FF(A, B, C, i) + D + SS2 + W_[i];
        TT2 = Sm3::GG(E, F, G, i) + H + SS1 + W[i];
        D = C;
        C = Sm3::left_rotate(B, 9);
        B = A;
        A = TT1;
        H = G;
        G = Sm3::left_rotate(F, 19);
        F = E;
        E = Sm3::P0(TT2);
    }

    // update intermediate_hash[0-7] through A-H.
    context->intermediate_hash[0] ^= A;
    context->intermediate_hash[1] ^= B;
    context->intermediate_hash[2] ^= C;
    context->intermediate_hash[3] ^= D;
    context->intermediate_hash[4] ^= E;
    context->intermediate_hash[5] ^= F;
    context->intermediate_hash[6] ^= G;
    context->intermediate_hash[7] ^= H;
}

/*
 * message: bytes as input.
 * message_len: message' size.
 * digest: It's a allocated 256 bits(32 bytes) value.
 */
unsigned char* Sm3::sm3_compute(const unsigned char* message, unsigned long message_len,
                                unsigned char digest[SM3_HASH_SIZE]) {
    Sm3Context context;
    unsigned int i, remainder;
    unsigned long bit_len;
    Sm3::init(&context);

    // sm3 is processed based on block(512 bits) at a time.
    // so if message's size > 512 bits(64 bytes), we should compute based on
    // this blocks first.
    for (i = 0; i < message_len / 64; i++) {
        // copy 64 bytes into message_block.
        memcpy(context.message_block, message + i * 64, 64);
        // compute based on message_block and result in intermediate_hash[0-7].
        Sm3::process_message_block(&context);
    }

    // message's size as bits.
    bit_len = message_len * 8;

    // should modify input from little endian to big endian.
    if (is_little_endian) {
        Sm3::reverse_double_word(&bit_len);
    }
    remainder = message_len % 64;
    // copy the remaining bytes into message_block.
    memcpy(context.message_block, message + i * 64, remainder);

    // 0x80 Corresponding binary "10000000".
    // It's a step to populate message.
    context.message_block[remainder] = 0x80;

    // if remaining bytes is <= 55, because length of message need 8 bytes,
    // so total bytes is not exceeded 64 bytes, so we can process in one block.
    if (remainder <= WRAP_AROUND_THRESHOLD) {
        memset(context.message_block + remainder + 1, 0, 64 - remainder - 1 - 8);
        memcpy(context.message_block + 64 - 8, &bit_len, 8);
        Sm3::process_message_block(&context);
    } else {
        // else we need wrap around to append "bit_len",
        // so we process the front part first.
        memset(context.message_block + remainder + 1, 0, 64 - remainder - 1);
        Sm3::process_message_block(&context);

        // then we process the backend part that end with "bit_len".
        memset(context.message_block, 0, 64 - 8);
        memcpy(context.message_block + 64 - 8, &bit_len, 8);
        Sm3::process_message_block(&context);
    }

    // should modify result from little endian to big endian.
    if (is_little_endian) {
        for (i = 0; i < 8; i++) Sm3::reverse_word(context.intermediate_hash + i);
    }
    memcpy(digest, context.intermediate_hash, SM3_HASH_SIZE);

    return digest;
}

} // namespace starrocks
