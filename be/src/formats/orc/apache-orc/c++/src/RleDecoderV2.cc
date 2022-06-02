/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Adaptor.hh"
#include "Compression.hh"
#include "RLEV2Util.hh"
#include "RLEv2.hh"
namespace orc {

int64_t RleDecoderV2::readLongBE(uint64_t bsz) {
    int64_t ret = 0, val;
    uint64_t n = bsz;
    while (n > 0) {
        n--;
        val = readByte();
        ret |= (val << (n * 8));
    }
    return ret;
}

inline int64_t RleDecoderV2::readVslong() {
    return unZigZag(readVulong());
}

uint64_t RleDecoderV2::readVulong() {
    uint64_t ret = 0, b;
    uint64_t offset = 0;
    do {
        b = readByte();
        ret |= (0x7f & b) << offset;
        offset += 7;
    } while (b >= 0x80);
    return ret;
}

RleDecoderV2::RleDecoderV2(std::unique_ptr<SeekableInputStream> input, bool _isSigned, MemoryPool& pool,
                           DataBuffer<char>* _sharedBufferPtr)
        : inputStream(std::move(input)),
          isSigned(_isSigned),
          firstByte(0),
          runLength(0),
          runRead(0),
          bufferStart(nullptr),
          bufferEnd(bufferStart),
          deltaBase(0),
          byteSize(0),
          firstValue(0),
          prevValue(0),
          bitSize(0),
          bitsLeft(0),
          curByte(0),
          patchBitSize(0),
          unpackedIdx(0),
          patchIdx(0),
          base(0),
          curGap(0),
          curPatch(0),
          patchMask(0),
          actualGap(0),
          unpacked(pool, 0),
          unpackedPatch(pool, 0),
          direct(pool, 0),
          sharedBuffer(pool, 0) {
    // PASS
    if (_sharedBufferPtr != nullptr) {
        sharedBufferPtr = _sharedBufferPtr;
    }
}

void RleDecoderV2::seek(PositionProvider& location) {
    // move the input stream
    inputStream->seek(location);
    // clear state
    bufferEnd = bufferStart = nullptr;
    runRead = runLength = 0;
    // skip ahead the given number of records
    skip(location.next());
}

void RleDecoderV2::skip(uint64_t numValues) {
    // simple for now, until perf tests indicate something encoding specific is
    // needed
    const uint64_t N = 64;
    int64_t dummy[N];

    while (numValues) {
        uint64_t nRead = std::min(N, numValues);
        next(dummy, nRead, nullptr);
        numValues -= nRead;
    }
}

void RleDecoderV2::next(int64_t* const data, const uint64_t numValues, const char* const notNull) {
    uint64_t nRead = 0;

    while (nRead < numValues) {
        // Skip any nulls before attempting to read first byte.
        while (notNull && !notNull[nRead]) {
            if (++nRead == numValues) {
                return; // ended with null values
            }
        }

        if (runRead == runLength) {
            resetRun();
            firstByte = readByte();
        }

        uint64_t offset = nRead, length = numValues - nRead;

        EncodingType enc = static_cast<EncodingType>((firstByte >> 6) & 0x03);
        switch (static_cast<int64_t>(enc)) {
        case SHORT_REPEAT:
            nRead += nextShortRepeats(data, offset, length, notNull);
            break;
        case DIRECT:
            nRead += nextDirect(data, offset, length, notNull);
            break;
        case PATCHED_BASE:
            nRead += nextPatched(data, offset, length, notNull);
            break;
        case DELTA:
            nRead += nextDelta(data, offset, length, notNull);
            break;
        default:
            throw ParseError("unknown encoding");
        }
    }
}

uint64_t RleDecoderV2::nextShortRepeats(int64_t* const data, uint64_t offset, uint64_t numValues,
                                        const char* const notNull) {
    if (runRead == runLength) {
        // extract the number of fixed bytes
        byteSize = (firstByte >> 3) & 0x07;
        byteSize += 1;

        runLength = firstByte & 0x07;
        // run lengths values are stored only after MIN_REPEAT value is met
        runLength += MIN_REPEAT;
        runRead = 0;

        // read the repeated value which is store using fixed bytes
        firstValue = readLongBE(byteSize);

        if (isSigned) {
            firstValue = unZigZag(static_cast<uint64_t>(firstValue));
        }
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    if (notNull) {
        for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
            if (notNull[pos]) {
                data[pos] = firstValue;
                ++runRead;
            }
        }
    } else {
        for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
            data[pos] = firstValue;
            ++runRead;
        }
    }

    return nRead;
}

#define OPT_RLE_V2
#ifndef OPT_RLE_V2

uint64_t RleDecoderV2::nextDirect(int64_t* const data, uint64_t offset, uint64_t numValues, const char* const notNull) {
    if (runRead == runLength) {
        // extract the number of fixed bits
        unsigned char fbo = (firstByte >> 1) & 0x1f;
        bitSize = decodeBitWidth(fbo);

        // extract the run length
        runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
        runLength |= readByte();
        // runs are one off
        runLength += 1;
        runRead = 0;
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    runRead += readLongs(data, offset, nRead, bitSize, notNull);

    if (isSigned) {
        if (notNull) {
            for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
                if (notNull[pos]) {
                    data[pos] = unZigZag(static_cast<uint64_t>(data[pos]));
                }
            }
        } else {
            for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
                data[pos] = unZigZag(static_cast<uint64_t>(data[pos]));
            }
        }
    }

    return nRead;
}

#else

uint64_t RleDecoderV2::nextDirect(int64_t* const data, uint64_t offset, uint64_t numValues, const char* const notNull) {
    if (runRead == runLength) {
        // extract the number of fixed bits
        unsigned char fbo = (firstByte >> 1) & 0x1f;
        bitSize = decodeBitWidth(fbo);

        // extract the run length
        runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
        runLength |= readByte();
        // runs are one off
        runLength += 1;
        runRead = 0;

        direct.reserve(runLength);
        readLongsFully(direct.data(), runLength, bitSize);
        directIdx = 0;
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    if (isSigned) {
        if (notNull) {
            for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
                if (notNull[pos]) {
                    data[pos] = unZigZag(static_cast<uint64_t>(direct[directIdx]));
                    directIdx++;
                    runRead++;
                }
            }
        } else {
            runRead += nRead;
            for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
                data[pos] = unZigZag(static_cast<uint64_t>(direct[directIdx]));
                directIdx++;
            }
        }
    } else {
        if (notNull) {
            for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
                if (notNull[pos]) {
                    data[pos] = direct[directIdx];
                    directIdx++;
                    runRead++;
                }
            }
        } else {
            runRead += nRead;
            memcpy(data + offset, direct.data() + directIdx, sizeof(data[0]) * nRead);
            directIdx += nRead;
        }
    }

    return nRead;
}

#endif

uint64_t RleDecoderV2::nextPatched(int64_t* const data, uint64_t offset, uint64_t numValues,
                                   const char* const notNull) {
    if (runRead == runLength) {
        // extract the number of fixed bits
        unsigned char fbo = (firstByte >> 1) & 0x1f;
        bitSize = decodeBitWidth(fbo);

        // extract the run length
        runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
        runLength |= readByte();
        // runs are one off
        runLength += 1;
        runRead = 0;

        // extract the number of bytes occupied by base
        uint64_t thirdByte = readByte();
        byteSize = (thirdByte >> 5) & 0x07;
        // base width is one off
        byteSize += 1;

        // extract patch width
        uint32_t pwo = thirdByte & 0x1f;
        patchBitSize = decodeBitWidth(pwo);

        // read fourth byte and extract patch gap width
        uint64_t fourthByte = readByte();
        uint32_t pgw = (fourthByte >> 5) & 0x07;
        // patch gap width is one off
        pgw += 1;

        // extract the length of the patch list
        size_t pl = fourthByte & 0x1f;
        if (pl == 0) {
            throw ParseError("Corrupt PATCHED_BASE encoded data (pl==0)!");
        }

        // read the next base width number of bytes to extract base value
        base = readLongBE(byteSize);
        int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
        // if mask of base value is 1 then base is negative value else positive
        if ((base & mask) != 0) {
            base = base & ~mask;
            base = -base;
        }

        // TODO: something more efficient than resize
        unpacked.resize(runLength);
        unpackedIdx = 0;
        readLongs(unpacked.data(), 0, runLength, bitSize);
        // any remaining bits are thrown out
        resetReadLongs();

        // TODO: something more efficient than resize
        unpackedPatch.resize(pl);
        patchIdx = 0;
        // TODO: Skip corrupt?
        //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
        if ((patchBitSize + pgw) > 64) {
            throw ParseError(
                    "Corrupt PATCHED_BASE encoded data "
                    "(patchBitSize + pgw > 64)!");
        }
        uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
        readLongs(unpackedPatch.data(), 0, pl, cfb);
        // any remaining bits are thrown out
        resetReadLongs();

        // apply the patch directly when decoding the packed data
        patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

        adjustGapAndPatch();
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        // skip null positions
        if (notNull && !notNull[pos]) {
            continue;
        }
        if (static_cast<int64_t>(unpackedIdx) != actualGap) {
            // no patching required. add base to unpacked value to get final value
            data[pos] = base + unpacked[unpackedIdx];
        } else {
            // extract the patch value
            int64_t patchedVal = unpacked[unpackedIdx] | (curPatch << bitSize);

            // add base to patched value
            data[pos] = base + patchedVal;

            // increment the patch to point to next entry in patch list
            ++patchIdx;

            if (patchIdx < unpackedPatch.size()) {
                adjustGapAndPatch();

                // next gap is relative to the current gap
                actualGap += unpackedIdx;
            }
        }

        ++runRead;
        ++unpackedIdx;
    }

    return nRead;
}

uint64_t RleDecoderV2::nextDelta(int64_t* const data, uint64_t offset, uint64_t numValues, const char* const notNull) {
    if (runRead == runLength) {
        // extract the number of fixed bits
        unsigned char fbo = (firstByte >> 1) & 0x1f;
        if (fbo != 0) {
            bitSize = decodeBitWidth(fbo);
        } else {
            bitSize = 0;
        }

        // extract the run length
        runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
        runLength |= readByte();
        ++runLength; // account for first value
        runRead = deltaBase = 0;

        // read the first value stored as vint
        if (isSigned) {
            firstValue = static_cast<int64_t>(readVslong());
        } else {
            firstValue = static_cast<int64_t>(readVulong());
        }

        prevValue = firstValue;

        // read the fixed delta value stored as vint (deltas can be negative even
        // if all number are positive)
        deltaBase = static_cast<int64_t>(readVslong());
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    uint64_t pos = offset;
    for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (!notNull || notNull[pos]) break;
    }
    if (runRead == 0 && pos < offset + nRead) {
        data[pos++] = firstValue;
        ++runRead;
    }

    if (bitSize == 0) {
        // add fixed deltas to adjacent values
        for (; pos < offset + nRead; ++pos) {
            // skip null positions
            if (notNull && !notNull[pos]) {
                continue;
            }
            prevValue = data[pos] = prevValue + deltaBase;
            ++runRead;
        }
    } else {
        for (; pos < offset + nRead; ++pos) {
            // skip null positions
            if (!notNull || notNull[pos]) break;
        }
        if (runRead < 2 && pos < offset + nRead) {
            // add delta base and first value
            prevValue = data[pos++] = firstValue + deltaBase;
            ++runRead;
        }

        // write the unpacked values, add it to previous value and store final
        // value to result buffer. if the delta base value is negative then it
        // is a decreasing sequence else an increasing sequence
        uint64_t remaining = (offset + nRead) - pos;
        runRead += readLongs(data, pos, remaining, bitSize, notNull);

        if (deltaBase < 0) {
            for (; pos < offset + nRead; ++pos) {
                // skip null positions
                if (notNull && !notNull[pos]) {
                    continue;
                }
                prevValue = data[pos] = prevValue - data[pos];
            }
        } else {
            for (; pos < offset + nRead; ++pos) {
                // skip null positions
                if (notNull && !notNull[pos]) {
                    continue;
                }
                prevValue = data[pos] = prevValue + data[pos];
            }
        }
    }
    return nRead;
}

} // namespace orc
