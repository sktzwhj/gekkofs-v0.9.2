/*
  Copyright 2018-2024, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2024, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  This file is part of GekkoFS.

  GekkoFS is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  GekkoFS is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with GekkoFS.  If not, see <https://www.gnu.org/licenses/>.

  SPDX-License-Identifier: GPL-3.0-or-later
*/

#ifndef GKFS_COMMON_ARITHMETIC_HPP
#define GKFS_COMMON_ARITHMETIC_HPP

#include <cstdint>
#include <unistd.h>
#include <cassert>
#include <algorithm>
namespace gkfs::utils::arithmetic {

namespace cfg = gkfs::config::rpc;

/** 
 * --PFL implementation-- 
 * Used for locate component according to chunkId or offset
 * @param [in] container, PFL containner, PFLchunkID or PFLlayout
 * @param [in] x, chunkId or offset
 * @returns component number, from 0 to PFLcomponent -1
 */
template<typename Container>
typename Container::size_type last_smaller_equal(const Container& container, const typename Container::value_type& x) {
    auto it = std::upper_bound(container.begin(), container.end(), x);
    if (it == container.begin()) {
        return container.size() ;
    }
    return std::distance(container.begin(), it - 1);
}

/**
 * Check whether integer `n` is a power of 2.
 *
 * @param [in] n the number to check.
 * @returns `true` if `n` is a power of 2; `false` otherwise.
 */
constexpr bool
is_power_of_2(uint64_t n) {
    return n && (!(n & (n - 1u)));
}

/**
 * Compute the base2 logarithm for 64 bit integers.
 *
 * @param [in] n the number from which to compute the log2.
 * @returns the base 2 logarithm of `n`.
 */
constexpr std::size_t
log2(uint64_t n) {
    return 8u * sizeof(uint64_t) - __builtin_clzll(n) - 1;
}

/**
 * Check whether @n is aligned to a block boundary, i.e. if it is divisible by
 * @block_size.
 *
 * @note This function assumes that block_size is a power of 2.
 *
 * @param [in] n the number to check.
 * @param [in] block_size
 * @returns true if @n is divisible by @block_size; false otherwise.
 */
constexpr bool
is_aligned(const uint64_t n, const size_t block_size) {
    using gkfs::utils::arithmetic::log2;
    assert(is_power_of_2(block_size));
    return !(n & ((1u << log2(block_size)) - 1));
}

/**
 * Given a file @offset and a @block_size, align the @offset to its
 * closest left-side block boundary.
 *
 * @note This function assumes that block_size is a power of 2.
 *
 * @param [in] offset the offset to align.
 * @param [in] block_size the block size used to compute boundaries.
 * @returns an offset aligned to the left-side block boundary.
 */
constexpr uint64_t
align_left(const uint64_t offset, const size_t block_size) {
    // This check is automatically removed in release builds
    assert(is_power_of_2(block_size));
    return offset & ~(block_size - 1u);
}


/**
 * Given a file @offset and a @block_size, align the @offset to its
 * closest right-side block boundary.
 *
 * @note This function assumes that block_size is a power of 2.
 *
 * @param [in] offset the offset to align.
 * @param [in] block_size the block size used to compute boundaries.
 * @returns an offset aligned to the right-side block boundary.
 */
constexpr uint64_t
align_right(const uint64_t offset, const size_t block_size) {
    // This check is automatically removed in release builds
    assert(is_power_of_2(block_size));
    return align_left(offset, block_size) + block_size;
}


/**
 * Return the overrun bytes that separate @offset from the closest left side
 * block boundary.
 *
 * @note This function assumes that block_size is a power of 2.
 *
 * @param [in] offset the offset for which the overrun distance should be
 * computed.
 * @param [in] block_size the block size used to compute boundaries.
 * @returns the distance in bytes between the left-side boundary of @offset
 */
inline size_t
block_overrun(uint64_t offset, size_t block_size) {
    /* --PFL implementation-- 
    * reset offset and blocksize to those of component
    * offset = offset at current component
    * blocksize = stripe size at current component */
    if(cfg::use_PFL){
        unsigned int cpn = last_smaller_equal(cfg::PFLlayout, offset); //component number
        offset = offset - cfg::PFLlayout[cpn];
        block_size = cfg::PFLsize[cpn];
    }
    /* --PFL implementation-- */
    assert(is_power_of_2(block_size));
    return offset & (block_size - 1u);
}


/**
 * Return the underrun bytes that separate @offset from the closest right side
 * block boundary.
 *
 * @note This function assumes that block_size is a power of 2.
 *
 * @param [in] offset the offset for which the overrun distance should be
 * computed.
 * @param [in] block_size the block size used to compute boundaries.
 * @returns the distance in bytes between the right-side boundary of @offset
 */
inline size_t
block_underrun(uint64_t offset, size_t block_size) {
    /* --PFL implementation-- 
    * reset offset and blocksize to those of component
    * offset = offset at current component
    * blocksize = stripe size at current component */
    if(cfg::use_PFL){
        unsigned int cpn = last_smaller_equal(cfg::PFLlayout, offset); //component number
        offset = offset - cfg::PFLlayout[cpn];
        block_size = cfg::PFLsize[cpn];
    }
    if(is_aligned(offset, block_size))
        return 0;
    assert(is_power_of_2(block_size));
    return align_right(offset, block_size) - offset;
}


/**
 * Given an @offset and a @block_size, compute the block index to which @offset
 * belongs.
 *
 * @note Block indexes are (conceptually) computed by dividing @offset
 * by @block_size, with index 0 referring to block [0, block_size - 1],
 * index 1 to block [block_size, 2 * block_size - 1], and so on up to
 * a maximum index FILE_LENGTH / block_size.
 *
 * @note This function assumes that @block_size is a power of 2.
 *
 * @param [in] offset the offset for which the block index should be computed.
 * @param [in] block_size the block_size that should be used to compute the
 * index.
 * @returns the index of the block containing @offset.
 */
inline uint64_t
block_index(uint64_t offset, size_t block_size) {

    using gkfs::utils::arithmetic::log2;
    unsigned long long prefix = 0;
    /* --PFL implementation-- 
    * reset offset and blocksize to those of component
    * offset = offset at current component
    * blocksize = stripe size at current component 
    * prefix should revise this reset
    * */
    if(cfg::use_PFL){
        //component number
        unsigned int cpn = last_smaller_equal(cfg::PFLlayout, offset); 
        offset = offset - cfg::PFLlayout[cpn];
        block_size = cfg::PFLsize[cpn];
        prefix = cfg::PFLchunkID[cpn];
    }
    /* --PFL implementation-- */

    // This check is automatically removed in release builds
    assert(is_power_of_2(block_size));
    return (align_left(offset, block_size) >> log2(block_size)) + prefix;
}


/**
 * --PFL implementation-- This func seems no use in real situations.
 * Compute the number of blocks involved in an operation affecting the
 * regions from [@offset, to @offset + @count).
 *
 * @note This function assumes that @block_size is a power of 2.
 * @note This function assumes that @offset + @count does not
 * overflow.
 *
 * @param [in] offset the operation's initial offset.
 * @param [in] size the number of bytes affected by the operation.
 * @param [in] block_size the block size that should be used to compute the
 * number of blocks.
 * @returns the number of blocks affected by the operation.
 */
constexpr std::size_t
block_count(const uint64_t offset, const size_t size, const size_t block_size) {

    using gkfs::utils::arithmetic::log2;

    // These checks are automatically removed in release builds
    assert(is_power_of_2(block_size));

#if defined(__GNUC__) && !defined(__clang__)
    assert(!__builtin_add_overflow_p(offset, size, uint64_t{0}));
#else
    assert(offset + size > offset);
#endif

    const uint64_t first_block = align_left(offset, block_size);
    const uint64_t final_block = align_left(offset + size, block_size);
    const size_t mask = -!!size; // this is either 0 or ~0

    return (((final_block >> log2(block_size)) -
             (first_block >> log2(block_size)) +
             !is_aligned(offset + size, block_size))) &
           mask;
}

} // namespace gkfs::utils::arithmetic

#endif // GKFS_COMMON_ARITHMETIC_HPP