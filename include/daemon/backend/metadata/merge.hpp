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

#ifndef DB_MERGE_HPP
#define DB_MERGE_HPP

#include <daemon/backend/metadata/metadata_module.hpp>
#include <rocksdb/merge_operator.h>
#include <common/metadata.hpp>

namespace rdb = rocksdb;

namespace gkfs::metadata {

/**
 * @brief Merge operator classifiers
 */
enum class OperandID : char {
    increase_size = 'i',
    decrease_size = 'd',
    create = 'c'
};

/**
 * @brief Base class for merge operands
 */
class MergeOperand {
public:
    constexpr static char operand_id_suffix = ':';

    std::string
    serialize() const;

    static OperandID
    get_id(const rdb::Slice& serialized_op);

    static rdb::Slice
    get_params(const rdb::Slice& serialized_op);

protected:
    std::string
    serialize_id() const;

    virtual std::string
    serialize_params() const = 0;

    virtual OperandID
    id() const = 0;
};
/**
 * @brief Increase size operand
 */
class IncreaseSizeOperand : public MergeOperand {
private:
    constexpr const static char serialize_sep = ',';
    constexpr const static char serialize_end = '\0';

    size_t size_;
    /*
     * ID of the merge operation that this operand belongs to.
     * This ID is used only in append operations to communicate the starting
     * write offset from the asynchronous Merge operation back to the caller in
     * `increase_size_impl()`.
     */
    uint16_t merge_id_;
    bool append_;

public:
    IncreaseSizeOperand(size_t size);

    IncreaseSizeOperand(size_t size, uint16_t merge_id, bool append);

    explicit IncreaseSizeOperand(const rdb::Slice& serialized_op);

    OperandID
    id() const override;

    std::string
    serialize_params() const override;

    size_t
    size() const {
        return size_;
    }

    uint16_t
    merge_id() const {
        return merge_id_;
    }

    bool
    append() const {
        return append_;
    }
};
/**
 * @brief Decrease size operand
 */
class DecreaseSizeOperand : public MergeOperand {
private:
    size_t size_;

public:
    explicit DecreaseSizeOperand(size_t size);

    explicit DecreaseSizeOperand(const rdb::Slice& serialized_op);

    OperandID
    id() const override;

    std::string
    serialize_params() const override;

    size_t
    size() const {
        return size_;
    }
};
/**
 * @brief Create operand
 */
class CreateOperand : public MergeOperand {
public:
    std::string metadata;

    explicit CreateOperand(const std::string& metadata);

    OperandID
    id() const override;

    std::string
    serialize_params() const override;
};
/**
 * @brief Merge operator class passed to RocksDB, used during merge operations
 */
class MetadataMergeOperator : public rocksdb::MergeOperator {
public:
    ~MetadataMergeOperator() override = default;

    /**
     * @brief Merges all operands in chronological order for the same key
     * @param op1 Input operand
     * @param op2 Output operand
     * @return Result of the merge operation
     */
    bool
    FullMergeV2(const MergeOperationInput& merge_in,
                MergeOperationOutput* merge_out) const override;

    /**
     * @brief TODO functionality unclear. Currently unused.
     * @param key
     * @param operand_list
     * @param new_value
     * @param logger
     * @return
     */
    bool
    PartialMergeMulti(const rdb::Slice& key,
                      const std::deque<rdb::Slice>& operand_list,
                      std::string* new_value,
                      rdb::Logger* logger) const override;

    /**
     * @brief Returns the name of this Merge operator
     * @return
     */
    const char*
    Name() const override;

    /**
     * @brief Merge Operator configuration which allows merges with just a
     * single operand.
     * @return
     */
    bool
    AllowSingleOperand() const override;
};

} // namespace gkfs::metadata

#endif // DB_MERGE_HPP
