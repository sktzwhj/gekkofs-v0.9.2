/*
  Copyright 2018-2022, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2022, Johannes Gutenberg Universitaet Mainz, Germany

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

#include <daemon/backend/metadata/merge.hpp>
#include <stdexcept>

using namespace std;

namespace gkfs::metadata {

string
MergeOperand::serialize_id() const {
    string s;
    s.reserve(2);
    s += (char) id(); // TODO check if static_cast can be used
    s += operand_id_suffix;
    return s;
}

string
MergeOperand::serialize() const {
    string s = serialize_id();
    s += serialize_params();
    return s;
}

OperandID
MergeOperand::get_id(const rdb::Slice& serialized_op) {
    return static_cast<OperandID>(serialized_op[0]);
}

rdb::Slice
MergeOperand::get_params(const rdb::Slice& serialized_op) {
    assert(serialized_op[1] == operand_id_suffix);
    return {serialized_op.data() + 2, serialized_op.size() - 2};
}

IncreaseSizeOperand::IncreaseSizeOperand(const size_t size)
    : size_(size), merge_id_(0), append_(false) {}

IncreaseSizeOperand::IncreaseSizeOperand(const size_t size,
                                         const uint16_t merge_id,
                                         const bool append)
    : size_(size), merge_id_(merge_id), append_(append) {}

IncreaseSizeOperand::IncreaseSizeOperand(const rdb::Slice& serialized_op) {
    size_t read = 0;
    // Parse size
    size_ = std::stoul(serialized_op.data(), &read);
    if(read + 1 == serialized_op.size() ||
       serialized_op[read] == serialize_end) {
        merge_id_ = 0;
        append_ = false;
        return;
    }
    assert(serialized_op[read] == serialize_sep);
    // Parse merge id
    merge_id_ = static_cast<uint16_t>(
            std::stoul(serialized_op.data() + read + 1, nullptr));
    append_ = true;
}

OperandID
IncreaseSizeOperand::id() const {
    return OperandID::increase_size;
}

string
IncreaseSizeOperand::serialize_params() const {
    // serialize_end avoids rogue characters in the serialized string
    if(append_)
        return fmt::format("{}{}{}{}", size_, serialize_sep, merge_id_,
                           serialize_end);
    else {
        return fmt::format("{}{}", size_, serialize_end);
    }
}


DecreaseSizeOperand::DecreaseSizeOperand(const size_t size) : size_(size) {}

DecreaseSizeOperand::DecreaseSizeOperand(const rdb::Slice& serialized_op) {
    // Parse size
    size_t read = 0;
    // we need to convert serialized_op to a string because it doesn't contain
    // the leading slash needed by stoul
    size_ = ::stoul(serialized_op.ToString(), &read);
    // check that we consumed all the input string
    assert(read == serialized_op.size());
}

OperandID
DecreaseSizeOperand::id() const {
    return OperandID::decrease_size;
}

string
DecreaseSizeOperand::serialize_params() const {
    return ::to_string(size_);
}


CreateOperand::CreateOperand(const string& metadata) : metadata(metadata) {}

OperandID
CreateOperand::id() const {
    return OperandID::create;
}

string
CreateOperand::serialize_params() const {
    return metadata;
}

/**
 * @internal
 * Merges all operands in chronological order for the same key.
 *
 * This is called before each Get() operation, among others. Therefore, it is
 * not possible to return a result for a specific merge operand. The return as
 * well as merge_out->new_value is for RocksDB internals The new value is the
 * merged value of multiple value that is written to one key.
 *
 * Append operations receive special treatment here as the corresponding write
 * function that triggered the size update needs the starting offset. In
 * parallel append operations this is crucial. This is done by accessing a mutex
 * protected std::map which may incur performance overheads for append
 * operations.
 * @endinternal
 */
bool
MetadataMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                   MergeOperationOutput* merge_out) const {

    string prev_md_value;
    auto ops_it = merge_in.operand_list.cbegin();
    if(merge_in.existing_value == nullptr) {
        // The key to operate on doesn't exists in DB
        if(MergeOperand::get_id(ops_it[0]) != OperandID::create) {
            throw ::runtime_error(
                    "Merge operation failed: key do not exists and first operand is not a creation");
        }
        prev_md_value = MergeOperand::get_params(ops_it[0]).ToString();
        ops_it++;
    } else {
        prev_md_value = merge_in.existing_value->ToString();
    }

    Metadata md{prev_md_value};

    size_t fsize = md.size();

    for(; ops_it != merge_in.operand_list.cend(); ++ops_it) {
        const rdb::Slice& serialized_op = *ops_it;
        assert(serialized_op.size() >= 2);
        auto operand_id = MergeOperand::get_id(serialized_op);
        auto parameters = MergeOperand::get_params(serialized_op);

        if constexpr(gkfs::config::metadata::use_mtime) {
            md.update_mtime_now();
        }

        if(operand_id == OperandID::increase_size) {
            auto op = IncreaseSizeOperand(parameters);
            if(op.append()) {
                auto curr_offset = fsize;
                // append mode, just increment file size
                fsize += op.size();
                // save the offset where this append operation should start
                // it is retrieved later in RocksDBBackend::increase_size_impl()
                GKFS_METADATA_MOD->append_offset_reserve_put(op.merge_id(),
                                                             curr_offset);
            } else {
                fsize = ::max(op.size(), fsize);
            }
        } else if(operand_id == OperandID::decrease_size) {
            auto op = DecreaseSizeOperand(parameters);
            assert(op.size() < fsize); // we assume no concurrency here
            fsize = op.size();
        } else if(operand_id == OperandID::create) {
            continue;
        } else {
            throw ::runtime_error("Unrecognized merge operand ID: " +
                                  (char) operand_id);
        }
    }

    md.size(fsize);
    merge_out->new_value = md.serialize();
    return true;
}

bool
MetadataMergeOperator::PartialMergeMulti(
        const rdb::Slice& key, const ::deque<rdb::Slice>& operand_list,
        string* new_value, rdb::Logger* logger) const {
    return false;
}

const char*
MetadataMergeOperator::Name() const {
    return "MetadataMergeOperator";
}

bool
MetadataMergeOperator::AllowSingleOperand() const {
    return true;
}

} // namespace gkfs::metadata
