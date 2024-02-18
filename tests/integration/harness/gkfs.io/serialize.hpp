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

#ifndef GKFS_IO_SERIALIZE_HPP
#define GKFS_IO_SERIALIZE_HPP

#include <nlohmann/json.hpp>
#include <reflection.hpp>

extern "C" {
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/statvfs.h>
}

template <typename T>
nlohmann::json
serialize(const T& object) {

    using json = nlohmann::json;

    json j;

    constexpr auto n = std::tuple_size<decltype(T::properties)>::value;

    refl::for_sequence(std::make_index_sequence<n>{}, [&](auto i) {
        constexpr auto p = std::get<i>(T::properties);

        // j[p.name] = json {
        //    { "type" , p.type },
        //    { "value" , object.*(p.member) }
        //};

        j[p.name] = object.*(p.member);
    });

    return j;
}

namespace nlohmann {

// ADL specialization for pointers to complete types that
// we want serialized
template <typename T>
struct adl_serializer<T*> {
    static void
    to_json(json& j, const T* opt) {
        if(opt) {
            j = *opt;
        } else {
            j = nullptr;
        }
    }
};

// ADL specialization for C strings
template <>
struct adl_serializer<const char*> {
    static void
    to_json(json& j, const char* opt) {
        if(opt) {
            j = std::string{opt};
        } else {
            j = std::string{};
        }
    }
};

// base serializer for opaque pointers
template <typename T>
struct opaque_ptr_serializer {
    static void
    to_json(json& j, const T opt) {
        if(opt) {
            j = reinterpret_cast<uint64_t>(opt);
        } else {
            j = nullptr;
        }
    }
};

// ADL specialization for opaque ::DIR* type
template <>
struct adl_serializer<::DIR*> : public opaque_ptr_serializer<::DIR*> {
    using opaque_ptr_serializer<::DIR*>::to_json;
};

// ADL specialization for void* type
template <>
struct adl_serializer<void*> : public opaque_ptr_serializer<void*> {
    using opaque_ptr_serializer<void*>::to_json;
};

// ADL specialization for struct ::timespec type
template <>
struct adl_serializer<struct ::timespec> {
    static void
    to_json(json& j, const struct ::timespec opt) {

        j = json{{"tv_sec", opt.tv_sec}, {"tv_nsec", opt.tv_nsec}};
    }
};

#ifdef STATX_TYPE
// ADL specialization for struct ::statx_timestamp type
template <>
struct adl_serializer<struct ::statx_timestamp> {
    static void
    to_json(json& j, const struct ::statx_timestamp opt) {

        j = json{{"tv_sec", opt.tv_sec}, {"tv_nsec", opt.tv_nsec}};
    }
};
#endif

// ADL specialization for struct ::dirent type
template <>
struct adl_serializer<struct ::dirent> {
    static void
    to_json(json& j, const struct ::dirent opt) {

        j = json{
                {"d_ino", opt.d_ino},       {"d_off", opt.d_off},
                {"d_reclen", opt.d_reclen}, {"d_type", opt.d_type},
                {"d_name", opt.d_name},
        };
    }
};

// std::ostream&
// operator<<(std::ostream& os, const struct ::dirent& d) {
//    return os << "hello there!\n";
//}

// ADL specialization for struct ::stat type
template <>
struct adl_serializer<struct ::stat> {
    static void
    to_json(json& j, const struct ::stat opt) {

        j = json{{"st_dev", opt.st_dev},         {"st_ino", opt.st_ino},
                 {"st_mode", opt.st_mode},       {"st_nlink", opt.st_nlink},
                 {"st_uid", opt.st_uid},         {"st_gid", opt.st_gid},
                 {"st_rdev", opt.st_rdev},       {"st_size", opt.st_size},
                 {"st_blksize", opt.st_blksize}, {"st_blocks", opt.st_blocks},
                 {"st_atim", opt.st_atim},       {"st_mtim", opt.st_mtim},
                 {"st_ctim", opt.st_ctim}};
    }
};

// ADL specialization for struct ::statfs (not exhaustive) type
template <>
struct adl_serializer<struct ::statfs> {
    static void
    to_json(json& j, const struct ::statfs opt) {
        j = json{{"f_type", opt.f_type},
        {"f_bsize", opt.f_bsize},
        {"f_blocks", opt.f_blocks},
        {"f_bfree", opt.f_bfree},
        {"f_bavail", opt.f_bavail},
        {"f_files", opt.f_files},
        {"f_ffree", opt.f_ffree}};
    }
};

#ifdef STATX_TYPE
// ADL specialization for struct ::statx type
template <>
struct adl_serializer<struct ::statx> {
    static void
    to_json(json& j, const struct ::statx opt) {

        j = json{{"stx_mask", opt.stx_mask},
                 {"stx_blksize", opt.stx_blksize},
                 {"stx_attributes", opt.stx_attributes},
                 {"stx_nlink", opt.stx_nlink},
                 {"stx_uid", opt.stx_uid},
                 {"stx_gid", opt.stx_gid},
                 {"stx_mode", opt.stx_mode},
                 {"stx_ino", opt.stx_ino},
                 {"stx_size", opt.stx_size},
                 {"stx_blocks", opt.stx_blocks},
                 {"stx_attributes_mask", opt.stx_attributes_mask},
                 {"stx_atime", opt.stx_atime},
                 {"stx_btime", opt.stx_btime},
                 {"stx_ctime", opt.stx_ctime},
                 {"stx_mtime", opt.stx_mtime},

                 {"stx_rdev_major", opt.stx_rdev_major},
                 {"stx_rdev_minor", opt.stx_rdev_minor},
                 {"stx_dev_major", opt.stx_dev_major},
                 {"stx_dev_minor", opt.stx_dev_minor}

        };
    }
};
#endif

} // namespace nlohmann

namespace fmt {

template <>
struct formatter<struct ::dirent> {
    constexpr auto
    parse(format_parse_context& ctx) {
        // [ctx.begin(), ctx.end()) is a character range that contains a part of
        // the format string starting from the format specifications to be
        // parsed, e.g. in
        //
        //   fmt::format("{:f} - point of interest", point{1, 2});
        //
        // the range will contain "f} - point of interest". The formatter should
        // parse specifiers until '}' or the end of the range. In this example
        // the formatter should parse the 'f' specifier and return an iterator
        // pointing to '}'.

        // Parse the presentation format and store it in the formatter:
        auto it = ctx.begin(), end = ctx.end();

        // Check if reached the end of the range:
        if(it != end && *it != '}')
            throw format_error("invalid format");

        // Return an iterator past the end of the parsed range:
        return it;
    }

    template <typename FormatContext>
    auto
    format(const struct ::dirent& dirent, FormatContext& ctx) {
        return format_to(ctx.out(),
                         "struct dirent {{\n"
                         "   d_ino = {};\n"
                         "   d_off = {};\n"
                         "   d_reclen = {};\n"
                         "   d_type = {};\n"
                         "   d_name = {};\n"
                         "}}",
                         dirent.d_ino, dirent.d_off, dirent.d_reclen,
                         dirent.d_type, dirent.d_name);
    }
};

} // namespace fmt

#endif // GKFS_IO_SERIALIZE_HPP
