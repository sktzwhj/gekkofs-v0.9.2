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

#ifndef GKFS_IO_REFLECTION_HPP
#define GKFS_IO_REFLECTION_HPP

#include <nlohmann/json.hpp>
#include <fmt/format.h>
#include <boost/preprocessor.hpp>

extern "C" {
#include <dirent.h> // required by DIR*
}

namespace refl {
namespace detail {

template <typename Class, typename T>
struct property_impl {
    constexpr property_impl(T Class::*aMember, const char* aType,
                            const char* aName)
        : member{aMember}, type{aType}, name{aName} {}

    using Type = T;

    T Class::*member;
    const char* type;
    const char* name;
};

} // namespace detail

template <typename Class, typename T>
constexpr auto
property(T Class::*member, const char* type, const char* name) {
    return detail::property_impl<Class, T>{member, type, name};
}

template <typename T, T... S, typename F>
constexpr void
for_sequence(std::integer_sequence<T, S...>, F&& f) {
    using unpack_t = int[];
    (void) unpack_t{
            (static_cast<void>(f(std::integral_constant<T, S>{})), 0)..., 0};
}

} // namespace refl


/* private helper macros, do not call directly */
#define _REFL_STRUCT_NAME(t)         BOOST_PP_TUPLE_ELEM(0, t)
#define _REFL_STRUCT_MEMBER_COUNT(t) BOOST_PP_TUPLE_ELEM(1, t)
#define _REFL_MEMBER_TYPE(t)         BOOST_PP_TUPLE_ELEM(0, t)
#define _REFL_MEMBER_NAME(t)         BOOST_PP_TUPLE_ELEM(1, t)

#define _REFL_GEN_FIELD(r, data, index, elem)                                  \
    refl::property(                                                            \
            &_REFL_STRUCT_NAME(data)::_REFL_MEMBER_NAME(elem),                 \
            BOOST_PP_STRINGIZE(_REFL_MEMBER_TYPE(elem)),                       \
                               BOOST_PP_STRINGIZE(_REFL_MEMBER_NAME(elem)))    \
                    BOOST_PP_COMMA_IF(BOOST_PP_NOT_EQUAL(                      \
                            index,                                             \
                            BOOST_PP_DEC(_REFL_STRUCT_MEMBER_COUNT(data))))

/* public interface */
#define REFL_DECL_MEMBER(type, name) (type, name)

#define REFL_DECL_STRUCT(struct_name, ...)                                     \
    constexpr static auto properties =                                         \
            std::make_tuple(BOOST_PP_SEQ_FOR_EACH_I(                           \
                    _REFL_GEN_FIELD,                                           \
                    (struct_name, BOOST_PP_VARIADIC_SIZE(__VA_ARGS__)),        \
                    BOOST_PP_TUPLE_TO_SEQ((__VA_ARGS__))));                    \
    static_assert(true, "")

#endif // GKFS_IO_REFLECTION_HPP
