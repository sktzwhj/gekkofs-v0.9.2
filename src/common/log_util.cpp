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

#include <common/log_util.hpp>

#include <spdlog/sinks/basic_file_sink.h>
#include <vector>
#include <algorithm>
#include <list>

using namespace std;

spdlog::level::level_enum
gkfs::log::get_level(string level_str) {
    char* parse_end;
    auto level = strtoul(level_str.c_str(), &parse_end, 10);
    if(parse_end != (level_str.c_str() + level_str.size())) {
        // no conversion could be performed. Must be a string then
        ::transform(level_str.begin(), level_str.end(), level_str.begin(),
                    ::tolower);
        if(level_str == "off"s)
            return spdlog::level::off;
        else if(level_str == "critical"s)
            return spdlog::level::critical;
        else if(level_str == "err"s)
            return spdlog::level::err;
        else if(level_str == "warn"s)
            return spdlog::level::warn;
        else if(level_str == "info"s)
            return spdlog::level::info;
        else if(level_str == "debug"s)
            return spdlog::level::debug;
        else if(level_str == "trace"s)
            return spdlog::level::trace;
        else
            throw runtime_error(fmt::format(
                    "Error: log level '{}' is invalid. Check help/readme.",
                    level_str));
    } else
        return get_level(level);
}

spdlog::level::level_enum
gkfs::log::get_level(unsigned long level) {
    switch(level) {
        case 0:
            return spdlog::level::off;
        case 1:
            return spdlog::level::critical;
        case 2:
            return spdlog::level::err;
        case 3:
            return spdlog::level::warn;
        case 4:
            return spdlog::level::info;
        case 5:
            return spdlog::level::debug;
        default:
            return spdlog::level::trace;
    }
}

void
gkfs::log::setup(const vector<string>& loggers_name,
                 spdlog::level::level_enum level, const string& path) {

    /* Create common sink */
    auto file_sink = make_shared<spdlog::sinks::basic_file_sink_mt>(path);

    /* Create and configure loggers */
    auto loggers = list<shared_ptr<spdlog::logger>>();
    for(const auto& name : loggers_name) {
        auto logger = make_shared<spdlog::logger>(name, file_sink);
        logger->flush_on(spdlog::level::trace);
        loggers.push_back(logger);
    }

    /* register loggers */
    for(const auto& logger : loggers) {
        spdlog::register_logger(logger);
    }

    // set logger format
    spdlog::set_pattern("[%C-%m-%d %H:%M:%S.%f] %P [%L][%n] %v");

    spdlog::set_level(level);
}
