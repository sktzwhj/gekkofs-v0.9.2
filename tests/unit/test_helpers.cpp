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

#include <catch2/catch.hpp>
#include "helpers/helpers.hpp"

SCENARIO("random strings can be generated", "[test_helpers][random_string]") {

    GIVEN("A desired length ") {

        WHEN("Length is zero ") {

            const auto s = helpers::random_string(0);

            THEN(" The generated string is empty ") {
                REQUIRE(s.empty());
                REQUIRE(s.length() == 0);
            }
        }

        WHEN("Length is a positive integer") {

            const std::size_t n = GENERATE(range(1, 5432));
            const auto s = helpers::random_string(n);

            THEN(" The generated string is empty ") {
                REQUIRE(s.length() == n);
            }
        }
    }
}

SCENARIO(" temporary directories can be created ",
         "[test_helpers][temporary_directory]") {

    GIVEN(" a temporary directory ") {

        auto tmpdir = std::make_unique<helpers::temporary_directory>();

        WHEN(" dirname is called ") {
            THEN(" a corresponding directory has been created ") {
                REQUIRE(fs::exists(tmpdir->dirname()));
            }
        }

        WHEN(" the temporary_directory has been destroyed ") {

            const auto dirname = tmpdir->dirname();
            tmpdir.reset();

            REQUIRE(!fs::exists(dirname));
        }
    }
}

SCENARIO(" temporary files can be created ", "[test_helpers][temporary_file]") {


    GIVEN(" a filename ") {
        const std::string filename{"foobar"};

        AND_GIVEN(" a temporary file ") {

            auto tmpfile = std::make_unique<helpers::temporary_file>(filename);

            WHEN(" a temporary_file is created ") {

                THEN(" a corresponding temporary file is created ") {
                    REQUIRE(tmpfile->filename() == filename);
                    REQUIRE(fs::exists(tmpfile->filename()));
                }
            }

            WHEN(" a temporary_file is destroyed ") {

                tmpfile.reset();

                THEN(" the file is removed ") {

                    REQUIRE(!fs::exists(filename));
                }
            }

            WHEN(" text data is written to the file ") {

                const std::string text{"sometext"};
                tmpfile->write(text);

                THEN(" its contents match the provided text ") {

                    std::string contents;
                    helpers::load_string_file(tmpfile->filename(), contents);

                    REQUIRE(contents == text);
                }
            }
        }

        AND_GIVEN(" some text for data ") {

            const std::string text{"sometext"};

            AND_GIVEN(" a temporary file ") {

                auto tmpfile = std::make_unique<helpers::temporary_file>(
                        filename, text);

                WHEN(" a temporary_file is created ") {

                    THEN(" a corresponding temporary file is created ") {
                        REQUIRE(tmpfile->filename() == filename);
                        REQUIRE(fs::exists(tmpfile->filename()));

                        AND_THEN(" its contents match the provided text ") {

                            std::string contents;
                            helpers::load_string_file(tmpfile->filename(),
                                                      contents);

                            REQUIRE(contents == text);
                        }
                    }
                }

                WHEN(" a temporary_file is destroyed ") {

                    tmpfile.reset();

                    THEN(" the file is removed ") {
                        tmpfile.reset();

                        REQUIRE(!fs::exists(filename));
                    }
                }

                WHEN(" text data is written to the file ") {

                    const std::string more_text{"moretext"};
                    tmpfile->write(more_text);

                    THEN(" its contents match the provided text ") {

                        std::string contents;
                        helpers::load_string_file(tmpfile->filename(), contents);

                        REQUIRE(contents == text + more_text);
                    }
                }
            }
        }
    }
}