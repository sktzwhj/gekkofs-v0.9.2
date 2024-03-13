/*
  Copyright 2018-2020, Barcelona Supercomputing Center (BSC), Spain
  Copyright 2015-2020, Johannes Gutenberg Universitaet Mainz, Germany

  This software was partially supported by the
  EC H2020 funded project NEXTGenIO (Project ID: 671951, www.nextgenio.eu).

  This software was partially supported by the
  ADA-FS project under the SPPEXA project funded by the DFG.

  SPDX-License-Identifier: MIT
*/

#include <catch2/catch.hpp>
#include <fmt/format.h>
#include <common/rpc/distributor.hpp>
#include <fstream>

TEST_CASE( "Guided distributor Testing", "[Distributor]" ) {

    GIVEN( "A distributor" ) {
        // Generate a guided.txt that will put some files in
        // the server num 4
        std::ofstream o;
        o.open("/tmp/guided.txt");
        o << "/t.c01 0 3" << std::endl;
        o << "/t.c02 0 3" << std::endl;
        o << "/t.c01 1 3" << std::endl;
        o << "/t.c02 1 3" << std::endl;
        o << "/t.c01 2 3" << std::endl;
        o << "/t.c02 2 3" << std::endl;
        o << "/t.c03 1 3" << std::endl;
        o << "/t.c04 1 3" << std::endl;
        o << "/t.c05 1 3" << std::endl;
        o << "/t.c06 1 3" << std::endl;
        o << "/t.c07 1 3" << std::endl;
        o.close();

        // The distributor should return 3 for all the tested files
        auto d = gkfs::rpc::GuidedDistributor();

        REQUIRE( d.locate_data("/t.c01",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c02",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c03",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c04",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c05",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c06",1,10,0) == 3 );
        REQUIRE( d.locate_data("/t.c07",1,10,0) == 3 );

        // Next result is random, but with the same seed is consistent
        // We ask for chunk 5 that is distributed randomly between the
        // 10 servers.
        REQUIRE ( (d.locate_data("/t.c01",5,10,0) +
                  d.locate_data("/t.c02",5,10,0) +
                  d.locate_data("/t.c03",5,10,0) +
                  d.locate_data("/t.c04",5,10,0) +
                  d.locate_data("/t.c05",5,10,0) +
                  d.locate_data("/t.c06",5,10,0) +
                  d.locate_data("/t.c07",5,10,0) ) == 42);
    }
}
