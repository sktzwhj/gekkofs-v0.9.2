# Coverage

This guide describes how to generate coverage reports for the GekkoFS 
source code. Please note that the GekkoFS CI pipelines already generate 
coverage reports automatically for newer commits. Thus, the procedures 
described here are focused towards developers willing to generate a coverage 
report in their local workstations to check whether their work is properly 
covered by tests.

## TLDR

```shell
# 1. configure appropriate coverage flags, etc.
$ cmake --preset coverage

# 2. build GekkoFS and co.
$ cmake --build builds/coverage --parallel 8

# 3. generate zero coverage information (builds/coverage/zerocount.info)
$ cmake --build builds/coverage --target coverage-zerocount

# 4. run ALL TESTS to generate coverage data files
$ ctest --test-dir builds/user-gcc-coverage --parallel 8

# 5. generate a unified LCOV trace file (builds/coverage/coverage.info)
$ cmake --build builds/coverage --target coverage-unified

# 6. (optional) print summary stats
$ cmake --build builds/coverage --target coverage-summary

# 7. (optional) generate a HTML report (builds/coverage/coverage_html)
$ cmake --build builds/coverage --target coverage-html

# 8. (optional) generate a Cobertura XML report (builds/coverage/coverage-cobertura.xml)
$ cmake --build builds/coverage --target coverage-cobertura
```

## Configuring the build

In order to generate coverage information, the source code needs to be 
compiled and linked in debug mode with (at least) the `--coverage` flags. 
While the flags can be set manually by overriding several 
variables such as `CMAKE_CXX_FLAGS`, since v3.19 supports 
[configuration presets](https://cmake.org/cmake/help/latest/manual/cmake-presets.7.html)
that greatly simplify this process. Thus, GekkoFS provides a CMake 
`default-coverage` preset that can be used to configure a coverage build:

```shell
$ cmake --preset=default-coverage
Preset CMake variables:

  CMAKE_BUILD_TYPE="Coverage"
  CMAKE_CXX_COMPILER="/usr/bin/g++"
  CMAKE_CXX_FLAGS="-Wall -Wextra -fdiagnostics-color=always --pedantic -Wno-unused-parameter -Wno-missing-field-initializers -DGKFS_DEBUG_BUILD -DHERMES_DEBUG_BUILD"
  CMAKE_CXX_FLAGS_COVERAGE="-Og -g --coverage -fkeep-static-functions"
  CMAKE_C_COMPILER="/usr/bin/gcc"
  CMAKE_C_FLAGS_COVERAGE="-Og -g --coverage -fkeep-static-functions"
  CMAKE_EXE_LINKER_FLAGS_COVERAGE="--coverage"
  CMAKE_MAP_IMPORTED_CONFIG_COVERAGE="Coverage;RelWithDebInfo;Release;Debug;"
  CMAKE_SHARED_LINKER_FLAGS_COVERAGE="--coverage"
  GKFS_CLIENT_LOG_MESSAGE_SIZE="512"
  GKFS_ENABLE_CLIENT_LOG:BOOL="TRUE"
  GKFS_BUILD_DOCUMENTATION:BOOL="TRUE"
  GKFS_BUILD_TESTS:BOOL="TRUE"
  GKFS_CHUNK_STATS:BOOL="TRUE"
  GKFS_ENABLE_PARALLAX:BOOL="FALSE"
  GKFS_ENABLE_PROMETHEUS:BOOL="TRUE"
  GKFS_ENABLE_ROCKSDB:BOOL="TRUE"
  GKFS_GENERATE_COVERAGE_REPORTS:BOOL="TRUE"
  GKFS_INSTALL_TESTS:BOOL="TRUE"
  SYMLINK_SUPPORT:BOOL="TRUE"

-- The C compiler identification is GNU 11.1.0
-- The CXX compiler identification is GNU 11.1.0
   [...]
```

## Generating a coverage report

GekkoFS relies on the `scripts/dev/coverage.py` script to manage all its 
coverage generation and reporting needs. The script internally calls
[`lcov`](https://github.com/linux-test-project/lcov) as needed to generate 
both intermediate and unified coverage traces, and 
[`lcov_cobertura`](https://pypi.org/project/lcov-cobertura/) to generate 
Cobertura XML reports.

As described by the `lcov` man page, the recommended procedure to generate 
coverage data for a test case is the following:

1. create baseline coverage data file
2. execute some tests to gather coverage data files
3. create a coverage data file for the tests
4. combine baseline and test coverage data

Since `coverage.py` is internally based on `lcov`, the script offers the 
following working modes:

- `capture`: Generate a `lcov` tracefile from existing coverage data. This 
  working mode is intended for generating initial zero coverage data as well 
  as coverage data from one or many tests.
- `merge`: Merge existing `lcov` tracefiles into a unified tracefile. This 
  working mode is intended to be used for merging several `lcov` tracefiles 
  into a unified tracefile. For instance, it can be used to merge the 
  tracefile for initial zero coverage with the traces for captured from 
  running tests.
- `summary`: Print summary coverage information for a specified `lcov` tracefile.
- `html_report`: Generate a HTML report for a specified `lcov` tracefile.
- `cobertura`: Generate a Cobertura XML report for a specified `lcov` tracefile.

Thus, in order to generate a coverage report for tests `test_directories` and 
`test_syscalls`, we can run the following commmands from the project's 
source directory:

```shell
SOURCE_DIR="$PWD"
BUILD_DIR="$SOURCE_DIR/build"
COVERAGE_DIR="$BUILD_DIR/coverage"

# generate a zero coverage tracefile while excluding any coverage data 
# referring to source files in $BUILD_DIR/_deps and $SOURCE_DIR/external
$ scripts/dev/coverage.py \
    --initial \
    --root-directory "$BUILD_DIR" \
    --output-file \
    "$COVERAGE_DIR/zerocount.info" \
    --sources-directory "$SOURCE_DIR" \
    --exclude "$BUILD_DIR/_deps/*" \
    --exclude "$SOURCE_DIR/external/*"
    
# execute the tests we are interested in
$ ctest --test-dir $BUILD_DIR -R test_directories
$ ctest --test-dir $BUILD_DIR -R test_syscalls

# generate a coverage tracefile for the generated coverage data while excluding 
# any coverage data referring to source files in $BUILD_DIR/_deps and 
# $SOURCE_DIR/external
$ scripts/dev/coverage.py \
    --root-directory "$BUILD_DIR" \
    --output-file \
    "$COVERAGE_DIR/directories_and_syscalls_tests.info" \
    --sources-directory "$SOURCE_DIR" \
    --exclude "$BUILD_DIR/_deps/*" \
    --exclude "$SOURCE_DIR/external/*"
    
# merge the generated tracefiles (i.e. $COVERAGE_DIR/zerocount.info and 
# $COVERAGE_DIR/directories_and_syscalls_tests.info) into a unified file
$ scripts/dev/coverage.py \
    merge \
    --output-file $COVERAGE_DIR/coverage.info \
    --search-pattern "$COVERAGE_DIR/*.info"
    
# print a summary, generate a HTML report, etc.
$ scripts/dev/coverage.py \
    summary \
    --input-tracefile $COVERAGE_DIR/coverage.info
Reading tracefile build/coverage/coverage.info
Summary coverage rate:
  lines......: 75.1% (6332 of 8430 lines)
  functions..: 83.2% (957 of 1150 functions)
  branches...: 42.1% (5987 of 14224 branches)
```

Since running all these steps manually is cumbersome and error-prone, GekkoFS' 
build script provides several targets to simplify report generation by 
automatically setting directories, exclusions, and so on:

- `coverage-zerocount`: Capture initial zero coverage data and write it to
  `${COVERAGE_ZEROCOUNT_TRACEFILE}`.
- `coverage-capture`: Capture coverage data from existing `.gcda` files and
  write it to `${COVERAGE_CAPTURE_TRACEFILE}`.
- `coverage-unified`: Merge any coverage data files found in
  `COVERAGE_OUTPUT_DIR` and generate a unified coverage trace.
- `coverage-summary`: Print a summary of the coverage data found in
  `${COVERAGE_UNIFIED_TRACEFILE}`.
- `coverage-html_report`: Write a HTML report from the coverage data
  found in `${COVERAGE_UNIFIED_TRACEFILE}`.
- `coverage-cobertura`: Write a Cobertura report from the coverage data
  found in `${COVERAGE_UNIFIED_TRACEFILE}`.

```{important}
It is possible to override the default values for all the `COVERAGE_` CMake 
variables mentioned above by setting them during CMake's project 
configuration, e.g. 

    `cmake --preset=default-coverage -DCOVERAGE_OUTPUT_DIR=/tmp`
```

The steps above can then be simplified:

```shell

# write a zero coverage tracefile in $COVERAGE_DIR/zerocount.info
# (with automatic exclusions)
$ cmake --build builds/coverage --target coverage-zerocount
[0/1] Generating zerocount coverage tracefile
Output written to '/home/user/gekkofs/build/coverage/zerocount.info'

# execute the tests we are interested in
$ ctest --test-dir $BUILD_DIR -R test_directories
$ ctest --test-dir $BUILD_DIR -R test_syscalls
 
# This executes the following:
# 1. write a coverage tracefile for the generated coverage data into
#    $COVERAGE_DIR/capture.info (with automatic exclusions)
# 2. merge $COVERAGE_DIR/zerocount.info with $COVERAGE_DIR/capture.info and 
#    write the result into $COVERAGE_DIR/coverage.info
# 3. print a stats report for $COVERAGE_DIR/capture.info
$ cmake --build $BUILD_DIR -t coverage-summary
[1/3] Generating capture coverage tracefile
Output written to '/home/user/gekkofs/source/build/coverage/capture.info'
[2/3] Generating unified coverage tracefile
Output written to '/home/user/gekkofs/source/build/coverage/coverage.info'
[3/3] Gathering coverage information
Reading tracefile /home/user/gekkofs/build/coverage/coverage.info
Summary coverage rate:
  lines......: 75.1% (6332 of 8430 lines)
  functions..: 83.2% (957 of 1150 functions)
  branches...: 42.1% (5987 of 14224 branches)
```
