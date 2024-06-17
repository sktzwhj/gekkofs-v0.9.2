We modify gekkofs to adapt to TH-express network through UCX support
To compile:
1. cd scripts/
2. run clean_up_compiled_deps.sh to clean up previously compilation files
3. run ./compile_dep.sh -p arm:latest ../deps-arm ../deps-arm-install/ to compile dependencies
4. cd .. && source env.sh
5. ./build.sh && cd build && make -j install
