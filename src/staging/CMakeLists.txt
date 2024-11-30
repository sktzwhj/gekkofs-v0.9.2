find_package(MPI REQUIRED)
set(CMAKE_CXX_COMPILER mpic++)
add_executable(stage-in)
add_executable(stage-out)
# We need to add here any files that may have different compile definitions
target_sources(
  stage-in
  PRIVATE src/stage-in.cpp
  PUBLIC ${CMAKE_SOURCE_DIR}/include/config.hpp
         ${CMAKE_SOURCE_DIR}/include/version.hpp.in
         ${CMAKE_SOURCE_DIR}/include/common/arithmetic/arithmetic.hpp
)
target_sources(
  stage-out
  PRIVATE src/stage-out.cpp
  PUBLIC ${CMAKE_SOURCE_DIR}/include/config.hpp
         ${CMAKE_SOURCE_DIR}/include/version.hpp.in
         ${CMAKE_SOURCE_DIR}/include/common/arithmetic/arithmetic.hpp
)


target_link_libraries(stage-in PUBLIC MPI::MPI_CXX)
target_link_libraries(stage-out PUBLIC MPI::MPI_CXX)

find_package(Threads REQUIRED)
target_link_libraries(stage-in PRIVATE Threads::Threads)
target_link_libraries(stage-out PRIVATE Threads::Threads)

# 添加一个自定义目标来构建 Go 可执行文件
add_custom_target(build-go
    COMMAND go build -o ${CMAKE_SOURCE_DIR}/install/bin/tools ${CMAKE_SOURCE_DIR}/src/staging/main.go
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/staging
    COMMENT "Building Go application"
)

# 添加一个伪目标来表示生成的 Go 工具
#add_executable(tools IMPORTED)
#set_property(TARGET tools PROPERTY IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/tools)

install(TARGETS stage-in RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(TARGETS stage-out RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
#install(TARGETS tools RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# 指定配置文件路径
set(CLUSH_CONFIG_FILES ./clush.conf)
set(TOOLS_CONFIG_FILES ./config.yaml)
# 安装配置文件
install(FILES ${CLUSH_CONFIG_FILES} DESTINATION ${CMAKE_SOURCE_DIR}/install/bin/)
install(FILES ${TOOLS_CONFIG_FILES} DESTINATION ${CMAKE_SOURCE_DIR}/install/bin/)
