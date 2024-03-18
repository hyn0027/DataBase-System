cmake_minimum_required(VERSION 3.10)
project(dbs_project_19)

# set(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
# set(GCC_COVERAGE_LINK_FLAGS "-lgcov")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall ${GCC_COVERAGE_COMPILE_FLAGS}")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

# include(FetchContent)
# FetchContent_Declare(
#   googletest
#   URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
# )

# set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# FetchContent_MakeAvailable(googletest)

include_directories(
  src/include
  src/antlr4
)

file(
  GLOB_RECURSE
  LIBS
  "src/src/*.cpp"
  "src/include/*/*.hpp"
)

file(
  GLOB_RECURSE
  ANTLR4_SOURCES
  "src/antlr4/*.cpp"
)

add_library(antlr4_lib STATIC ${ANTLR4_SOURCES})
add_executable(main src/main.cpp ${LIBS})
target_link_libraries(main antlr4_lib)

# enable_testing()

# add_executable(
#   unit_test
#   test/fs_test.cc
#   test/record_test.cc
#   test/index_test.cc
#   test/interpreter_test.cc
#   ${LIBS}
# )

# target_link_libraries(unit_test antlr4_lib GTest::gtest_main)

# include(GoogleTest)
# gtest_discover_tests(unit_test)


set_target_properties(main PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
