# ulimit -m unlimited
# ulimit -s 16384

# mkdir -p lcoverage
mkdir -p build
cd build
cmake ..
cmake --build .