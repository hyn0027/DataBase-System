cd build
# ulimit -m 262144
ctest
# ulimit -m unlimited
cd ../
mkdir -p lcoverage
current_directory=$(pwd)
YELLOW='\033[0;33m'
NC='\033[0m'
echo "${YELLOW}Processing coverage... Do not exit${NC}"
lcov --capture --directory ./build --output-file lcoverage/main_coverage.info > lcoverage/main_coverage.log
lcov --extract lcoverage/main_coverage.info \
"$current_directory/src/src/*" "$current_directory/src/include/*" "$current_directory/src/main.cpp" \
--output-file lcoverage/coverage_filtered.info > lcoverage/coverage_filtered.log
genhtml lcoverage/coverage_filtered.info --output-directory lcoverage > lcoverage/coverage.log