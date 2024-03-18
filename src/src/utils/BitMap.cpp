#include "utils/BitMap.hpp"

namespace dbs{
namespace utils{

BitMap::BitMap(int _capacity, bool init_val) {
    capacity = _capacity;
    bit_map_size = (_capacity >> BIT_MAP_BIAS);
    data = new uint[bit_map_size];
    if (init_val == 1) 
        memset(data, 0xff, sizeof(uint) * bit_map_size);
    else
        memset(data, 0, sizeof(uint) * bit_map_size);
}

BitMap::~BitMap() {
    delete[] data;
}

void BitMap::setBit(int index, bool value) {
    assert(index < capacity);
    int pos, bit;
    getPos(index, pos, bit);
    uint mask = (1 << bit);
    uint w = data[pos] & (~mask);
    if (value)
        w |= mask;
    data[pos] = w;
}

bool BitMap::getBit(int index) {
    assert(index < capacity);
    int pos, bit;
    getPos(index, pos, bit);
    uint mask = (1 << bit);
    return (data[pos] & mask) != 0;
}

}  // namespace utils
}  // namespace dbs
