#pragma once

#include <cstring>
#include <cassert>

#include "common/Config.hpp"

namespace dbs{
namespace utils{

class BitMap {
public:
    /**
     * @brief Construct a new Bit Map object
     * @param _capacity the capacity of the bit map
     * @param init_val the initial value of the bit map (0 or 1)
     */
	BitMap(int _capacity, bool init_val);
    /**
     * @brief Destroy the Bit Map object
    */
    ~BitMap();
    /**
     * @brief Set the Bit object
     * @param index the index of the bit to be set
     * @param value the value to be set (0 or 1)
    */
	void setBit(int index, bool value);
    /**
     * @brief Get the Bit object
     * @param index the index of the bit to be get
     * @return true if the bit is 1
     * @return false if the bit is 0
    */
    bool getBit(int index);

private:
	static void getPos(int index, int& pos, int& bit) {
		pos = (index >> BIT_MAP_BIAS);
		bit = index - (pos << BIT_MAP_BIAS);
	}
 
	uint* data;
	int capacity, bit_map_size;
};

}  // namespace utils
}  // namespace dbs
