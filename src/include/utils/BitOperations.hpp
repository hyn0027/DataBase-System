#pragma once

#include <cassert>
#include <cstring>

#include "common/Config.hpp"

namespace dbs {
namespace utils {

/**
 * @brief convert 32 bits to int
 * @param i
 * @return
 */
unsigned int int2bit32(int i);

/**
 * @brief convert int to 32 bits
 *
 * @param b
 * @return int
 */
int bit322int(unsigned int b);

/**
 * @brief convert float to 32 bits
 *
 * @param f
 * @return unsigned int
 */
void float2bit32(double f, unsigned int &b1, unsigned int &b2);

/**
 * @brief convert 32 bits to float
 *
 * @param b
 * @return float
 */
double bit322float(unsigned int b1, unsigned int b2);

/**
 * @brief Get the Bit object
 *
 * @param b = BIT_PER_BUF = 32 bits
 * @param position = 0 ~ 31
 * @return true
 * @return false
 */
bool getBitFromNum(unsigned int b, int position);

/**
 * @brief Get the value of b at position.
 *
 * @param b = 0 ~ BIT_PER_PAGE - 1 bits
 * @param position which bit (0 ~ 8192 * 8 - 1)
 * @return true
 * @return false
 */
bool getBitFromBuf(BufType b, int position);

/**
 * @brief set b at position.
 *
 * @param b
 * @param position
 * @param value
 */
void setBitFromNum(unsigned int &b, int position, bool value);

/**
 * @brief Set the Bit object
 *
 * @param b = 0 ~ BIT_PER_PAGE - 1 bits
 * @param position which bit (0 ~ 8192 * 8 - 1)
 * @param value
 */
void setBitFromBuf(BufType b, int position, bool value);

/**
 * @brief get the first 2 bytes (position = 0) or second 2 bytes (position = 1)
 *
 * @param b 4 bytes, 32 bits
 * @param positon 0 or 1
 * @return int
 */
unsigned int get2Bytes(unsigned int b, int positon);

/**
 * @brief set the first 2 bytes (position = 0) or second 2 bytes (position = 1)
 *
 * @param b
 * @param positon
 * @param value
 */
void set2Bytes(unsigned int &b, int positon, unsigned int value);

/**
 * @brief get the position-th byte (position = 0 ~ 3)
 *
 * @param b
 * @param positon (0 ~ 3)
 * @return char
 */
char get1Byte(unsigned int b, int positon);

/**
 * @brief set the position-th byte (position = 0 ~ 3)
 *
 * @param b
 * @param positon
 * @param value
 */
void set1Byte(unsigned int &b, int positon, char value);

/**
 * @brief Get the First Zero Bit object
 *
 * @param b
 * @return int
 */
int getFirstZeroBit(unsigned int b);

}  // namespace utils
}  // namespace dbs