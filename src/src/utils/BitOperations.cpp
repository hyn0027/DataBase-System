#include "utils/BitOperations.hpp"

namespace dbs {
namespace utils {

unsigned int int2bit32(int i) {
    unsigned int result;
    memcpy(&result, &i, sizeof(int));
    return result;
}

int bit322int(unsigned int b) {
    int result;
    memcpy(&result, &b, sizeof(int));
    return result;
}

void float2bit32(double f, unsigned int &b1, unsigned int &b2) {
    // save f into those 2 unsigned int
    unsigned int *intArray = reinterpret_cast<unsigned int *>(&f);
    b1 = intArray[0];
    b2 = intArray[1];
}
double bit322float(unsigned int b1, unsigned int b2) {
    // save those 2 unsigned int into f
    unsigned int intArray[2];
    intArray[0] = b1;
    intArray[1] = b2;
    double f = *reinterpret_cast<double *>(intArray);
    return f;
}

bool getBitFromNum(unsigned int b, int position) {
    assert(position >= 0 && position < BIT_PER_BUF);
    return (b >> position) & 1;
}

bool getBitFromBuf(BufType b, int position) {
    assert(position >= 0 && position < BIT_PER_PAGE);
    int offset = position >> LOG_BIT_PER_BUF;
    int bias = position & BIT_PER_BUF_MASK;
    return getBitFromNum(b[offset], bias);
}

void setBitFromNum(unsigned int &b, int position, bool value) {
    assert(position >= 0 && position < BIT_PER_BUF);
    if (value) {
        b |= (1 << position);
    } else {
        b &= ~(1 << position);
    }
}

void setBitFromBuf(BufType b, int position, bool value) {
    assert(position >= 0 && position < BIT_PER_PAGE);
    int offset = position >> LOG_BIT_PER_BUF;
    int bias = position & BIT_PER_BUF_MASK;
    setBitFromNum(b[offset], bias, value);
}

unsigned int get2Bytes(unsigned int b, int positon) {
    assert(positon == 0 || positon == 1);
    return (b >> (positon << 4)) & 0xffff;
}

void set2Bytes(unsigned int &b, int positon, unsigned int value) {
    assert(positon == 0 || positon == 1);
    b &= ~(0xffff << (positon << 4));
    b |= (value << (positon << 4));
}

char get1Byte(unsigned int b, int positon) {
    assert(positon == 0 || positon == 1 || positon == 2 || positon == 3);
    return (b >> (positon << 3)) & 0xff;
}

void set1Byte(unsigned int &b, int positon, char value) {
    assert(positon == 0 || positon == 1 || positon == 2 || positon == 3);
    b &= ~(0xff << (positon << 3));
    b |= (value << (positon << 3));
}

int getFirstZeroBit(unsigned int b) {
    for (int i = 0; i < BIT_PER_BUF; i++) {
        if (!getBitFromNum(b, i)) {
            return i;
        }
    }
    return -1;
}

}  // namespace utils
}  // namespace dbs