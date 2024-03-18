#pragma once

#include "common/Config.hpp"
#include "utils/LinkList.hpp"
#include <type_traits>

namespace dbs{
namespace utils{

class HashItem {
public:
    virtual int hash() = 0;
    virtual ~HashItem() {}
};

class HashItemTwoInt : public HashItem {
public:
    int key1, key2;
    int val;
    HashItemTwoInt() : key1(0), key2(0), val(-1) {}
    HashItemTwoInt(int k1, int k2, int v) : key1(k1), key2(k2), val(v) {}
    HashItemTwoInt(int k1, int k2) : key1(k1), key2(k2), val(-1) {}
    int hash() {
        return (key1 * HASH_PRIME_1 + key2 * HASH_PRIME_2) % HASH_MOD;
    }
    friend bool operator==(const HashItemTwoInt& a, const HashItemTwoInt& b) {
        return a.key1 == b.key1 && a.key2 == b.key2;
    }
};

template <typename T>
class HashMap {
    // static_assert(std::is_base_of<HashItem, T>::value, "T must be derived from HashItem");
public:
    HashMap() {
        link_list = new LinkList<T>[HASH_MOD];
    }
    ~HashMap() {
        delete[] link_list;
    }
    void insert(T data)  {
        int hash = data.hash();
        link_list[hash].insertHead(data);
    }
    T find(T target)  {
        int hash = target.hash();
        LinkListNode<T>* p = link_list[hash].find(target);
        if (p == nullptr)
            return T();
        return p->data;
    }
    bool del(T target)  {
        int hash = target.hash();
        LinkListNode<T>* p = link_list[hash].find(target);
        if (p != nullptr) {
            link_list[hash].del(p);
            return true;
        }
        return false;
    }
private:
    LinkList<T>* link_list;
};

}  // namespace utils
}  // namespace dbs