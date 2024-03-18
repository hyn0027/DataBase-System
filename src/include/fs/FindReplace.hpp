#pragma once

#include "utils/LinkList.hpp"

namespace dbs {
namespace fs {

class FindReplace{
public:
    FindReplace(int capacity_);
    ~FindReplace();
    int find();
    void free(int index);
    void access(int index);

private:
    utils::LinkList<int> *list;
    utils::LinkListNode<int> **node;
    int capacity;
};

}  // namespace fs
}  // namespace dbs