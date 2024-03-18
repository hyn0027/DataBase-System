#include "fs/FindReplace.hpp"

namespace dbs {
namespace fs {

FindReplace::FindReplace(int capacity_) {
    capacity = capacity_;
    list = new utils::LinkList<int>();
    node = new utils::LinkListNode<int>*[capacity];
    for (int i = 0; i < capacity; i++) {
        list->insertHead(i);
        node[i] = list->getHead();
    }
}

FindReplace::~FindReplace() {
    delete list;
    for (int i = 0; i < capacity; i++)
        node[i] = nullptr;
    delete[] node;
}

int FindReplace::find() {
    int index = list->getTail()->data;
    list->del(node[index]);
    node[index] = nullptr;
    list->insertHead(index);
    node[index] = list->getHead();
    return index;
}

void FindReplace::access(int index) {
    list->del(node[index]);
    node[index] = nullptr;
    list->insertHead(index);
    node[index] = list->getHead();
}

void FindReplace::free(int index) {
    list->del(node[index]);
    node[index] = nullptr;
    list->insertTail(index);
    node[index] = list->getTail();
}

}  // namespace fs
}  // namespace dbs