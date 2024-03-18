#pragma once

namespace dbs {
namespace utils {

template <typename T>
struct LinkListNode {
    T data;
    LinkListNode<T>* next;
    LinkListNode<T>* prev;
    LinkListNode() : next(nullptr), prev(nullptr) {}
    LinkListNode(T _data) : data(_data), next(nullptr), prev(nullptr) {}
    ~LinkListNode() {
        next = nullptr;
        prev = nullptr;
    }
    void del() {
        if (prev != nullptr) prev->next = next;
        if (next != nullptr) next->prev = prev;
        delete this;
    }
};

template <typename T>
class LinkList {
   public:
    LinkList() : head(nullptr), tail(nullptr) {}
    ~LinkList() {
        LinkListNode<T>* p = head;
        while (p != nullptr) {
            LinkListNode<T>* q = p->next;
            delete p;
            p = q;
            q = nullptr;
        }
        head = tail = nullptr;
    }
    void insertHead(T data) {
        LinkListNode<T>* p = new LinkListNode<T>(data);
        if (head == nullptr) {
            head = tail = p;
        } else {
            p->next = head;
            head->prev = p;
            head = p;
        }
        p = nullptr;
    }
    void insertTail(T data) {
        LinkListNode<T>* p = new LinkListNode<T>(data);
        if (tail == nullptr) {
            head = tail = p;
        } else {
            p->prev = tail;
            tail->next = p;
            tail = p;
        }
        p = nullptr;
    }
    LinkListNode<T>* getHead() { return head; }
    LinkListNode<T>* getTail() { return tail; }
    LinkListNode<T>* find(T data) {
        LinkListNode<T>* p = head;
        while (p != nullptr) {
            if (p->data == data) return p;
            p = p->next;
        }
        return nullptr;
    }
    void del(LinkListNode<T>* p) {
        if (p == head) head = p->next;
        if (p == tail) tail = p->prev;
        p->del();
    }

   private:
    LinkListNode<T>* head;
    LinkListNode<T>* tail;
};

}  // namespace utils
}  // namespace dbs