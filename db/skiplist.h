// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...
// 一个线程安全的跳表实现
// 写操作需要加锁 读操作需要确保跳表没有被销毁(c++的世界也太容易内存异常了 智能指针用起来啊)
// leveldb里很多用了ref unref的方式来实现了引用计数确保内存被异常销毁(cache)
// 读操作不需要额外的同步动作
// 跳表中的节点不会被单独销毁 只会随着跳表对象销毁而销毁节点
// 跳表中的节点在被插入之后除了prev/next指针内容都是只读的
// 只有Insert函数会操作list, 小心初始化节点并且使用release-store内存序去发布一个节点

// todo 关于c++ atomic变量内存序的解释

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

// 声明内存池类型
class Arena;

template <typename Key, class Comparator>
class SkipList {
 private:
  // 节点类型 具体实现在下面
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  // arena 内存池对象 skiplist所有节点都是在arena中申请的
  // cmp 类似于java的Comparator接口
  explicit SkipList(Comparator cmp, Arena* arena);

  // 不准拷贝构造
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // 插入操作
  // 跳表中不允许存在重复元素
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  // 判断跳表中是否存在某个元素
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  // 跳表迭代器
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    // 找到第一个比target大的key
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    // 存储跳表指针
    const SkipList* list_;
    // 存储当前便利到的node
    Node* node_;
    // Intentionally copyable
  };

 private:
  // 跳表最高12层
  enum { kMaxHeight = 12 };

  // 获取当前跳表的高度
  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  // 创建一个node 传入key 以及这个节点的高度
  Node* NewNode(const Key& key, int height);
  // 随意生成一个高度
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  // 返回 key之后最先的节点
  // 如果prev指针是非空的 那就在prev中存储最先节点的各个层级的前置节点
  // 如下图所示 如果我们要在下面的跳表找到7的GreaterOrEqual
  // 那么首先我们会找到节点 8
  // 然后我们会填充prev数组 里面分别是14666
  // 1----------------------->8
  // 1------------->4-------->8
  // 1-------->3--->4--->6--->8
  // 1-------->3--->4--->6--->8
  // 1--->2--->3--->4--->6--->8
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // 找到小于key的最大节点
  // 套用上面的例子找7的FindLessThan 就是节点6
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  // 找到跳表的最后一个节点
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  // 跳表头结点
  // 使用的带链表头的链表方式 这样在处理第一个元素的时候不需要特殊处理
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  // 跳表的最大高度
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  // 随机种子
  Random rnd_;
};

// Implementation details follow
// 跳表Node的详细实现
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  // 初始化之后就不能改了哦
  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  // 这里牵涉到两种atomic的内存序 memory_order_acquire/memory_order_release
  // todo 后续补齐
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  // 指向下一层的指针列表 创建Node的时候回根据height决定申请相应的后续内存
  // c语言的老技巧了
  // 为什么要用原子变量封起来呢 防止多线程读写的时候发生冲突
  // 用了原子变量 那读写skiplist还需要加锁吗
  // 虽然写skiplist是需要上锁的 但是读不需要所以为了防止读写动作同时发生从而产生冲突所以用
  // 原子变量维护了这个next_数组
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // 从指定内存区块创建Node对象
  return new (node_memory) Node(key);
}

// 迭代器构造函数
template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  // 只返回第0层的元素
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  // 并没有存prev指针 需要从前往后索引
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  // 最高kMaxHeight
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 查找key节点的后继节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  // 从顶层开始
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      // key比node小或者key也不是nullptr
      x = next;
    } else {
      // 如果prev不是nullptr 说明需要传prev数组回去
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        // 到底了 返回节点
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

// 找到比指定key小的最大Node
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  // 从最上面那层开始找
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      // 如果next是空指针(遍历到头了)或者next的值比key大(第一次到这个分支, 说明x就是这层那个要找的值)
      if (level == 0) {
        // 到最下面那层了 说明确切值找到了
        return x;
      } else {
        // Switch to next list
        // 往下走一楼
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 从最高的那一层开始往后遍历
// 碰到队尾了 上一个节点楼层减一继续往下 直到到0层
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    // 头结点初始化next指针拉满
    head_->SetNext(i, nullptr);
  }
}

// 核心啊 插入操作
// 我们以下面的跳表插入7为例
// h
// ... 5个h
// h--->1---------------------------->9
// h--->1----------------------->8--->9
// h--->1------------->4-------->8--->9
// h--->1-------->3--->4--->6--->8--->9
// h--->1-------->3--->4--->6--->8--->9
// h--->1--->2--->3--->4--->6--->8--->9
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  // 找到适合插入的前置节点 并且找到所有指向这个节点的prev指针组
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);
  // 此时x = 8
  // prev = 66641

  // Our data structure does not allow duplicate insertion
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  // 假设我们height为7
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      // 设置新的楼层的前置节点为head_
      prev[i] = head_;
    }
    // 此时我们的prev数组为 666411h

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    // 第一步设置新节点的后继 取prev[i]的后继
    // h
    // h                            7
    // h                            7-------->9
    // h--->1--------------------   7--->8--->9
    // h--->1------------->4-----   7--->8--->9
    // h--->1-------->3--->4--->6   7--->8--->9
    // h--->1-------->3--->4--->6   7--->8--->9
    // h--->1--->2--->3--->4--->6   7--->8--->9
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));

    // 第二步设置指向7号的前驱节点的后继为7
    // h
    // h----------------------------7
    // h--->1-----------------------7-------->9
    // h--->1-----------------------7--->8--->9
    // h--->1------------->4--------7--->8--->9
    // h--->1-------->3--->4--->6---7--->8--->9
    // h--->1-------->3--->4--->6---7--->8--->9
    // h--->1--->2--->3--->4--->6---7--->8--->9
    prev[i]->SetNext(i, x);
  }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
