// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    // 析构的时候依次删除block列表
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  // 如果申请的大小 > 1/4 默认块大小
  // 则直接申请相应大小的block 防止内存浪费太严重
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // < 1/4 block size就直接申请一个块大小 然后指针偏移返回内存地址
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  // 修改剩余内存大小
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  // 修改申请后新内存块的空闲指针位置和空闲内存空间大小
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  // 计算对齐的位数
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  // 计算当前最新的空闲指针的对对齐位数的 mod值
  // 举个🌰  当前空闲指针位置是13
  // 如果要对8字节对齐 那么current_mod = 5 那么slop = 8 - 5 = 3
  // 也就是向后空出3个字节之后才是对齐的位置
  // needed = bytes + slop 也就是真正需要申请的空间
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    // 真正返回的起始地址需要加上slop
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    // 如果创建超过1/4 block size的 bytes大小也一定能保证内存对齐吗
    // 是的 直接从系统申请出来的内存块都是内存对齐的
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  // 申请内存块
  char* result = new char[block_bytes];
  // 新的块压到队尾
  blocks_.push_back(result);
  // 修改内存使用量
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
