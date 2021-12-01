// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
//  参考 https://zhuanlan.zhihu.com/p/45829937

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  // 实质上就是指向Table::BlockReader()函数
  BlockFunction block_function_;
  // 使用的时候，就是指向Table
  void* arg_;
   // 读选项
  const ReadOptions options_;
  Status status_;
  // 第一级iter，由data block index:iterator
  IteratorWrapper index_iter_;
  // 第二级iter, 由data block: iterator
  // 为了快速访问，这里使用了lrucache
  // 即IteratorWrapper
  // 因为经常会访问data_iter_ key和valid
  IteratorWrapper data_iter_;  // May be nullptr
  // 如果data_iter_非空的时候，这个data_block_handle_才是有效的
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // data block的offset/size
  // 这里称之为句柄
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

//index_iter_.key()表示所在block所有key的最大值
// 所以目标值一定在index_iter_指向的data_block

//|  (index_iter_ - 1).key() | current block | index_iter_.key()

void TwoLevelIterator::Seek(const Slice& target) {
  // 先在 index block 找到第一个>= target 的k:v, v是某个data_block的size&offset
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
  // 如果走到了一个block的边缘，那么
  // 走到下一个非空block的开始位置去。
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

// 下面两个函数就是在检查一下data_iter_如果要走下一步的情况下，是否是需要设置到下一个非空的block的第一个iter那里。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // 如果data_iter为空 或者不为空，但是走到底了
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    // 移动到下一个data block index.
    index_iter_.Next();
    InitDataBlock();
      // 注意：生成iterator之后，并不代表着iterator已经存在于第一个item那里了
    // 有点类似于
    // std::vector<int>::iterator iter;
    // 声明之后，并不代表位于vector的.begin()位置。
    // 为了使用，这里还需要手动移一下到first.
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 当data_iter_走到一个block的边缘的时候，就需要走到下一个block的开始位置.
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  // data_iter_是一个iterator wrapper
  // 所以iter()会取到内部真正的iter
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  // 设置iter
  // Set函数会考虑原有的iter
  // 如果原有iter有指向一个block
  // 那么这里会把这个相应的内存释放掉
  data_iter_.Set(data_iter);
}
// 其实InitDataBlock这个函数名取得并不是特别好
// 取名叫GenerateSecondLevelIteratorFromIndexIterator()
// 这样更好，当然，名字太长了~
// 无论如何，我们知道这个函数的功能就是从一级iterator来生成二级
// iterator就可以了。
void TwoLevelIterator::InitDataBlock() {
  // 如果index_iter无效，那么设置data iter为nullptr.
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    // 取出offset/size
    Slice handle = index_iter_.value();
    // 如果已经指向这个block了
    // 那么就不用再操作了
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      // 这里直接生成一个新的iter
      // 注意：这里会去调用Table::BlockReader()!!
      // 然后生成一个新的Iter.
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      // 赋值新的offset/size
      // 并且会自动把原有的iter指向的内存释放掉
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
