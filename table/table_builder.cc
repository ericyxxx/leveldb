// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

//参考 https://izualzhy.cn/leveldb-sstable  & https://zhuanlan.zhihu.com/p/45693221

    // <beginning_of_file>
    // [data block 1]
    // [data block 2]
    // ...
    // [data block N]
    // [meta block 1] ---->>>> filter_block
    // ...
    // [meta block K]
    // [metaindex block]
    // [index block]
    // [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    // <end_of_file>
// 注：meta block按照 leveldb 的设计里，可能会有多种，filter block只是当前实现的一个。这也是为什么没有直接把 filter block 的 offset&size 写到 footer的原因。

// TableBuilder被用来实现生成 sstable

// data block/filter block/meta index block/index block，都按照|block_contents |compression_type |crc |这种格式组织，区别是 block_contents 格式不同
namespace leveldb {

//最终写sst的类 大致流程为
// auto tb = NewTableBuilder();
// tb.Add(key, val);
// tb.Add(key, val);
// tb.Flush();
// ...
// tb.Finish();
struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) { //默认第一次调用add不需要加索引
    index_block_options.block_restart_interval = 1;
    // 一开始把这个restart 设置为1
  }

  Options options;
  // data block index的选项
  // 实际上，这里可以节省空间的。因为有用的只有一个option.
  Options index_block_options;
  WritableFile* file;// 文件写出指针
  uint64_t offset;
  // 当前写的offset，写到哪了。
  // 注意：这个offset初始值为0，也就是说，
  // 它假设的是这个文件一开始的内容就是空的。
  // 或者说根本不关心当前文件的指针位置
  Status status;
  // 写的状态，之前的写入是否出错了？

   //data block&&index block都采用相同的格式，通过BlockBuilder完成
   //不过block_restart_interval参数不同
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  // 记录最后添加的key
  // 每个新来的key要进来的时候，都要与这个key进行比较，进而保证
  // 整体的顺序是有序的
  int64_t num_entries;  // 总共的条目数
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  // meta block
  // 由于meta block只有一个
  // 所以当写完meta block之后
  // 立即可以写入meta block index块
  // 这也就是为什么没有meta block index的原因。

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  // 只有切换到下一个data_block时才会写index_entry
  bool pending_index_entry;
  // 一开始并没有pending
  // 这个flag为true表明需要添加data block index item.
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  //  ???? 为什么这里设置options之后，restart_interval也被设置为1了。
  return Status::OK();
}

//             data block index：
//          | key | BlockHandle    |
//          | key | blockHandle    |
//          | offset1        |
//          | offset2        |
//          | number_of_items|
//          | compress type 1byte|
//          | crc32 4byte        |

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  // 一定还没有关闭
  // 用来确保使用者的调用顺序
  if (!ok()) return;
  // 如果当前的status已经不行了
  if (r->num_entries > 0) {
      // 如果已经添了加很多entry
      // 那么与最后一条进行比较
      // 一定要保证有序性
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  // 如果pending_index_entry == true
  // 当刚初始化好的时候，这个pending = false并不成立
  // 这里的意思是说，当需要生成一个新的data block的时候，
  // 这里需要生成一个新的data block index item.
  if (r->pending_index_entry) {
    assert(r->data_block.empty());// 此时data_block肯定为空
    r->options.comparator->FindShortestSeparator(&r->last_key, key);// 那么找一个能够分隔开的key
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);//刷写上一个block的index
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
    // 添加完data block index之后
    // 清空需要添加flag
  }

  if (r->filter_block != nullptr) {
     // 如果filter block不为空，那么就把这个key添加到filter里面。
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    // 如果预计的大小大于等于block_size
    // 那么就需要刷写了
    Flush();
  }
}

  //写入r->data_block到r->file
  //更新pending_handle: size为r->data_block的大小，offset为写入data_block前的offset
  //因此pending_handle可以定位一个完整的data_block
  //WriteBlock(&r->data_block, &r->pending_handle);
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  // 如果刷写成功，那么也需要刷写一次index.
  if (ok()) {
    // 由于data block index里面的item/entry里面的key是用来分隔两个block i / block i+1的。
    // 所以当第i个block要刷写到文件里面的时候，这个时候还并不能确定这个分隔的key的值。
    // 只能等到写i+1个data bock的时候，才可以确认。
    // 这里设置一个标志位，让i+1个block去刷写
    // 对应Add中对于pending_index_entry判断而产生的操作 
    // 全局只有FLASH会设置pending_index_entry = true
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    //更新filter
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
    // 如果没有压缩，这里直接就设置成raw格式
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      // 如果压缩率达到一定程度才用
      // 如果效果不好，那么就不用压缩了。
      // 所以这里需要根据压缩类型来选择
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
   // 直接写内存
  WriteRawBlock(block_contents, type, handle);
  // 清除压缩后的内存
  r->compressed_output.clear();
   // 重置block
  block->Reset();
}
// 其实就是从 block 取出数据，判断是否需要压缩，将最终结果调用WriteRawBlock.

// 判断是否压缩：
// 如果设置了kNoCompression，那么一定不压缩
// 如果设置了kSnappyCompression，那么尝试 snappy 压缩，
// 如果压缩后的大小小于原来的 87.5%，那么使用压缩后的值，否则也不压缩
// N个 data blocks, 1个 index block，1个 meta_index block，都使用这种方式写入，
// 也就是都采用BlockBuilder构造的数据组织格式，filter block的数据格式由FilterBlockBuilder构造。



// 这里真正的开始刷写Block的内存
// | block的内存       |
// | compress type 1B |
// | crc32 4 Byte     |
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 注意：这里去修改了pending_handle的值
  // 这个函数后面并没有再使用这个handle
  // 这个值被修改之后
  // 需要等到下一个block的第一个key被添加进来的时候，才会用到
  // ！！！！！所有的索引pending_handle 都是在block满了 刷盘的时候设置的 等下一次block写入时才同时写入index
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }


//     [data block 1]
//     [data block 2]
//     ...
//     [data block N]
//     [meta block]  <-- 只有一个
//     [metaindex block] <-- 只有一个
//     [index block]
//     [Footer]

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

// 放弃构建
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb


额外梳理下
// add -> add -> .... -> data_block满 ->Flush刷盘 ->设置写index_entry标记 -> add (设置上次的index_entry1)
//  -> add -> .... ->data_block满 -> flush刷盘(写index_entry1)

// 如果添加key到最后一个时，如果刚好满足Flush的条件。这个时候会直接把data block刷写到File里面。

// 虽然把pending_index_entry设置为true，但是由于后面并没有key被加进来。
// 所以，最后一个block是不会在data block index里面添加一个条目的。
// 那么也就是说n个data block对应的meta block里面的条目应该是n-1个。
// meta block里面的条目记录的都是block i, block i+1中间的间隔。

