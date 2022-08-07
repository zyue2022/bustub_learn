//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FindFreeFrame(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  return replacer_->Victim(frame_id);
}

void BufferPoolManagerInstance::UpdatePage(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
  // 1 如果是脏页，一定要写回磁盘，并且把dirty置为false
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }

  // 2 更新page table
  page_table_.erase(page->page_id_);                 // 删除页表中原page_id和其对应frame_id
  if (new_page_id != INVALID_PAGE_ID) {              // 注意INVALID_PAGE_ID不要加到页表
    page_table_.emplace(new_page_id, new_frame_id);  // 新的page_id和其对应frame_id加到页表
  }

  // 3 重置page的data，更新page id
  page->ResetMemory();
  page->page_id_ = new_page_id;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock{latch_};

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t flush_fid = it->second;
  Page *page = &pages_[flush_fid];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock{latch_};

  for (size_t fid = 0; fid < pool_size_; ++fid) {
    Page *page = &pages_[fid];
    page_id_t pid = page->page_id_;
    if (pid != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(pid, page->data_);
      page->is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};

  bool is_all = true;
  for (size_t fid = 0; fid < pool_size_; ++fid) {
    if (pages_[fid].pin_count_ == 0) {
      is_all = false;
      break;
    }
  }
  if (is_all) {
    return nullptr;
  }

  frame_id_t frame_id = INVALID_PAGE_ID;
  // 1 无法得到victim frame_id
  if (!FindFreeFrame(&frame_id)) {
    return nullptr;
  }
  assert(frame_id != INVALID_PAGE_ID);
  // 2 得到victim frame_id（从free_list或replacer中得到）
  *page_id = AllocatePage();       // 分配一个新的page_id（修改了外部参数*page_id）
  Page *page = &pages_[frame_id];  // 由frame_id得到page,此时还是旧页
  UpdatePage(page, *page_id, frame_id);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;

  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 该页面在缓存池里面
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    ++page->pin_count_;
    replacer_->Pin(frame_id);
    return page;
  }

  // 需要从空闲链表或满的缓冲区取，然后放入缓冲区
  frame_id_t replace_fid = INVALID_PAGE_ID;
  if (!FindFreeFrame(&replace_fid)) {
    return nullptr;
  }
  assert(replace_fid != INVALID_PAGE_ID);

  // 处理page对象的旧内容
  Page *page = &pages_[replace_fid];
  UpdatePage(page, page_id, replace_fid);

  // 设置page对象的新内容
  disk_manager_->ReadPage(page_id, page->data_);
  replacer_->Pin(replace_fid);
  page->pin_count_ = 1;

  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};

  auto it = page_table_.find(page_id);
  // 1 该page在内存中不存在，就是在磁盘，返回true
  if (it == page_table_.end()) {
    return true;
  }

  // 2 该page在页表中存在
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  // 3 有线程还在使用，不能删除
  if (page->pin_count_ > 0) {
    return false;
  }

  // 4 从内存清除该页
  DeallocatePage(page_id);                      // This does not actually need to do anything for now
  UpdatePage(page, INVALID_PAGE_ID, frame_id);  // 虽然是无效页id，但更新函数中不会添加到哈希表
  page->pin_count_ = 0;
  free_list_.push_back(frame_id);  // 加到空闲帧位置链表的尾部

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};

  // 1. 如果不在page_table中，就是不在内存中，就不能加到lru，返回false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  // 2. 找到要被unpin的page
  frame_id_t unpinned_fid = it->second;
  Page *unpinned_page = &pages_[unpinned_fid];
  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }

  // 3. 没有线程使用该页，放入lru，返回true
  if (unpinned_page->pin_count_ == 0) {
    replacer_->Unpin(unpinned_fid);
    return true;
  }

  // 4. 减小引用计数，并判断是否可以放入lru
  --unpinned_page->pin_count_;
  if (unpinned_page->pin_count_ == 0) {
    replacer_->Unpin(unpinned_fid);
  }

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
