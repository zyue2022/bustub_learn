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

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances,
                                                     uint32_t     instance_index,
                                                     DiskManager *disk_manager,
                                                     LogManager  *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
    BUSTUB_ASSERT(num_instances > 0,
                  "If BPI is not part of a pool, then the pool size should just be 1");
    BUSTUB_ASSERT(instance_index < num_instances,
                  "BPI index cannot be greater than the number of BPIs in the pool. In "
                  "non-parallel case, index should just be 1.");
    // We allocate a consecutive memory space for the buffer pool.
    pages_    = new Page[pool_size_];
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

/**
 * @description: 寻找pool里面的空闲位置
 * 	1. 如果pool未满，直接返回一个空闲frame位置，即空闲链表非空的时候
 *  2. pool满了，就是空闲链表为空，就需要执行LRU替换算法，
 * 	3. 替换过程是 ： victim函数找到一个淘汰的frame，
 * 					再由此frame找到对应于数据库的page，
 * 					如果是脏页还需写回磁盘，
 * 					同时重置引用计数，也删除umap中的映射关系
 * @param {frame_id_t} *frame_id
 * @return {*}
 */
auto BufferPoolManagerInstance::findFreeFrame(frame_id_t *frame_id) -> bool {
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }

    return replacer_->Victim(frame_id);
}

void BufferPoolManagerInstance::updatePage(Page *page, page_id_t new_page_id,
                                           frame_id_t new_frame_id) {
    // 1 如果是脏页，一定要写回磁盘，并且把dirty置为false
    if (page->IsDirty()) {
        disk_manager_->WritePage(page->page_id_, page->data_);
        page->is_dirty_ = false;
    }

    // 2 更新page table
    page_table_.erase(page->page_id_);     // 删除页表中原page_id和其对应frame_id
    if (new_page_id != INVALID_PAGE_ID) {  // 注意INVALID_PAGE_ID不要加到页表
        page_table_.emplace(new_page_id, new_frame_id);  // 新的page_id和其对应frame_id加到页表
    }

    // 3 重置page的data，更新page id
    page->ResetMemory();
    page->page_id_ = new_page_id;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
    // Make sure you call DiskManager::WritePage!
    latch_.lock();
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
        latch_.unlock();
        return false;
    }

    frame_id_t flush_fid = iter->second;
    Page      *page      = &pages_[flush_fid];
    disk_manager_->WritePage(page_id, page->data_);
    page->is_dirty_ = false;

    latch_.unlock();
    return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
    // You can do it!
    // std::unique_lock<std::mutex> lock(latch_);
    latch_.lock();
    for (size_t i = 0; i < pool_size_; i++) {
        // FlushPageImpl(i); // 这样写有问题，因为FlushPageImpl传入的参数是page id，其值可以>=pool size
        Page *page = &pages_[i];
        if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
            disk_manager_->WritePage(page->page_id_, page->data_);
            page->is_dirty_ = false;
        }
    }
    latch_.unlock();
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
    // 0.   Make sure you call AllocatePage!
    // 1.   If all the pages in the buffer pool are pinned, return nullptr.
    // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    // 4.   Set the page ID output parameter. Return a pointer to P.
	latch_.lock();
    page_id_t new_page_id = AllocatePage();

    bool is_all = true;
    for (size_t i = 0; i < pool_size_; i++) {
        if (pages_[i].pin_count_ == 0) {
            is_all = false;
            break;
        }
    }
    if (is_all) {
        latch_.unlock();
        return nullptr;
    }

    frame_id_t frame_id = -1;
    // 1 无法得到victim frame_id
    if (!findFreeFrame(&frame_id)) {
		latch_.unlock();
        return nullptr;
    }
    // 2 得到victim frame_id（从free_list或replacer中得到）
    *page_id   = new_page_id;        // 分配一个新的page_id（修改了外部参数*page_id）
    Page *page = &pages_[frame_id];  // 由frame_id得到page
    // pages_[frame_id]就是首地址偏移frame_id，左边的*page表示是一个指针指向那个地址，所以右边加&
    updatePage(page, *page_id, frame_id);
    replacer_->Pin(frame_id);  // FIX BUG in project2 checkpoint1（这里忘记pin了）
    page->pin_count_ = 1;

	latch_.unlock();
    return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
    // 1.     Search the page table for the requested page (P).
    // 1.1    If P exists, pin it and return it immediately.
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    // 2.     If R is dirty, write it back to the disk.
    // 3.     Delete R from the page table and insert P.
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    latch_.lock();
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        // 该页面在缓存池里面
        frame_id_t frame_id = it->second;
        Page      *page     = &pages_[frame_id];
        ++page->pin_count_;
        replacer_->Pin(frame_id);
        latch_.unlock();
        return page;
    }
    // 需要从空闲链表或满的缓冲区取，然后放入缓冲区
    frame_id_t replace_fid = -1;
    if (!findFreeFrame(&replace_fid)) {
        latch_.unlock();
        return nullptr;
    }
    // 处理page对象的旧内容
    Page *page = &pages_[replace_fid];
    updatePage(page, page_id, replace_fid);

    // 设置page对象的新内容
    disk_manager_->ReadPage(page_id, page->data_);
    replacer_->Pin(replace_fid);
    page->pin_count_ = 1;

	latch_.unlock();
    return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
    // 0.   Make sure you call DeallocatePage!
    // 1.   Search the page table for the requested page (P).
    // 1.   If P does not exist, return true.
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    // std::unique_lock<std::mutex> lock(latch_);
	latch_.lock();

    auto iter = page_table_.find(page_id);
    // 1 该page在页表中不存在
    if (iter == page_table_.end()) {
		latch_.unlock();
        return true;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page      *page     = &pages_[frame_id];  // 由frame_id得到page

    // the page still used by some thread, can not deleted(replaced)
    if (page->pin_count_ > 0) {
		latch_.unlock();
        return false;
    }

    DeallocatePage(page_id);  // This does not actually need to do anything for now
    updatePage(page, INVALID_PAGE_ID,
               frame_id);  // FIX BUG in project2 checkpoint2（此处不要把INVALID_PAGE_ID加到页表）
    page->pin_count_ = 0;
    free_list_.push_back(frame_id);  // 加到尾部

	latch_.unlock();
    return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
    latch_.lock();
    // 1. 如果page_table中就没有
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        latch_.unlock();
        return false;
    }
    // 2. 找到要被unpin的page
    frame_id_t unpinned_fid  = iter->second;
    Page      *unpinned_page = &pages_[unpinned_fid];
    if (is_dirty) {
        unpinned_page->is_dirty_ = true;
    }
    // if page的pin_count == 0 则直接return
    if (unpinned_page->pin_count_ == 0) {
        latch_.unlock();
        return false;
    }
    --unpinned_page->pin_count_;
    if (unpinned_page->GetPinCount() == 0) {
        replacer_->Unpin(unpinned_fid);
    }
    latch_.unlock();
    return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
    const page_id_t next_page_id = next_page_id_;
    next_page_id_ += num_instances_;
    ValidatePageId(next_page_id);
    return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
    assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
