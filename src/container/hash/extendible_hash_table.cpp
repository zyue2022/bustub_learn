//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

/**
 * @description: 在构造函数分配目录页和第一个桶
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  // 分配目录页
  directory_page_id_ = INVALID_PAGE_ID;
  Page *dir_page_raw = buffer_pool_manager_->NewPage(&directory_page_id_);
  assert(directory_page_id_ != INVALID_PAGE_ID);
  assert(dir_page_raw != nullptr);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  dir_page->SetPageId(directory_page_id_);

  // 分配第一个桶页
  page_id_t buc_page_id = INVALID_PAGE_ID;
  Page *buc_page_raw = buffer_pool_manager_->NewPage(&buc_page_id);
  assert(buc_page_id != INVALID_PAGE_ID);
  assert(buc_page_raw != nullptr);
  dir_page->SetBucketPageId(0, buc_page_id);  // 索引是0

  // 记得unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, true));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

/**
 * @description: 找到某键值对应的桶
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

/**
 * @description: 这儿不要unpin，只是取出，还没有使用；由使用目录页的函数用完后unpin
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *dir_page_raw = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(dir_page_raw != nullptr);
  return reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
}
/**
 * @description: fetch目录页和桶页的返回类型有些不同，因为是模板，也不unpin
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t buc_page_id) {
  assert(buc_page_id != INVALID_PAGE_ID);
  Page *buc_page_raw = buffer_pool_manager_->FetchPage(buc_page_id);
  assert(buc_page_raw != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buc_page_raw->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // 全局哈希表加读锁
  table_latch_.RLock();  // Readers includes inserts and removes
  // 找到目录页，从而获取对应桶
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);
  // 桶页加读锁
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->RLatch();
  bool ok = buc_page->GetValue(key, comparator_, result);
  buc_page_raw->RUnlatch();
  // 记得unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
  table_latch_.RUnlock();
  return ok;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @description: 插入数据，有容量就简单插入，无容量就困难的扩容
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 全局哈希表加读锁
  table_latch_.RLock();
  // 找到目录页，从而获取对应桶
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);
  // 桶页加写锁了
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->WLatch();
  // 该桶还有容量
  if (!buc_page->IsFull()) {
    bool ok = buc_page->Insert(key, value, comparator_);
    buc_page_raw->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(buc_page_id, true));
    table_latch_.RUnlock();
    return ok;
  }
  // 目标桶容量不足，需要分裂
  buc_page_raw->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

/**
 * @description: lab2最难的函数了...，扩容后再尝试插入
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 全局哈希表加写锁，分裂时目录页和桶页都可能变化
  table_latch_.WLock();  // writers are splits and merges
  // 同样每次先取得目录页和桶页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);

  // 遇到buc_local_depth == global_depth，还需要扩目录页
  uint32_t buc_idx = KeyToDirectoryIndex(key, dir_page);  // 获取当前桶在目录页的索引，不是桶页id
  if (dir_page->GetLocalDepth(buc_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 先增加local_depth,这一步很关键，想想逻辑
  dir_page->IncrLocalDepth(buc_idx);

  // 取出原来 bucket的信息，先加锁
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->WLatch();
  uint32_t num = buc_page->NumReadable();
  MappingType *old_pairs_arr = buc_page->FetchAllMappingType();
  buc_page->ResetBucketPage();

  // 创建一个新的bucket页面
  page_id_t image_buc_page_id = INVALID_PAGE_ID;
  Page *image_buc_page_raw = buffer_pool_manager_->NewPage(&image_buc_page_id);
  assert(image_buc_page_id != INVALID_PAGE_ID);
  assert(image_buc_page_raw != nullptr);

  // 往新页面转移数据
  image_buc_page_raw->WLatch();
  auto image_buc_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(image_buc_page_raw);
  uint32_t image_buc_idx = dir_page->GetSplitImageIndex(buc_idx);
  dir_page->SetLocalDepth(image_buc_idx, dir_page->GetLocalDepth(buc_idx));
  dir_page->SetBucketPageId(image_buc_idx, image_buc_page_id);
  for (uint32_t i = 0; i < num; ++i) {
    // 相当于 key to 桶id，用localmask是保证数据只能落在原桶或镜像桶
    uint32_t new_buc_idx = Hash(old_pairs_arr[i].first) & dir_page->GetLocalDepthMask(buc_idx);
    page_id_t temp_page_id = dir_page->GetBucketPageId(new_buc_idx);
    assert(temp_page_id == buc_page_id || temp_page_id == image_buc_page_id);
    if (temp_page_id == buc_page_id) {
      buc_page->Insert(old_pairs_arr[i].first, old_pairs_arr[i].second, comparator_);
    } else {
      image_buc_page->Insert(old_pairs_arr[i].first, old_pairs_arr[i].second, comparator_);
    }
  }
  delete[] old_pairs_arr;

  // 不懂？？？？？？？？？？？？？？？？？？？？？？？？
  // 上面只修改了原bucket与image_bucket的相关信息，
  // 实际上可能之前存在许多bucket映射到bucket对应的page上,这些信息也要相应的修改
  uint32_t step = 1 << (dir_page->GetLocalDepth(buc_idx));
  for (uint32_t i = buc_idx; i >= step; i -= step) {
    dir_page->SetBucketPageId(i, buc_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(buc_idx));
  }
  for (uint32_t i = buc_idx; i < dir_page->Size(); i += step) {
    dir_page->SetBucketPageId(i, buc_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(buc_idx));
  }
  for (uint32_t i = image_buc_idx; i >= step; i -= step) {
    dir_page->SetBucketPageId(i, image_buc_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_buc_idx));
  }
  for (uint32_t i = image_buc_idx; i < dir_page->Size(); i += step) {
    dir_page->SetBucketPageId(i, image_buc_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_buc_idx));
  }

  buc_page_raw->WUnlatch();
  image_buc_page_raw->WUnlatch();

  // Unpin
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_buc_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  table_latch_.WUnlock();

  // 再次尝试插入数据
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 同样先哈希表的全局读锁
  table_latch_.RLock();  // Readers includes inserts and removes
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);

  // 桶上写锁
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->WLatch();
  bool ok = buc_page->Remove(key, value, comparator_);

  // 如果当前bucket空了，则执行合并
  if (buc_page->IsEmpty()) {
    buc_page_raw->WUnlatch();
    buffer_pool_manager_->UnpinPage(buc_page_id, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return ok;
  }
  buc_page_raw->WUnlatch();
  buffer_pool_manager_->UnpinPage(buc_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return ok;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 合并就需要加哈希表的写锁了
  table_latch_.WLock();  // writers are splits and merges

  // 同样先获得目录页、桶页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t buc_page_id = dir_page->GetBucketPageId(buc_idx);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);

  // 桶在目录的索引，不是页id
  uint32_t buc_idx = KeyToDirectoryIndex(key, dir_page); 
  uint32_t image_buc_idx = dir_page->GetSplitImageIndex(buc_idx);

  // local depth为0说明已经最小了，不收缩
  if (dir_page->GetLocalDepth(buc_idx) == 0) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return;
  }

  // 如果该bucket与其split image深度不同，也不收缩
  if (dir_page->GetLocalDepth(buc_idx) != dir_page->GetLocalDepth(image_buc_idx)) {
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    table_latch_.WUnlock();
    return;
  }

  // 因为并发问题，也需要检查是不是空
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->RLatch();
  if (!buc_page->IsEmpty()) {
    buc_page_raw->RUnlatch();
    buffer_pool_manager_->UnpinPage(buc_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  buc_page_raw->RUnlatch();

  // 删除bucket，此时该bucket已经为空
  buffer_pool_manager_->UnpinPage(buc_page_id, false);
  buffer_pool_manager_->DeletePage(buc_page_id);

  // 执行合并
  page_id_t image_page_id = dir_page->GetBucketPageId(image_buc_idx);
  dir_page->SetBucketPageId(buc_idx, image_page_id);
  dir_page->DecrLocalDepth(buc_idx);
  dir_page->DecrLocalDepth(image_buc_idx);
  assert(dir_page->GetLocalDepth(buc_idx) == dir_page->GetLocalDepth(image_buc_idx));

  // 遍历整个directory，将所有指向buc_page的bucket全部重新指向image_buc_page
  uint32_t size = dir_page->Size();
  for (uint32_t i = 0; i < size; ++i) {
    page_id_t temp_page_id = dir_page->GetBucketPageId(i);
    if (temp_page_id == buc_page_id) {
      dir_page->SetBucketPageId(i, image_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_buc_idx));
    }
  }

  // 判断global_depth是否需要缩减
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
