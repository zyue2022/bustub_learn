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
  page_id_t buc0_page_id = INVALID_PAGE_ID;
  Page *buc0_page_raw = buffer_pool_manager_->NewPage(&buc0_page_id);
  assert(buc0_page_id != INVALID_PAGE_ID);
  assert(buc0_page_raw != nullptr);
  dir_page->SetBucketPageId(0, buc0_page_id);  // 索引是0
  dir_page->SetLocalDepth(0, 0);

  // 记得unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(buc0_page_id, true));
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
 * @description: 找到某键值对应的桶索引
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

/**
 * @description: 返回key对应的桶id
 * @return {*}
 */
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
  // fix bug: 每FetchPage就将页面加入内存并且pin住了，不能再在同一个函数(线程)fetch
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
  bool get_ok = buc_page->GetValue(key, comparator_, result);
  buc_page_raw->RUnlatch();

  // 记得unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
  // 最后解全局锁
  table_latch_.RUnlock();
  return get_ok;
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
  bool is_full = buc_page->IsFull();
  bool insert_ok = false;

  // 该桶还有容量，直接添加
  if (!is_full) {
    insert_ok = buc_page->Insert(key, value, comparator_);
  }
  buc_page_raw->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, true));
  table_latch_.RUnlock();

  // 目标桶容量不足，需要分裂
  if (is_full) {
    return SplitInsert(transaction, key, value);
  }

  return insert_ok;
}

/**
 * @description: lab2最难的函数了...，扩容后再尝试插入
 *               目录扩展将使散列结构中存在的目录数量增加一倍
 *            任何局部深度小于全局深度的存储桶都被多个目录指向
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 全局哈希表加写锁，分裂时目录页和桶页都可能变化
  table_latch_.WLock();  // writers are splits and merges

  // 同样先取得目录页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t buc_idx = KeyToDirectoryIndex(key, dir_page);  // 获取当前桶在目录页的索引，不是桶页id

  // 高度控制，不能再扩容
  assert(dir_page->GetLocalDepth(buc_idx) <= MAX_GLOBAL_DEPTH);
  if (dir_page->GetLocalDepth(buc_idx) == MAX_GLOBAL_DEPTH) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return false;
  }

  // 原来的bucket
  page_id_t buc_page_id = dir_page->GetBucketPageId(buc_idx);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);

  // 再次检查是不是满了
  if (!buc_page->IsFull()) {
    bool insert_ok = buc_page->Insert(key, value, comparator_);
    assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return insert_ok;
  }

  // 遇到buc_local_depth == global_depth，还需要扩目录页高度
  if (dir_page->GetLocalDepth(buc_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 必须增加local_depth 因为求镜像桶索引时，split函数是高位取反，需要先增加一位；当然，该桶满了肯定要增加深度
  dir_page->IncrLocalDepth(buc_idx);
  uint32_t new_local_deep = dir_page->GetLocalDepth(buc_idx);  // 两兄弟桶高度必定相同

  // 旧桶
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->WLatch();  // 先加锁
  std::vector<MappingType> old_pairs_arr = buc_page->FetchAllMappingType();
  buc_page->ResetBucketPage();  // 旧桶清空

  // 创建一个新的bucket页面
  page_id_t image_buc_page_id = INVALID_PAGE_ID;
  Page *image_buc_page_raw = buffer_pool_manager_->NewPage(&image_buc_page_id);
  assert(image_buc_page_id != INVALID_PAGE_ID);
  assert(image_buc_page_raw != nullptr);
  auto image_buc_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(image_buc_page_raw->GetData());
  image_buc_page_raw->WLatch();

  // 设置新页面高度和id，溢出桶中的元素被重新散列到目录的新全局深度
  uint32_t image_buc_idx = dir_page->GetSplitImageIndex(buc_idx);
  dir_page->SetLocalDepth(image_buc_idx, new_local_deep);  // 新旧桶局部高度相同
  dir_page->SetBucketPageId(image_buc_idx, image_buc_page_id);
  assert(dir_page->GetLocalDepth(buc_idx) == dir_page->GetLocalDepth(image_buc_idx));
  assert(buc_page_id == dir_page->GetBucketPageId(buc_idx));
  assert(image_buc_page_id == dir_page->GetBucketPageId(image_buc_idx));

  // 遍历旧桶对应页面的元素,重新分配位置
  for (auto &old_pair : old_pairs_arr) {
    const auto &[cur_key, cur_value] = old_pair;
    // 相当于 key to 桶的id，用localmask是保证数据只能落在原桶或镜像桶
    // dir_page->GetLocalDepthMask(buc_idx) 是该键值对之前加入桶时的 2*mask+1，就是掩码高了一位
    uint32_t new_buc_idx = Hash(cur_key) & dir_page->GetLocalDepthMask(buc_idx);
    page_id_t new_buc_page_id = dir_page->GetBucketPageId(new_buc_idx);
    assert(new_buc_page_id == buc_page_id || new_buc_page_id == image_buc_page_id);
    assert(new_buc_idx == buc_idx || new_buc_idx == image_buc_idx);
    if (new_buc_page_id == buc_page_id) {
      assert(buc_page->Insert(cur_key, cur_value, comparator_));
    } else {
      assert(image_buc_page->Insert(cur_key, cur_value, comparator_));
    }
  }

  // 可能之前存在许多bucket映射到bucket对应的page上,这些信息也要相应的修改
  uint32_t diff = (1 << new_local_deep);  // diff相当于一个分界线，因为全局高度是翻倍增加的
  for (uint32_t i = buc_idx; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, buc_page_id);
    dir_page->SetLocalDepth(i, new_local_deep);
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = buc_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, buc_page_id);
    dir_page->SetLocalDepth(i, new_local_deep);
  }
  for (uint32_t i = image_buc_idx; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, image_buc_page_id);
    dir_page->SetLocalDepth(i, new_local_deep);
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = image_buc_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_buc_page_id);
    dir_page->SetLocalDepth(i, new_local_deep);
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
  bool remove_ok = buc_page->Remove(key, value, comparator_);
  buc_page_raw->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();

  // 如果当前bucket空了，则执行合并
  if (remove_ok && buc_page->IsEmpty()) {
    Merge(transaction, key, value);
  }

  return remove_ok;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/**
 * @description: 合并其实要递归合并
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 这里加读锁，因为只是找到buc_idx
  table_latch_.RLock();

  // 同样先获得目录页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // 原桶、镜像桶在目录的索引，不是页id
  uint32_t buc_idx = KeyToDirectoryIndex(key, dir_page);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));

  table_latch_.RUnlock();

  MergeTwo(buc_idx);
  // 合并可能的余下空桶
  for (uint32_t idx = 0; idx < dir_page->Size(); ++idx) {
    MergeTwo(idx);
  }
}

/**
 * @description: 自定义辅助函数，递归合并空桶
 * @return {*}
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::MergeTwo(uint32_t buc_idx) {
  // 合并就需要加哈希表的写锁了
  table_latch_.WLock();  // writers are splits and merges

  // 同样先获得目录页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();

  // 无效索引
  if (buc_idx >= dir_page->Size()) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }

  // 原桶的镜像桶在目录的索引，不是页id
  uint32_t image_buc_idx = dir_page->GetSplitImageIndex(buc_idx);  // 其兄弟桶索引

  // local depth为0说明已经最小了，不收缩
  // 如果该bucket与其split image深度不同，也不收缩
  // 如果两兄弟桶对应的page相同，也不合并
  if ((dir_page->GetLocalDepth(buc_idx) == 0) ||
      (dir_page->GetLocalDepth(buc_idx) != dir_page->GetLocalDepth(image_buc_idx)) ||
      (dir_page->GetBucketPageId(buc_idx) == dir_page->GetBucketPageId(image_buc_idx))) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }

  // 因为并发问题，也需要检查桶是不是空
  page_id_t buc_page_id = dir_page->GetBucketPageId(buc_idx);
  HASH_TABLE_BUCKET_TYPE *buc_page = FetchBucketPage(buc_page_id);
  Page *buc_page_raw = reinterpret_cast<Page *>(buc_page);
  buc_page_raw->RLatch();
  if (!buc_page->IsEmpty()) {
    buc_page_raw->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }
  buc_page_raw->RUnlatch();

  // 删除bucket，此时该bucket是空的 先unpin空页
  assert(buffer_pool_manager_->UnpinPage(buc_page_id, false));
  assert(buffer_pool_manager_->DeletePage(buc_page_id));

  // 合并，就是改变桶指向 （桶是一个抽象的概念，目录页的索引指向一个桶，桶对应着page页面）
  page_id_t image_page_id = dir_page->GetBucketPageId(image_buc_idx);
  // 空桶对应的page被删除后，该桶指向其兄弟桶的page，高度也都-1
  dir_page->SetBucketPageId(buc_idx, image_page_id);
  dir_page->DecrLocalDepth(buc_idx);
  dir_page->DecrLocalDepth(image_buc_idx);
  assert(dir_page->GetLocalDepth(buc_idx) == dir_page->GetLocalDepth(image_buc_idx));

  // 遍历目录，将所有指向buc_page的bucket全部重新指向image_buc_page
  uint32_t size = dir_page->Size();
  for (uint32_t i = 0; i < size; ++i) {
    page_id_t cur_page_id = dir_page->GetBucketPageId(i);
    // fix bug, 还需要检查指向 iamge桶对应的页面 的桶，因为局部高度变化
    if (cur_page_id == buc_page_id || cur_page_id == image_page_id) {
      dir_page->SetBucketPageId(i, image_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_buc_idx));
    }
  }

  // 判断global_depth是否需要缩减
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  // 这里只需要unpin目录页，不用unpin镜像桶页，因为只是取出来目录页，原桶对应page已经被delete
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
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
