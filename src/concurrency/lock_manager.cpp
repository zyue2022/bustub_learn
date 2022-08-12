//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::scoped_lock locker{latch_};
  
  // 已经是abort状态，即终止状态
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // 不是处于 2PL 的 GROWING 阶段，直接 abort
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 读未提交不需要读锁（共享锁）
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 已经获取到共享锁或独占锁
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 正式等待获取共享锁
  LockRequest req(txn, LockMode::SHARED);  // 先构造一个请求
  auto& lock_queue = lock_table_[rid];  // 该元组对应的请求队列，其实含有一个双向链表，链表节点是一个请求
  auto it = lock_queue.request_queue_.begin();

  if (lock_table_.count(rid) == 0) {  // 当前资源未被占用
    req.granted_ = true;
    
  } 
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
