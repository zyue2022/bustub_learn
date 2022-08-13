// ===----------------------------------------------------------------------===//
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

#include <utility>
#include <vector>
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/**
 * @description: abort新事务的写锁，等待老事务的写锁，和读锁共存
 * @param {Transaction} *txn
 * @param {RID} &rid
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

check:
  // 已经终止
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 读未提交，不用读锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // REPEATABLE_READ只有在第一阶段获得锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 已经获得共享锁或读锁
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  if (lock_table_.count(rid) > 0) {
    // 该rid对应的元组上有事务
    auto it = lock_table_[rid].request_queue_.begin();
    while (it != lock_table_[rid].request_queue_.end()) {
      // 根据wound-wait原则,如果新事物持有写锁就需要Abort这个事务,新事务持有读锁则可以共存
      Transaction *cur_trans = TransactionManager::GetTransaction(it->txn_id_);
      // txd_id大的就是新事务，小的是老事务
      if (it->txn_id_ > txn->GetTransactionId() && it->lock_mode_ == LockMode::EXCLUSIVE) {
        // Abort掉申请写锁的新事物
        cur_trans->GetExclusiveLockSet()->erase(rid);
        cur_trans->SetState(TransactionState::ABORTED);
        it = lock_table_[rid].request_queue_.erase(it);
      } else if (it->txn_id_ < txn->GetTransactionId() && it->lock_mode_ == LockMode::EXCLUSIVE) {
        // 等待持有写锁的老事务
        lock_table_[rid].cv_.wait(ul);
        goto check;  // 等待到获取锁后还需要重新判断
      } else {
        ++it;
      }
    }
  }
  // 成功获取到读锁
  LockRequest new_req(txn->GetTransactionId(), LockMode::SHARED, true);  // 新构造一个请求，txn_id mode granted
  lock_table_[rid].request_queue_.emplace_back(new_req);                 // 加到链表形式的请求队列的尾部
  txn->GetSharedLockSet()->emplace(rid);                                 // 加入共享锁集
  txn->SetState(TransactionState::GROWING);
  return true;
}

/**
 * @description: 和任意锁都是冲突的,txn要abort新事务，但如果有任何老事务，txn要被abort
 * @param {Transaction} *txn
 * @param {RID} &rid
 */
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  if (lock_table_.count(rid) > 0) {
    auto it = lock_table_[rid].request_queue_.begin();
    while (it != lock_table_[rid].request_queue_.end()) {
      Transaction *cur_trans = TransactionManager::GetTransaction(it->txn_id_);  // cur是当前遍历到的事务
      if (it->txn_id_ > txn->GetTransactionId()) {
        // 当前是新事务，txn是老事务，abort掉新事务
        cur_trans->SetState(TransactionState::ABORTED);
        if (it->lock_mode_ == LockMode::SHARED) {
          cur_trans->GetSharedLockSet()->erase(rid);
        } else {
          cur_trans->GetExclusiveLockSet()->erase(rid);
        }
        it = lock_table_[rid].request_queue_.erase(it);
      } else if (it->txn_id_ < txn->GetTransactionId()) {
        // 当前是老事务，txn是想获取写锁的新事务，不能和老事务共存，txn自己被abort
        txn->SetState(TransactionState::ABORTED);
        return false;
      } else {
        ++it;
      }
    }
  }

  // 成功获取到写锁
  LockRequest new_req(txn->GetTransactionId(), LockMode::EXCLUSIVE, true);
  lock_table_[rid].request_queue_.emplace_back(new_req);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);
  return true;
}

/**
 * @description: 只有当比你 older 的 txn 不含有 exclusive lock 时，你才可以 upgrade 你的 shared lock
 * @param {Transaction} *txn
 * @param {RID} &rid
 */
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

check:
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 如果事务本身并没有获取到共享锁，则返回false
  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 如果已经获得排它锁，则直接返回true
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto it = lock_table_[rid].request_queue_.begin();
  while (it != lock_table_[rid].request_queue_.end()) {
    Transaction *cur_trans = TransactionManager::GetTransaction(it->txn_id_);
    if (it->txn_id_ > txn->GetTransactionId()) {
      // txn算是老事务，它可以abort当前这个新事务，不管新事务是读锁还是写锁
      cur_trans->SetState(TransactionState::ABORTED);
      if (it->lock_mode_ == LockMode::SHARED) {
        cur_trans->GetSharedLockSet()->erase(rid);
      } else {
        cur_trans->GetExclusiveLockSet()->erase(rid);
      }
      it = lock_table_[rid].request_queue_.erase(it);
    } else if (it->txn_id_ < txn->GetTransactionId()) {
      // 当前是老事物，事务txn作为新事务需要等待
      lock_table_[rid].cv_.wait(ul);
      goto check;
    } else {
      ++it;
    }
  }

  // 此时队列中应该只剩下待升级为写锁的lockShared请求
  auto &shared = lock_table_[rid].request_queue_.front();
  assert(lock_table_[rid].request_queue_.size() == 1);
  assert(shared.txn_id_ == txn->GetTransactionId());
  shared.lock_mode_ = LockMode::EXCLUSIVE;
  shared.granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> ul(latch_);

  // 没有锁
  if (!txn->IsExclusiveLocked(rid) && !txn->IsSharedLocked(rid)) {
    return false;
  }

  // 如果当前事务隔离级别是 REPETABLE_READ，且处于 2PL 的 GROWING 阶段，将 2PL 设置为 SHRINKING 阶段
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto it = lock_table_[rid].request_queue_.begin();
  bool ret = false;
  while (it != lock_table_[rid].request_queue_.end()) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      // 找到已经持有锁的txn，其在队列的位置
      assert(it->lock_mode_ == LockMode::SHARED || it->lock_mode_ == LockMode::EXCLUSIVE);
      if (it->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        lock_table_[rid].cv_.notify_all();
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
        lock_table_[rid].cv_.notify_all();
      }
      it->granted_ = false;  // 感觉 granted_ 并未用到
      it = lock_table_[rid].request_queue_.erase(it);
      // return true;
      ret = true;
    }
    ++it;
  }
  return ret;
}

}  // namespace bustub
