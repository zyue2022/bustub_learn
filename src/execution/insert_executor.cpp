//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

#include <memory>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      catalog_(nullptr),
      table_info_(nullptr),
      child_executor_(move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
}

// 插入，记录+索引
void InsertExecutor::InsertIntoTableWithIndex(Tuple *tuple) {
  RID new_rid;

  // 插入记录
  if (!table_info_->table_->InsertTuple(*tuple, &new_rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
  }

  // 插入后再加锁，有点小问题
  LockManager *locker = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  // 加锁，加写锁，如果原来是读锁就需要升级，检查到没有加写锁就需要加写锁
  if (txn->IsSharedLocked(new_rid)) {
    locker->LockUpgrade(txn, new_rid);
  } else if (!txn->IsExclusiveLocked(new_rid)) {
    locker->LockExclusive(txn, new_rid);
  }

  // 索引更新
  for (auto &indexinfo : catalog_->GetTableIndexes(table_info_->name_)) {
    // 增加索引
    indexinfo->index_->InsertEntry(tuple->KeyFromTuple(table_info_->schema_, *(indexinfo->index_->GetKeySchema()),
                                                       indexinfo->index_->GetKeyAttrs()),
                                   new_rid, exec_ctx_->GetTransaction());
    // 同时记录事务变更
    txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(new_rid, table_info_->oid_, WType::INSERT, *tuple,
                                                           indexinfo->index_oid_, exec_ctx_->GetCatalog()));
  }

  // 解锁，READ_UNCOMMITTED 和 READ_COMMITTED 写入完成后立刻释放 exclusive lock。
  // REPEATABLE_READ 会在整个事务 commit 时统一 unlock，不需要我们自己编写代码
  if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
    locker->Unlock(txn, new_rid);
  }
}

/**
 * @description: 有两种插入情况，先将待插入的tuple保存到容器中
 * @param {[[maybe_unused]] Tuple} *tuple
 * @param {RID} *rid
 */
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    // 原始插入
    for (const auto &row_value : plan_->RawValues()) {
      insert_tuples_.emplace_back(Tuple(row_value, &table_info_->schema_));
    }
  } else {
    // 有嵌套
    child_executor_->Init();
    try {
      Tuple tuple;
      RID rid;
      while (child_executor_->Next(&tuple, &rid)) {
        insert_tuples_.emplace_back(tuple);
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
      return false;
    }
  }

  for (auto &insert_row : insert_tuples_) {
    InsertIntoTableWithIndex(&insert_row);
  }
  return false;
}

}  // namespace bustub
