//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"

#include <memory>

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(move(child_executor)), table_info_(nullptr) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 锁
  LockManager *locker = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();

  Tuple del_tuple;
  RID del_rid;
  while (true) {
    // 执行子查询器，catch异常然后接着抛
    try {
      if (!child_executor_->Next(&del_tuple, &del_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor:child execute error.");
      return false;
    }

    // 加锁，删除肯定是加写锁，如果原来是读锁就需要升级，检查到没有加写锁就需要加写锁
    if (txn->IsSharedLocked(del_rid)) {
      locker->LockUpgrade(txn, del_rid);
    } else if (!txn->IsExclusiveLocked(del_rid)) {
      locker->LockExclusive(txn, del_rid);
    }

    // 根据子查询器的结果来调用TableHeap标记删除状态
    table_info_->table_->MarkDelete(del_rid, exec_ctx_->GetTransaction());
    // 更新索引
    for (const auto &indexinfo : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      // 删除索引
      indexinfo->index_->DeleteEntry(del_tuple.KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                            indexinfo->index_->GetKeyAttrs()),
                                     del_rid, exec_ctx_->GetTransaction());
      // 记录事务变更
      txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(del_rid, table_info_->oid_, WType::DELETE, del_tuple,
                                                             indexinfo->index_oid_, exec_ctx_->GetCatalog()));
    }

    // 解锁，READ_UNCOMMITTED 和 READ_COMMITTED 写入完成后立刻释放 exclusive lock。
    // REPEATABLE_READ 会在整个事务 commit 时统一 unlock，不需要我们自己编写代码
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      locker->Unlock(txn, del_rid);
    }
  }
  return false;
}

}  // namespace bustub
