//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {
// RID()生成一个无效的rid
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 如果条件存在，就跳过不符合条件的行
  while (iter_ != table_info_->table_->End() && plan_->GetPredicate() != nullptr) {
    // 加锁,如果不是 READ_UNCOMMITTED，读取均需要获取 shared lock
    LockManager *locker = GetExecutorContext()->GetLockManager();
    Transaction *txn = GetExecutorContext()->GetTransaction();
    locker->LockShared(txn, iter_->GetRid());  // 这里会在LockShared函数判断是不是READ_UNCOMMITTED

    bool is_valid = plan_->GetPredicate()->Evaluate(&(*iter_), &(table_info_->schema_)).GetAs<bool>();

    // 解锁，如果是 READ_COMMITTED，读完后需要立刻释放 shared lock
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      locker->Unlock(txn, iter_->GetRid());
    }

    if (is_valid) {
      break;
    }
    ++iter_;
  }

  // 返回 RID 是为了能找到磁盘中的投影之前的原始数据
  if (iter_ != table_info_->table_->End()) {
    // 加锁,如果不是 READ_UNCOMMITTED，读取均需要获取 shared lock
    LockManager *locker = GetExecutorContext()->GetLockManager();
    Transaction *txn = GetExecutorContext()->GetTransaction();
    locker->LockShared(txn, iter_->GetRid());  // 这里会在LockShared函数判断是不是READ_UNCOMMITTED

    // 取出当前符合条件行的每一列数据
    std::vector<Value> values;
    for (size_t i = 0; i < GetOutputSchema()->GetColumnCount(); ++i) {
      values.emplace_back(GetOutputSchema()->GetColumn(i).GetExpr()->Evaluate(&(*iter_), &(table_info_->schema_)));
    }
    *tuple = Tuple(values, GetOutputSchema());
    *rid = iter_->GetRid();

    // 解锁，如果是 READ_COMMITTED，读完后需要立刻释放 shared lock
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      locker->Unlock(txn, iter_->GetRid());
    }

    ++iter_;
    return true;  // 返回true其上层引擎会继续调用
  }

  // 迭代器尾部，即表尾
  return false;
}

}  // namespace bustub
