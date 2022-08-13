//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 锁
  LockManager *locker = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();

  Tuple old_tuple;
  RID old_tuple_rid;
  while (true) {
    // 执行子查询
    try {
      if (!child_executor_->Next(&old_tuple, &old_tuple_rid)) {
        break;
      }
    } catch (Exception &e) {  // 接住Exception接着往上抛
      throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateExecutor:child execute error.");
      return false;
    }

    // 加锁，加写锁，如果原来是读锁就需要升级，检查到没有加写锁就需要加写锁
    if (txn->IsSharedLocked(old_tuple_rid)) {
      locker->LockUpgrade(txn, old_tuple_rid);
    } else if (!txn->IsExclusiveLocked(old_tuple_rid)) {
      locker->LockExclusive(txn, old_tuple_rid);
    }

    // 更新记录
    Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
    table_info_->table_->UpdateTuple(new_tuple, old_tuple_rid, exec_ctx_->GetTransaction());
    // 更新索引
    for (const auto &indexinfo : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      // 先删旧索引后增新索引
      indexinfo->index_->DeleteEntry(old_tuple.KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                            indexinfo->index_->GetKeyAttrs()),
                                     old_tuple_rid, exec_ctx_->GetTransaction());
      indexinfo->index_->InsertEntry(new_tuple.KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                            indexinfo->index_->GetKeyAttrs()),
                                     old_tuple_rid, exec_ctx_->GetTransaction());
      // 记录事务变更
      IndexWriteRecord write_record(old_tuple_rid, table_info_->oid_, WType::DELETE, new_tuple, indexinfo->index_oid_,
                                    exec_ctx_->GetCatalog());
      write_record.old_tuple_ = old_tuple;
      txn->GetIndexWriteSet()->emplace_back(write_record);
    }

    // 解锁，READ_UNCOMMITTED 和 READ_COMMITTED 写入完成后立刻释放 exclusive lock。
    // REPEATABLE_READ 会在整个事务 commit 时统一 unlock，不需要我们自己编写代码
    if (txn->GetIsolationLevel() != IsolationLevel::REPEATABLE_READ) {
      locker->Unlock(txn, old_tuple_rid);
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
