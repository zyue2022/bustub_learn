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

    // 根据子查询器的结果来调用TableHeap标记删除状态
    table_info_->table_->MarkDelete(del_rid, exec_ctx_->GetTransaction());

    // 还要删除索引
    for (const auto &indexinfo : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      indexinfo->index_->DeleteEntry(del_tuple.KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(),
                                                            indexinfo->index_->GetKeyAttrs()),
                                     del_rid, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
