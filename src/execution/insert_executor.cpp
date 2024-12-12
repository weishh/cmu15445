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

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto catlog = exec_ctx_->GetCatalog();
  table_info_ = catlog->GetTable(plan_->GetTableOid());
  child_executor_->Init();
}

// 从child获取一条数据，然后
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_rows == -1) {
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    Tuple tmp;
    
  }

  return false;
}

}  // namespace bustub
