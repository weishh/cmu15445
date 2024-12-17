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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
  child_executor_->Init();
  has_inserted_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(has_inserted_){
    return false;
  }
  has_inserted_ = true; 
  int count = 0;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tp {}; 
  RID child_rid {};
  while(child_executor_->Next(&child_tp, &child_rid)){
    ++count;
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    
  }
}

}  // namespace bustub
