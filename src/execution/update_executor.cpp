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
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tp{};
  RID child_rid{};
  // project3的更新策略是先删除然后再插入
  while (child_executor_->Next(&child_tp, &child_rid)) {
    ++count;
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    std::vector<Value> new_values{};
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tp, child_executor_->GetOutputSchema()));
    }
    auto update_tuple = Tuple{new_values, &table_info_->schema_};
    std::optional<RID> new_rid_opt = table_info_->table_->InsertTuple(TupleMeta{0, false}, update_tuple);
    if (!new_rid_opt.has_value()) {
      return false;
    }
    RID new_rid = new_rid_opt.value();
    // 更新所有相关索引，需要先删除直接的索引，然后插入新的索引信息
    for (auto &index_info : indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tp.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      auto new_key = update_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      // 从索引中删除旧元组的条目
      index->DeleteEntry(old_key, child_rid, this->exec_ctx_->GetTransaction());
      // 向索引中插入新元组的条目
      index->InsertEntry(new_key, new_rid, this->exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true; 
}

}  // namespace bustub
