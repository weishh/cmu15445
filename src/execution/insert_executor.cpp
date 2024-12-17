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
  has_inserted_ = false;
}

// 从child获取一条数据，然后
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  // 从子执行器 child_executor_ 中逐个获取元组并插入到表中，同时更新所有的索引
  // next函数是虚函数，会自动调用seq_scan中的实现
  while (this->child_executor_->Next(tuple, rid)) {
    std::optional<RID> new_rid_optional = table_info->table_->InsertTuple(TupleMeta{0, false}, *tuple);
    // 遍历所有索引，为每个索引更新对应的条目
    if (!new_rid_optional.has_value()) {
      return false;
    }
    ++count;
    RID new_rid = new_rid_optional.value();
    for (auto &index_info : indexes) {
      // 从元组中提取索引键
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      // 向索引中插入键和新元组的RID
      index_info->index_->InsertEntry(key, new_rid, this->exec_ctx_->GetTransaction());
    }
  }
  // 创建了一个 vector对象values，其中包含了一个 Value 对象。这个 Value 对象表示一个整数值，值为 count
  // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;

  return false;
}

}  // namespace bustub
