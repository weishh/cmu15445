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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // auto ctlog = exec_ctx_->GetCatalog();
  // auto tb_info = ctlog->GetTable(plan_->table_oid_);
  // tb_iter_ = std::make_unique<TableIterator>(tb_info->table_->MakeIterator());
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto iter = table_heap_->MakeIterator();
  rids_.clear();
  while (!iter.IsEnd()) {
    rids_.push_back(iter.GetRID());
    ++iter;
  }
  rid_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // auto filter = plan_->filter_predicate_;
  // if (tb_iter_->IsEnd()) {
  //   return false;
  // }
  // auto tmp = tb_iter_->GetTuple();
  // *tuple = tmp.second;
  // *rid = tb_iter_->GetRID();
  // ++*tb_iter_;
  // if (tmp.first.is_deleted_) {
  //   return false;
  // }
  // if (filter != nullptr) {
  //   auto value = filter->Evaluate(tuple, GetOutputSchema());
  //   if (value.IsNull() || value.GetAs<bool>()) {
  //     return false;
  //   }
  // }
  // return true;
  TupleMeta meta{};
  do {
    if (rid_iter_ == rids_.end()) {
      return false;
    }
    meta = table_heap_->GetTuple(*rid_iter_).first;
    if (!meta.is_deleted_) {
      *tuple = table_heap_->GetTuple(*rid_iter_).second;
      *rid = *rid_iter_;
    }
    ++rid_iter_;
  } while (meta.is_deleted_ ||
           (plan_->filter_predicate_ != nullptr &&
            !plan_->filter_predicate_
                 ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
                 .GetAs<bool>()));
  return true;
}

}  // namespace bustub
