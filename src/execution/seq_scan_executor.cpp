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
  auto ctlog = exec_ctx_->GetCatalog();
  auto tb_info = ctlog->GetTable(plan_->table_oid_);
  tb_iter_ = std::make_unique<TableIterator>(tb_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter = plan_->filter_predicate_;
  if (tb_iter_->IsEnd()) {
    return false;
  }
  auto tmp = tb_iter_->GetTuple();
  *tuple = tmp.second;
  *rid = tb_iter_->GetRID();
  ++*tb_iter_;
  if (tmp.first.is_deleted_) {
    return false;
  }
  if (filter != nullptr) {
    auto value = filter->Evaluate(tuple, GetOutputSchema());
    if (value.IsNull() || value.GetAs<bool>()) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
