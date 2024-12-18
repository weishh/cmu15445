//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  this->htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto table_schema = index_info->key_schema_;
  // 获取键值
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &table_schema);
  result_rids_.clear();

  htable_->ScanKey(index_key, &result_rids_, exec_ctx_->GetTransaction());
  rids_iter_ = result_rids_.begin();
}

// requirements说是点查找，因此只会有一个结果满足要求。next和seqscan的逻辑差不多。知道了RID则gettuple是O1复杂度。
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta{};
  do {
    if (rids_iter_ == result_rids_.end()) {
      return false;
    }
    meta = table_heap_->GetTuple(*rids_iter_).first;
    if (!meta.is_deleted_) {
      *tuple = table_heap_->GetTuple(*rids_iter_).second;
      *rid = *rids_iter_;
    }
    ++rids_iter_;
  } while (meta.is_deleted_ ||
           (plan_->filter_predicate_ != nullptr &&
            !plan_->filter_predicate_
                 ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)->schema_)
                 .GetAs<bool>()));
  return true;
}

}  // namespace bustub
