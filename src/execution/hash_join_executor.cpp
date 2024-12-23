//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan) ,left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  simple_ht_ = std::make_unique<SimpleJoinHashTable>();
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  simple_ht_->Clear();
  left_flag_ = left_executor_->Next(&left_tuple_, &left_rid_);
  Tuple right_tuple{};
  RID right_rid{};
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    auto right_hash_key = GetRightJoinKey(&right_tuple);
    simple_ht_->InsertKey(right_hash_key, right_tuple);
  }
  auto left_hash_key = GetLeftJoinKey(&left_tuple_);
  right_tuple_ = simple_ht_->GetValue(left_hash_key);
  if (right_tuple_ != nullptr) {
    iter_ = right_tuple_->begin();
    has_done_ = true;
  } else {
    has_done_ = false;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 有匹配到
    if (right_tuple_ != nullptr && iter_ != right_tuple_->end()) {
      std::vector<Value> values;
      auto rtp = *iter_;
      for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null
      for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(rtp.GetValue(&this->right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      ++iter_;
      return true;
    }
    // 左链接且没有匹配
    if (plan_->GetJoinType() == JoinType::LEFT && !has_done_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < this->left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&this->left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < this->right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(this->right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      has_done_ = true;
      return true;
    }
    left_flag_ = left_executor_->Next(&left_tuple_, &left_rid_);
    if (!left_flag_) {
      return false;
    }
    auto left_hash_key = GetLeftJoinKey(&left_tuple_);
    // 在哈希表中查找与左侧元组匹配的右侧元组
    right_tuple_ = simple_ht_->GetValue(left_hash_key);
    if (right_tuple_ != nullptr) {
      iter_ = right_tuple_->begin();
      has_done_ = true;
    } else {
      has_done_ = false;
    }
  }
}

}  // namespace bustub
