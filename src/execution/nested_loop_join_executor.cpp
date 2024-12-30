//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_is_exist_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID right_rid{};
  auto l_scheme = left_executor_->GetOutputSchema();
  auto r_scheme = right_executor_->GetOutputSchema();
  auto predicate = plan_->predicate_;
  if (plan_->GetJoinType() == JoinType::LEFT) {
    while (left_is_exist_) {
      // 匹配成功
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto match_res = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                 right_executor_->GetOutputSchema());
        if (match_res.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint32_t i = 0; i < l_scheme.GetColumnCount(); ++i) {
            values.emplace_back(left_tuple_.GetValue(&l_scheme, i));
          }
          for (uint32_t i = 0; i < r_scheme.GetColumnCount(); ++i) {
            values.emplace_back(right_tuple.GetValue(&r_scheme, i));
          }
          *tuple = {values, &GetOutputSchema()};
          is_match_ = true;
          return true;
        }
      }
      // 匹配失败
      if (!is_match_) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < l_scheme.GetColumnCount(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&l_scheme, i));
        }
        for (uint32_t i = 0; i < r_scheme.GetColumnCount(); ++i) {
          values.emplace_back(ValueFactory::GetNullValueByType(r_scheme.GetColumn(i).GetType()));
        }
        *tuple = {values, &GetOutputSchema()};
        is_match_ = true;
        return true;
      }
      left_is_exist_ = left_executor_->Next(&left_tuple_, &left_rid_);
      right_executor_->Init();
      is_match_ = false;
    }
    return false;
  }

  // inner join,要用while判断，自动滤除那些不匹配的。
  while (left_is_exist_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto match_res = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema());
      if (match_res.GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < l_scheme.GetColumnCount(); ++i) {
          values.emplace_back(left_tuple_.GetValue(&l_scheme, i));
        }
        for (uint32_t i = 0; i < r_scheme.GetColumnCount(); ++i) {
          values.emplace_back(right_tuple.GetValue(&r_scheme, i));
        }
        *tuple = {values, &GetOutputSchema()};
        is_match_ = true;
        return true;
      }
    }
    left_is_exist_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
