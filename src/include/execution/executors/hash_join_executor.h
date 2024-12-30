//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "aggregation_executor.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  std::vector<Value> hash_keys_;
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.hash_keys_.size(); ++i) {
      if (hash_keys_[i].CompareEquals(other.hash_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
};  // namespace bustub
namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.hash_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
};  // namespace std

namespace bustub {

class SimpleJoinHashTable {
 public:
  void InsertKey(const HashJoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> tuple_vector;
      tuple_vector.push_back(tuple);
      ht_.insert({join_key, tuple_vector});
    } else {
      ht_.at(join_key).push_back(tuple);
    }
  }

  auto GetValue(const HashJoinKey &join_key) -> std::vector<Tuple> * {
    if (ht_.find(join_key) == ht_.end()) {
      return nullptr;
    }
    return &(ht_.find(join_key)->second);
  }

  void Clear() { ht_.clear(); }

 private:
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  auto GetLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, left_executor_->GetOutputSchema()));
    }
    return {values};
  }

  auto GetRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, right_executor_->GetOutputSchema()));
    }
    return {values};
  }

  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unique_ptr<SimpleJoinHashTable> simple_ht_;
  std::vector<Tuple>::iterator iter_;
  Tuple left_tuple_{};
  RID left_rid_{};
  std::vector<Tuple> *right_tuple_{nullptr};
  bool has_done_;
  bool left_flag_;
};

}  // namespace bustub
