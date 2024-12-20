//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  Tuple child_tp{};
  RID rid{};
  while (child_executor_->Next(&child_tp, &rid)) {
    auto key = MakeAggregateKey(&child_tp);
    auto value = MakeAggregateValue(&child_tp);
    aht_->InsertCombine(key, value);
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  flag = false;
  std::cout << "After AHT creation - types: " << aht_->agg_types_.size() << std::endl;
}
// 每次返回聚合好的一条tuple
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //  先判断分组空不空
  std::cout << "After AHT creation - types: " << aht_->agg_types_.size() << std::endl;
  if (aht_->Begin() != aht_->End()) {
    if (*aht_iterator_ == aht_->End()) {
      return false;
    }
    // 获取聚合键和聚合值，已经是计算好的
    auto agg_key = aht_iterator_->Key();
    auto agg_val = aht_iterator_->Val();
    // 根据聚合键和聚合值生成查询结果元组
    std::vector<Value> values{};
    // 遍历聚合键和聚合值，生成查询结果元组
    // 根据文件要求，有groupby和aggregate两个部分的情况下，groupby也要算上，都添加到value中
    values.reserve(agg_key.group_bys_.size() + agg_val.aggregates_.size());
    for (auto &group_values : agg_key.group_bys_) {
      values.emplace_back(group_values);
    }
    for (auto &agg_value : agg_val.aggregates_) {
      values.emplace_back(agg_value);
    }
    *tuple = {values, &GetOutputSchema()};
    ++*aht_iterator_;
    return true;
  }
  // 设置flag的原因是对于空表只返回一次结果，再一次的next直接返回false
  if (flag) {
    return false;
  }
  flag = true;
  // 没有groupby语句则生成一个初始的聚合值元组并返回
  if (plan_->GetGroupBys().empty()) {
    std::vector<Value> values{};
    Tuple tuple_buffer{};
    // 检查当前表是否为空，如果为空生成默认的聚合值，对于 count(*) 来说是 0，对于其他聚合函数来说是 integer_null(
    // 默认聚合值要求由GenerateInitialAggregateValue实现, 有bug，先delete全部数据后再select count(*) 会有bug
    for (auto &agg_value : aht_->GenerateInitialAggregateValue().aggregates_) {
      values.emplace_back(agg_value);
    }
    *tuple = {values, &GetOutputSchema()};
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
