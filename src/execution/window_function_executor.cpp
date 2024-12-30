#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  auto window_functions = plan_->window_functions_;
  auto column_sz = plan_->columns_.size();
  std::vector<bool> is_order_by(plan_->columns_.size(), false);
  std::vector<AbstractExpressionRef> window_exprs(column_sz);
  std::vector<WindowFunctionType> window_func_types(column_sz);
  std::vector<std::vector<AbstractExpressionRef>> partition_bys(column_sz);
  std::vector<std::vector<std::pair<OrderByType, AbstractExpressionRef>>> order_bys(column_sz);
  std::vector<bool> is_function_expr(column_sz, false);
  for (uint32_t i = 0; i < column_sz; i++) {
    if (window_functions.find(i) == window_functions.end()) {
      window_exprs[i] = plan_->columns_[i];
      is_function_expr[i] = false;
      is_order_by[i] = false;
      whts_.emplace_back(window_func_types[i]);
      continue;
    }
    is_function_expr[i] = true;
    const auto &window_function = window_functions.find(i)->second;
    window_exprs[i] = window_function.function_;
    window_func_types[i] = window_function.type_;
    partition_bys[i] = window_function.partition_by_;
    order_bys[i] = window_function.order_by_;
    is_order_by[i] = !window_function.order_by_.empty();
    whts_.emplace_back(window_func_types[i]);
  }
  Tuple tuple{};
  RID rid{};
  std::vector<Tuple> tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(tuple);
  }
  const auto &order_by(window_functions.begin()->second.order_by_);
  if (!order_by.empty()) {
    std::sort(tuples.begin(), tuples.end(), Comparator(&child_executor_->GetOutputSchema(), order_by));
  }
  std::vector<std::vector<AggregateKey>> tuple_keys;
  for (const auto &this_tuple : tuples) {
    std::vector<Value> values{};
    std::vector<AggregateKey> keys;
    for (uint32_t i = 0; i < column_sz; ++i) {
      if (is_function_expr[i]) {
        auto agg_key = MakeWinKey(&this_tuple, partition_bys[i]);
        if (window_func_types[i] == WindowFunctionType::Rank) {
          auto new_value = order_by[0].second->Evaluate(&this_tuple, this->GetOutputSchema());
          values.emplace_back(whts_[i].InsertCombine(agg_key, new_value));
          keys.emplace_back(agg_key);
          continue;
        }
        auto agg_val = MakeWinValue(&this_tuple, window_exprs[i]);
        values.emplace_back(whts_[i].InsertCombine(agg_key, agg_val));
        keys.emplace_back(agg_key);
        continue;
      }
      values.emplace_back(window_exprs[i]->Evaluate(&this_tuple, this->GetOutputSchema()));
      keys.emplace_back();
    }

    tuples_.emplace_back(std::move(values));

    tuple_keys.emplace_back(std::move(keys));
  }
  for (uint32_t tuple_idx = 0; tuple_idx < tuples_.size(); ++tuple_idx) {
    auto &tuplenew = tuples_[tuple_idx];
    for (uint32_t i = 0; i < tuplenew.size(); ++i) {
      if (is_function_expr[i] && !is_order_by[i]) {
        tuplenew[i] = whts_[i].Find(tuple_keys[tuple_idx][i]);
      }
    }
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  // 获取元组
  *tuple = Tuple(tuples_.front(), &this->GetOutputSchema());
  *rid = tuple->GetRid();
  tuples_.pop_front();
  return true;
}
}  // namespace bustub
