#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, HeapComparator> top_tuples(
      HeapComparator(&GetOutputSchema(), plan_->GetOrderBy()));
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    top_tuples.push(tuple);
    if (top_tuples.size() > plan_->GetN()) {
      top_tuples.pop();
    }
  }
  while (!top_tuples.empty()) {
    top_tuples_.push_back(top_tuples.top());
    top_tuples.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (top_tuples_.empty()) {
    return false;
  }
  *tuple = top_tuples_.back();
  top_tuples_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_tuples_.size(); };

}  // namespace bustub
