#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  cursor_ = 0;
  result_tuple_.clear();
  child_executor_->Init();
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    result_tuple_.emplace_back(tuple);
  }
  sort(result_tuple_.begin(), result_tuple_.end(),
       SortComparator(plan_->order_bys_, child_executor_->GetOutputSchema()));
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ != result_tuple_.size()) {
    *tuple = result_tuple_[cursor_];
    *rid = tuple->GetRid();
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
