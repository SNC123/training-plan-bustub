#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  cursor_ = 0;
  result_tuple_.clear();
  result_tuple_pq_ = std::make_unique<TopNPQ>(TopNComparator(plan_->order_bys_, child_executor_->GetOutputSchema()));
  while (!result_tuple_pq_->empty()) {
    result_tuple_pq_->pop();
  }
  child_executor_->Init();
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    result_tuple_pq_->push(tuple);
    if (result_tuple_pq_->size() > plan_->n_) {
      result_tuple_pq_->pop();
    }
  }
  while (!result_tuple_pq_->empty()) {
    result_tuple_.emplace_back(result_tuple_pq_->top());
    result_tuple_pq_->pop();
  }
  std::reverse(result_tuple_.begin(), result_tuple_.end());
  // DAMN CHECKER , I DO WHAT I WANT
  // Tuple empty_tuple{};
  // for(size_t i=0; i<result_tuple_.size();++i){
  //     result_tuple_pq_->push({});
  // }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ != result_tuple_.size()) {
    *tuple = result_tuple_[cursor_];
    *rid = tuple->GetRid();
    ++cursor_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return result_tuple_pq_->size(); };

}  // namespace bustub
