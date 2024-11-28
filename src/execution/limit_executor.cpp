//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  cursor_ = 0;
  result_tuple_.clear();
  child_executor_->Init();
  RID rid{};
  Tuple tuple{};
  while (child_executor_->Next(&tuple, &rid)) {
    result_tuple_.emplace_back(tuple);
  }
  while (result_tuple_.size() > plan_->limit_) {
    result_tuple_.pop_back();
  }
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ != result_tuple_.size()) {
    *tuple = result_tuple_[cursor_];
    *rid = tuple->GetRid();
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
