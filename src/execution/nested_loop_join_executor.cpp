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
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      result_tuple_({}) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  cursor_ = 0;
  result_tuple_.clear();
  left_executor_->Init();
  right_executor_->Init();
  RID left_rid{};
  RID right_rid{};
  Tuple left_tuple{};
  Tuple right_tuple{};

  if (plan_->GetJoinType() == JoinType::INNER) {
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                     right_executor_->GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          std::vector<Value> values;
          auto left_schema_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
          for (uint32_t idx = 0; idx < left_schema_col_cnt; ++idx) {
            values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), idx));
          }
          auto right_schema_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
          for (uint32_t idx = 0; idx < right_schema_col_cnt; ++idx) {
            values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), idx));
          }
          result_tuple_.emplace_back(Tuple(values, &GetOutputSchema()));
        }
      }
      right_executor_->Init();
    }
  } else if (plan_->GetJoinType() == JoinType::LEFT) {
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      bool right_empty = true;
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        auto value = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                     right_executor_->GetOutputSchema());
        if (!value.IsNull() && value.GetAs<bool>()) {
          right_empty = false;
          std::vector<Value> values;
          auto left_schema_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
          for (uint32_t idx = 0; idx < left_schema_col_cnt; ++idx) {
            values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), idx));
          }
          auto right_schema_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
          for (uint32_t idx = 0; idx < right_schema_col_cnt; ++idx) {
            values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), idx));
          }
          result_tuple_.emplace_back(Tuple(values, &GetOutputSchema()));
        }
      }
      if (right_empty) {
        std::vector<Value> values;
        auto left_schema_col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t idx = 0; idx < left_schema_col_cnt; ++idx) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        // append null value
        auto right_schema_col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t idx = 0; idx < right_schema_col_cnt; ++idx) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
        }
        result_tuple_.emplace_back(Tuple(values, &GetOutputSchema()));
      }
      right_executor_->Init();
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ != result_tuple_.size()) {
    *tuple = result_tuple_[cursor_];
    *rid = tuple->GetRid();
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
