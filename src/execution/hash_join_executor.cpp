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
#include "type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_child)),
    right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() { 
  cursor_ = 0;
  inner_hash_table_.clear();
  result_tuple_.clear();
  left_executor_->Init();
  right_executor_->Init();
  // To simplify, always choose right table as inner table to build hash table
  RID right_rid{};
  Tuple right_tuple{};
  while(right_executor_->Next(&right_tuple, &right_rid)) {
    inner_hash_table_[MakeRightJoinKey(&right_tuple)].emplace_back(
      MakeHashJoinValue(&right_tuple, &right_executor_->GetOutputSchema()));
  }
  if (plan_->GetJoinType() == JoinType::INNER) {
    RID left_rid{};
    Tuple left_tuple{};
    while(left_executor_->Next(&left_tuple, &left_rid)){
      auto left_key = MakeLeftJoinKey(&left_tuple);
      auto iter = inner_hash_table_.find(left_key);
      if(iter == inner_hash_table_.end()) { continue; }
      // handle hash collisions
      auto right_values= iter->second;
      for(auto &join_value : right_values){
        auto right_tuple =  JoinValueToTuple(&join_value, &right_executor_->GetOutputSchema());
        auto right_key = MakeRightJoinKey(&right_tuple);
        if(left_key == right_key){
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
    }
  }else if(plan_->GetJoinType() == JoinType::LEFT){
    RID left_rid{};
    Tuple left_tuple{};
    while(left_executor_->Next(&left_tuple, &left_rid)){
      auto left_key = MakeLeftJoinKey(&left_tuple);
      auto iter = inner_hash_table_.find(left_key);
      if(iter == inner_hash_table_.end()) {
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
        continue; 
      }
      // handle hash collisions
      auto right_values= iter->second;
      for(auto &join_value : right_values){
        auto right_tuple =  JoinValueToTuple(&join_value, &right_executor_->GetOutputSchema());
        auto right_key = MakeRightJoinKey(&right_tuple);
        if(left_key == right_key){
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
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (cursor_ != result_tuple_.size()) {
    *tuple = result_tuple_[cursor_];
    *rid = tuple->GetRid();
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
