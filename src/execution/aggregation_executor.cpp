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
#define LOG_LEVEL LOG_LEVEL_OFF
#include "common/logger.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(SimpleAggregationHashTable(plan->aggregates_, plan->agg_types_)),
      aht_iterator_({}) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  // error!
  // Tuple *tuple = nullptr;
  // RID *rid = nullptr;
  RID rid{};
  Tuple tuple{};

  while (child_executor_->Next(&tuple, &rid)) {
    is_empty_ = false;
    LOG_DEBUG("tuple: %s", tuple.ToString(&child_executor_->GetOutputSchema()).c_str());
    AggregateKey aggr_key;
    // case: without group by, considered as one group named "*"
    if (plan_->group_bys_.empty()) {
      auto str_value = ValueFactory::GetVarcharValue("*");
      aggr_key = AggregateKey({std::vector<Value>{str_value}});
    } else {
      aggr_key = MakeAggregateKey(&tuple);
    }
    auto aggr_val = MakeAggregateValue(&tuple);
    // for(auto& it : aggr_key.group_bys_) {
    //     LOG_DEBUG("aggr_key_group_by: %s",it.ToString().c_str());
    // }
    // for(auto& it : aggr_val.aggregates_) {
    //     LOG_DEBUG("aggr_val_aggr: %s",it.ToString().c_str());
    // }
    aht_.InsertCombine(aggr_key, aggr_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("output schema: %s", GetOutputSchema().ToString().c_str());
  if (aht_iterator_ != aht_.End()) {
    if (plan_->group_bys_.empty()) {
      *tuple = Tuple(aht_iterator_.Val().aggregates_, &GetOutputSchema());
    } else {
      // prefix with key
      std::vector<Value> values(aht_iterator_.Key().group_bys_);
      values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
      *tuple = Tuple(values, &GetOutputSchema());
    }
    LOG_DEBUG("iter_tuple: %s", tuple->ToString(&GetOutputSchema()).c_str());
    *rid = tuple->GetRid();
    ++aht_iterator_;
    return true;
  }
  if (is_empty_) {
    // because run only once
    is_empty_ = false;
    if (plan_->group_bys_.empty()) {
      *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema());
    } else {
      return false;
      // prefix with key
      // auto init_vals = aht_.GenerateInitialAggregateValue().aggregates_;
      // std::vector<Value> values(aht_iterator_.Key().group_bys_);
      // values.insert(values.end(),
      //     init_vals.begin(),
      //     init_vals.end()
      // );
    }

    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
