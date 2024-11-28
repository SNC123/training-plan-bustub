//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

using OrderBys = std::vector<std::pair<OrderByType, AbstractExpressionRef>>;

class TopNComparator {
  OrderBys order_bys_;
  Schema schema_;

 public:
  explicit TopNComparator(OrderBys order_by, Schema schema)
      : order_bys_(std::move(order_by)), schema_(std::move(schema)) {}
  auto operator()(const Tuple &left_tuple, const Tuple &right_tuple) -> bool {
    for (const auto &order_pair : order_bys_) {
      auto left_value = order_pair.second->Evaluate(&left_tuple, schema_);
      auto right_value = order_pair.second->Evaluate(&right_tuple, schema_);
      auto cmp_expr_less = left_value.CompareLessThan(right_value);
      auto cmp_value_less = ValueFactory::GetBooleanValue(cmp_expr_less);
      auto cmp_expr_greater = left_value.CompareGreaterThan(right_value);
      auto cmp_value_greater = ValueFactory::GetBooleanValue(cmp_expr_greater);
      BUSTUB_ENSURE(cmp_value_less.IsNull() == false, "Comparator should be valid");
      BUSTUB_ENSURE(cmp_value_greater.IsNull() == false, "Comparator should be valid");
      BUSTUB_ENSURE(order_pair.first != OrderByType::INVALID, "OrderType should be valid");
      if (order_pair.first == OrderByType::ASC || order_pair.first == OrderByType::DEFAULT) {
        if (cmp_value_less.GetAs<bool>()) {
          return true;
        }
        if (cmp_value_greater.GetAs<bool>()) {
          return false;
        }
      } else {
        if (cmp_value_less.GetAs<bool>()) {
          return false;
        }
        if (cmp_value_greater.GetAs<bool>()) {
          return true;
        }
      }
    }
    return true;
  }
};

using TopNPQ = std::priority_queue<Tuple, std::vector<Tuple>, TopNComparator>;

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // added
  std::unique_ptr<TopNPQ> result_tuple_pq_{nullptr};
  std::vector<Tuple> result_tuple_;
  size_t cursor_{0};
};
}  // namespace bustub
