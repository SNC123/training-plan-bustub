//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

using OrderBys = std::vector<std::pair<OrderByType, AbstractExpressionRef>>;

class SortComparator {
  OrderBys order_bys_;
  Schema schema_;

 public:
  explicit SortComparator(OrderBys order_by, Schema schema)
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

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  // added
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> result_tuple_;
  size_t cursor_{0};
};
}  // namespace bustub
