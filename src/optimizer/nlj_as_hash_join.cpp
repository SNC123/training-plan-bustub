#define LOG_LEVEL LOG_LEVEL_OFF

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void ComparisonExpressionSpilt(const ComparisonExpression *expr,
                               std::vector<AbstractExpressionRef> *left_key_expressions,
                               std::vector<AbstractExpressionRef> *right_key_expressions) {
  if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
      left_expr != nullptr) {
    if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
        right_expr != nullptr) {
      // Ensure both exprs have tuple_id == 0
      auto left_expr_tuple_0 =
          std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
      auto right_expr_tuple_0 =
          std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
      if (left_expr->GetTupleIdx() == 0) {
        left_key_expressions->emplace_back(left_expr_tuple_0);
      } else {
        right_key_expressions->emplace_back(left_expr_tuple_0);
      }
      if (right_expr->GetTupleIdx() == 0) {
        left_key_expressions->emplace_back(right_expr_tuple_0);
      } else {
        right_key_expressions->emplace_back(right_expr_tuple_0);
      }
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  LOG_DEBUG("plan_type: %d", optimized_plan->GetType());
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    auto recursive_pred = nlj_plan.Predicate();
    LOG_DEBUG("pred: %s", recursive_pred->ToString().c_str());
    if (IsPredicateTrue(recursive_pred)) {
      return optimized_plan;
    }
    // extract predicate recursively
    auto *expr = dynamic_cast<const LogicExpression *>(recursive_pred.get());
    while (expr != nullptr) {
      // right child expression must be ComparisonExpression, when current expression is LogicExpression.
      auto equal_expr = dynamic_cast<const ComparisonExpression *>(recursive_pred->GetChildAt(1).get());
      BUSTUB_ENSURE(equal_expr != nullptr, "expression should be ComparisonExpression")
      // handle equal expr
      ComparisonExpressionSpilt(equal_expr, &left_key_expressions, &right_key_expressions);

      recursive_pred = recursive_pred->GetChildAt(0);
      expr = dynamic_cast<const LogicExpression *>(recursive_pred.get());
    }
    auto last_expr = dynamic_cast<const ComparisonExpression *>(recursive_pred.get());
    BUSTUB_ENSURE(last_expr != nullptr, "expression should be ComparisonExpression")
    ComparisonExpressionSpilt(last_expr, &left_key_expressions, &right_key_expressions);

    // if(nlj_plan.Predicate() !=nullptr){
    //   LOG_DEBUG("left pred: %s", nlj_plan.Predicate()->GetChildAt(0)->ToString().c_str());
    //   LOG_DEBUG("right pred: %s", nlj_plan.Predicate()->GetChildAt(1)->ToString().c_str());
    // }

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_key_expressions, right_key_expressions, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
