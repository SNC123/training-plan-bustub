#define LOG_LEVEL LOG_LEVEL_OFF

#include "binder/tokens.h"
#include "common/logger.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ == nullptr || seq_scan_plan.filter_predicate_->children_.empty()) {
      return optimized_plan;
    }
    // optimize to indexcasn iff single predicate on the indexed column
    LOG_DEBUG("pred: %s", seq_scan_plan.filter_predicate_->GetChildAt(0)->ToString().c_str());
    bool is_single = ((seq_scan_plan.filter_predicate_->children_[0]->children_.empty()) &&
                      (seq_scan_plan.filter_predicate_->children_[1]->children_.empty()));
    if (is_single) {
      const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      // check equal_comparsion
      if (auto *expr = dynamic_cast<ComparisonExpression *>(seq_scan_plan.filter_predicate_.get()); expr != nullptr) {
        if (expr->comp_type_ == ComparisonType::Equal) {
          if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
              left_expr != nullptr) {
            if (auto *right_expr = dynamic_cast<ConstantValueExpression *>(expr->children_[1].get());
                right_expr != nullptr) {
              // get col corresponding to preficate
              std::vector<uint32_t> seq_scan_column_ids;
              seq_scan_column_ids.push_back(left_expr->GetColIdx());
              // find match index
              for (const auto *index : indices) {
                const auto &columns = index->index_->GetKeyAttrs();
                // check match or not
                if (seq_scan_column_ids == columns) {
                  return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                             index->index_oid_, seq_scan_plan.filter_predicate_,
                                                             right_expr);
                }
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
