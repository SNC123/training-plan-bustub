//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define LOG_LEVEL LOG_LEVEL_OFF
#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"
#include "common/logger.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  cursor_ = 0;
  rid_results_.clear();
  auto table_id = plan_->table_oid_;
  auto index_id = plan_->index_oid_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());

  std::vector<Value> values{};
  LOG_DEBUG("pred_key: %s", plan_->pred_key_->ToString().c_str());
  auto value = plan_->pred_key_->Evaluate(nullptr, index_info_->key_schema_);
  LOG_DEBUG("value: %s", value.ToString().c_str());
  values.push_back(value);
  htable_->ScanKey(Tuple(values, &index_info_->key_schema_), &rid_results_, nullptr);
  LOG_DEBUG("found key size: %zu", rid_results_.size());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < rid_results_.size()) {
    // skip deleted tuple(s)
    // warn!  waiting for testing, because of weak seqscan testcase
    auto tuple_pair = table_info_->table_->GetTuple(rid_results_[cursor_]);
    if (tuple_pair.first.is_deleted_) {
      ++cursor_;
      continue;
    }
    // skip filterd tuple(s)
    if (plan_->filter_predicate_ != nullptr) {
      auto filter_expr = plan_->filter_predicate_;
      auto schema = table_info_->schema_;
      auto value = filter_expr->Evaluate(&tuple_pair.second, schema);
      // why Value is used in this way?
      if (!value.IsNull() && !value.GetAs<bool>()) {
        ++cursor_;
        continue;
      }
    }
    *tuple = tuple_pair.second;
    *rid = rid_results_[cursor_];
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
