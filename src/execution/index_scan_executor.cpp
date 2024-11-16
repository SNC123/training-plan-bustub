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
#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"
#include "common/logger.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  auto table_id = plan_->table_oid_;
  auto index_id = plan_->index_oid_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());

  std::vector<Value> values{};
  auto value = plan_->pred_key_->Evaluate(nullptr, Schema({}));
  values.push_back(value);
  htable_->ScanKey(Tuple(values, &index_info_->key_schema_), rid_results_, nullptr);
  LOG_DEBUG("found key size: %zu", rid_results_->size());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // htable_->
  // index_info_->index_->ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction)
  return false;
}

}  // namespace bustub
