//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define LOG_LEVEL LOG_LEVEL_OFF

#include "execution/executors/seq_scan_executor.h"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include "catalog/catalog.h"
#include "common/logger.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  // get init iter
  auto table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  table_iter_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
  if (plan_->filter_predicate_ != nullptr) {
    LOG_DEBUG("typeid: %d", plan_->filter_predicate_->GetReturnType());
    LOG_DEBUG("predicate: %s", plan_->filter_predicate_->ToString().c_str());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!table_iter_->IsEnd()) {
    // skip deleted tuple(s)
    // warn!  waiting for testing, because of weak seqscan testcase
    auto tuple_pair = table_iter_->GetTuple();
    if (tuple_pair.first.is_deleted_) {
      table_iter_->operator++();
      continue;
    }
    // skip filterd tuple(s)
    if (plan_->filter_predicate_ != nullptr) {
      auto filter_expr = plan_->filter_predicate_;
      auto schema = table_info_->schema_;
      auto value = filter_expr->Evaluate(&tuple_pair.second, schema);
      // why Value is used in this way?
      if (!value.IsNull() && !value.GetAs<bool>()) {
        table_iter_->operator++();
        continue;
      }
    }
    *tuple = tuple_pair.second;
    *rid = table_iter_->GetRID();
    table_iter_->operator++();
    return true;
  }

  return false;
}

}  // namespace bustub
