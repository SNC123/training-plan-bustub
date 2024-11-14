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

#include "execution/executors/seq_scan_executor.h"
#include <cstdlib>
#include <iostream>
#include <memory>
#include "common/logger.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  // get init iter
  auto &table_id = plan_->table_oid_;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(table_id);
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  // LOG_DEBUG("predicate: %s",plan_->filter_predicate_->ToString().c_str());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // handle bound situation
  if (table_iter_->IsEnd()) {
    return false;
  }
  // skip deleted tuple
  // warn!  waiting for testing, because of weak seqscan testcase
  auto result_tuple = table_iter_->GetTuple();
  if (result_tuple.first.is_deleted_) {
    return true;
  }
  // get tuple and rid
  *tuple = result_tuple.second;
  *rid = table_iter_->GetRID();
  table_iter_->operator++();
  return true;
}

}  // namespace bustub
