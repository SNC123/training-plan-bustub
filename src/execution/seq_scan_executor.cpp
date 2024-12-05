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
#include <optional>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

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
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tuple_pair = table_iter_->GetTuple();
    auto base_meta = tuple_pair.first;
    auto result_tuple = tuple_pair.second;
    // added at P4T2.2
    // Reconstruct tuple according to read timestamp
    // auto txn_tmp_ts = txn->GetTransactionTempTs();
    auto txn_commit_ts = base_meta.ts_;
    auto txn_read_ts = txn->GetReadTs();
    auto txn_id = txn->GetTransactionId();
    LOG_DEBUG("txn_readable_id: %ld", txn->GetTransactionIdHumanReadable());
    // if tuple is modified by the current transaction, directly return
    // else do reconstruction
    if (txn_id != txn_commit_ts && txn_read_ts < txn_commit_ts) {
      auto current_undo_link = txn_mgr->GetUndoLink(result_tuple.GetRid());
      // is necessary to check valid???
      if (current_undo_link == std::nullopt || !current_undo_link->IsValid()) {
        table_iter_->operator++();
        continue;
      }
      std::vector<UndoLog> undo_logs;
      // check whether past tuple exists
      bool is_past_exist = false;
      do {
        auto current_undo_log = txn_mgr->GetUndoLog(current_undo_link.value());
        undo_logs.emplace_back(current_undo_log);
        if (txn_read_ts >= current_undo_log.ts_) {
          is_past_exist = true;
          break;
        }
        current_undo_link = current_undo_log.prev_version_;
      } while (current_undo_link->IsValid());
      if (!is_past_exist) {
        table_iter_->operator++();
        continue;
      }
      auto past_tuple = ReconstructTuple(&table_info_->schema_, result_tuple, base_meta, undo_logs);
      if (past_tuple == std::nullopt) {
        LOG_DEBUG("past_tuple is deleted");
        table_iter_->operator++();
        continue;
      }
      LOG_DEBUG("past_tuple: %s", past_tuple->ToString(&table_info_->schema_).c_str());
      result_tuple = past_tuple.value();
    } else {
      // skip deleted tuple
      if (base_meta.is_deleted_) {
        table_iter_->operator++();
        continue;
      }
    }

    // skip filterd tuple
    if (plan_->filter_predicate_ != nullptr) {
      auto filter_expr = plan_->filter_predicate_;
      auto schema = table_info_->schema_;
      auto value = filter_expr->Evaluate(&result_tuple, schema);
      // why Value is used in this way?
      if (!value.IsNull() && !value.GetAs<bool>()) {
        table_iter_->operator++();
        continue;
      }
    }
    LOG_DEBUG("tuple: %s", result_tuple.ToString(&GetOutputSchema()).c_str());
    *tuple = result_tuple;
    *rid = tuple->GetRid();
    table_iter_->operator++();
    return true;
  }

  return false;
}

}  // namespace bustub
