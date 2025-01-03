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
#include <optional>
#define LOG_LEVEL LOG_LEVEL_OFF
#include "catalog/schema.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/index_scan_executor.h"

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
    // modified at P4T4.2
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto tuple_pair = table_info_->table_->GetTuple(rid_results_[cursor_]);
    auto base_meta = tuple_pair.first;
    auto result_tuple = tuple_pair.second;
    // Reconstruct tuple according to read timestamp
    auto txn_commit_ts = base_meta.ts_;
    auto txn_read_ts = txn->GetReadTs();
    auto txn_id = txn->GetTransactionId();
    LOG_DEBUG("txn_readable_id: %ld", txn->GetTransactionIdHumanReadable());

    // if tuple is modified by the current transaction, directly return
    // else do reconstruction
    if (txn_id != txn_commit_ts && txn_read_ts < txn_commit_ts) {
      std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(result_tuple.GetRid());
      // check version link exists or not
      if (version_link == std::nullopt) {
        ++cursor_;
        continue;
      }
      std::optional<UndoLink> current_undo_link = version_link->prev_;
      if (current_undo_link == std::nullopt || !current_undo_link->IsValid()) {
        ++cursor_;
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
        ++cursor_;
        continue;
      }
      auto past_tuple = ReconstructTuple(&table_info_->schema_, result_tuple, base_meta, undo_logs);
      if (past_tuple == std::nullopt) {
        LOG_DEBUG("past_tuple is deleted");
        ++cursor_;
        continue;
      }
      LOG_DEBUG("past_tuple: %s", past_tuple->ToString(&table_info_->schema_).c_str());
      result_tuple = past_tuple.value();
    } else {
      // skip deleted tuple
      if (base_meta.is_deleted_) {
        ++cursor_;
        continue;
      }
    }
    // // skip deleted tuple(s)
    // // warn!  waiting for testing, because of weak seqscan testcase
    // auto tuple_pair = table_info_->table_->GetTuple(rid_results_[cursor_]);
    // if (tuple_pair.first.is_deleted_) {
    //   ++cursor_;
    //   continue;
    // }
    // skip filterd tuple(s)
    if (plan_->filter_predicate_ != nullptr) {
      auto filter_expr = plan_->filter_predicate_;
      auto schema = table_info_->schema_;
      auto value = filter_expr->Evaluate(&result_tuple, schema);
      // why Value is used in this way?
      if (!value.IsNull() && !value.GetAs<bool>()) {
        ++cursor_;
        continue;
      }
    }
    *tuple = result_tuple;
    *rid = rid_results_[cursor_];
    ++cursor_;
    return true;
  }
  return false;
}

}  // namespace bustub
