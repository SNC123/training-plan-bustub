#include "execution/execution_common.h"
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::optional<Tuple> result_tuple = base_tuple;
  // when undo_logs is empty AND AND AND base meta is deleted (think carefully!!!)
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    result_tuple = std::nullopt;
  }
  for (auto &log : undo_logs) {
    if (log.is_deleted_) {
      result_tuple = std::nullopt;
      continue;
    }
    // update matched column
    auto base_column_num = schema->GetColumnCount();
    std::vector<Column> columns;
    for (size_t idx = 0; idx < base_column_num; ++idx) {
      if (log.modified_fields_[idx]) {
        columns.emplace_back(schema->GetColumn(idx));
      }
    }
    auto part_schema = Schema(columns);
    auto modified_idx = 0;
    std::vector<Value> values;
    for (size_t idx = 0; idx < base_column_num; ++idx) {
      if (log.modified_fields_[idx]) {
        values.emplace_back(log.tuple_.GetValue(&part_schema, modified_idx++));
      } else {
        // is necessary to check result tuple nullopt???
        values.emplace_back(result_tuple->GetValue(schema, idx));
      }
    }
    result_tuple = Tuple({values, schema});
  }
  return result_tuple;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  // store all outputs in a string to avoid being divided into several blocks
  std::string result_string;
  fmt::println(stderr, "debug_hook: {}", info);
  for (const auto &txn_iter : txn_mgr->txn_map_) {
    auto log_num = txn_iter.second->GetUndoLogNum();
    if (log_num > 0) {
      auto txn_str = fmt::format("txn{}:\n", (txn_iter.first ^ TXN_START_ID));
      result_string += txn_str;
    }
    for (size_t idx = 0; idx < log_num; ++idx) {
      auto undo_log = txn_iter.second->GetUndoLog(idx);
      auto log_str =
          fmt::format("idx:{} ts:{} prev_log idx:{} prev_txn:{} is_deleted:{}\n", idx, undo_log.ts_,
                      undo_log.prev_version_.prev_log_idx_, undo_log.prev_version_.prev_txn_, undo_log.is_deleted_);
      result_string += log_str;
    }
  }
  auto table_iter = table_heap->MakeIterator();
  while (!table_iter.IsEnd()) {
    auto rid = table_iter.GetRID();
    auto tuple = table_iter.GetTuple().second;
    auto tuple_meta = table_iter.GetTuple().first;
    int64_t ts = tuple_meta.ts_;
    auto ts_str = std::to_string(ts);
    if (ts > TXN_START_ID) {
      ts_str = fmt::format("txn{}", (ts ^ TXN_START_ID));
    }
    bool is_del = tuple_meta.is_deleted_;
    std::string del_mark;
    if (is_del) {
      del_mark = "<del marker>";
    }
    // print table heap data
    result_string += fmt::format("RID={}/{} ts={} {} {}\n", rid.GetPageId(), rid.GetSlotNum(), ts_str, del_mark,
                                 tuple.ToString(&table_info->schema_));
    // print log
    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link != std::nullopt && undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(undo_link.value());
      if (!undo_log.has_value()) {
        break;
      }
      // build timestamp string
      auto ts_str = undo_log->ts_;
      // build del mark string
      bool is_del = undo_log->is_deleted_;
      std::string del_mark;
      if (is_del) {
        del_mark = "<del>";
      }
      // build previous transaction string
      std::string prev_txn_str = std::to_string(undo_link->prev_txn_ ^ TXN_START_ID);
      // build undo log tuple string
      std::string tuple_str;
      auto base_column_num = table_info->schema_.GetColumnCount();
      std::vector<Column> columns;
      for (size_t idx = 0; idx < base_column_num; ++idx) {
        if (undo_log->modified_fields_[idx]) {
          columns.emplace_back(table_info->schema_.GetColumn(idx));
        }
      }
      auto modified_idx = 0;
      auto part_schema = Schema(columns);
      std::vector<Value> values;
      for (size_t idx = 0; idx < base_column_num; ++idx) {
        if (undo_log->modified_fields_[idx]) {
          tuple_str += undo_log->tuple_.GetValue(&part_schema, modified_idx++).ToString();
        } else {
          tuple_str += "_";
        }
        if (idx != base_column_num - 1) {
          tuple_str += ", ";
        }
      }
      result_string += fmt::format("  txn{}@ ts={} {} ({}) \n", prev_txn_str, ts_str, del_mark, tuple_str);
      undo_link = undo_log->prev_version_;
    }
    ++table_iter;
  }
  fmt::println(stderr, "{}", result_string);
  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

// atomic set in_progress in version link
auto LockVersionLink(TransactionManager *txn_mgr, RID rid) -> bool {
  std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(rid);
  if (version_link.has_value()) {
    return txn_mgr->UpdateVersionLink(rid, VersionUndoLink{version_link->prev_, true},
                                      [version_link](std::optional<VersionUndoLink> origin_version_link) -> bool {
                                        // To ensure version link avaliable and unchanged
                                        return origin_version_link.has_value() && !origin_version_link->in_progress_ &&
                                               origin_version_link->prev_ == version_link->prev_;
                                      });
  }
  return txn_mgr->UpdateVersionLink(
      rid, {VersionUndoLink{UndoLink{}, true}},
      [](std::optional<VersionUndoLink> origin_version_link) -> bool { return !origin_version_link.has_value(); });
}
// atomic unset in_progress in version link
auto UnlockVersionLink(TransactionManager *txn_mgr, RID rid) -> bool {
  std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(rid);
  if (version_link.has_value()) {
    return txn_mgr->UpdateVersionLink(rid, VersionUndoLink{version_link->prev_, false});
  }
  LOG_INFO("[UnlockVersionLink] Unlock failed....You are unlocking EMPTY verion link");
  return false;
}

auto IsWriteWriteConflict(Transaction *txn, timestamp_t meta_ts) -> bool {
  return meta_ts != txn->GetTransactionTempTs() && meta_ts > txn->GetReadTs();
}
// for self modification case, update first undo log in UndoVersionLink if exists
auto ModifyHeadUndoLog(Transaction *txn, TransactionManager *txn_mgr, const Schema &schema, RID rid,
                       const Tuple &old_tuple, const Tuple &new_tuple) -> void {
  auto version_link = txn_mgr->GetVersionLink(rid);
  if (version_link.has_value()) {
    auto header_log = txn_mgr->GetUndoLog(version_link->prev_);

    // get old partial schema
    std::vector<Column> old_partial_columns;
    auto old_column_count = schema.GetColumnCount();
    for (uint32_t idx = 0; idx < old_column_count; idx++) {
      if (header_log.modified_fields_[idx]) {
        old_partial_columns.emplace_back(schema.GetColumn(idx));
      }
    }
    auto old_partial_schema = Schema{old_partial_columns};

    // only add new column
    std::vector<bool> modified_fields;
    const auto column_count = schema.GetColumnCount();
    auto old_partial_count = 0;
    std::vector<Value> values;
    std::vector<Column> columns;
    for (uint32_t idx = 0; idx < column_count; ++idx) {
      if (header_log.modified_fields_[idx]) {
        modified_fields.emplace_back(true);
        values.emplace_back(header_log.tuple_.GetValue(&old_partial_schema, old_partial_count));
        columns.emplace_back(old_partial_schema.GetColumn(old_partial_count));
        old_partial_count++;
      } else {
        auto old_value = old_tuple.GetValue(&schema, idx);
        auto new_value = new_tuple.GetValue(&schema, idx);
        if (!old_value.CompareExactlyEquals(new_value)) {
          modified_fields.emplace_back(true);
          values.emplace_back(old_value);
          columns.emplace_back(schema.GetColumn(idx));
        } else {
          modified_fields.emplace_back(false);
        }
      }
    }

    // modify first undo log
    auto partial_schema = Schema{columns};
    auto partial_tuple = Tuple{values, &partial_schema};
    header_log.modified_fields_ = modified_fields;
    header_log.tuple_ = partial_tuple;
    auto header_log_txn = txn_mgr->txn_map_[version_link->prev_.prev_txn_];
    header_log_txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, header_log);
  }
}
auto CheckConflictAndLockLink(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info, RID rid,
                              std::string location_str) -> void {
  if (IsWriteWriteConflict(txn, table_info->table_->GetTupleMeta(rid).ts_)) {
    UnlockVersionLink(txn_mgr, rid);
    txn->SetTainted();
    throw ExecutionException(fmt::format("[{}] write-write conflict!", location_str));
  }
  bool is_locked = LockVersionLink(txn_mgr, rid);
  if (!is_locked) {
    txn->SetTainted();
    throw ExecutionException(fmt::format("[{}] other threads are using version link !", location_str));
  }
  if (IsWriteWriteConflict(txn, table_info->table_->GetTupleMeta(rid).ts_)) {
    UnlockVersionLink(txn_mgr, rid);
    txn->SetTainted();
    throw ExecutionException(fmt::format("[{}] write-write conflict!", location_str));
  }
}

auto UpsertTuple(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info,
                 const std::vector<IndexInfo *> &index_info_vector, Tuple &tuple) -> bool {
  auto schema = table_info->schema_;
  if (index_info_vector.size() <= 1) {
    // used for upsert tuple(true->insert, false->update)
    bool is_insert_new_tuple = true;
    // tuple in pk_rid needs updated
    RID pk_rid{};
    for (auto index_info : index_info_vector) {
      // determine whether new tuple has an index
      std::vector<RID> found_rids{};
      auto target_key = tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->ScanKey(target_key, &found_rids, nullptr);

      if (!found_rids.empty()) {
        // only consider the case of primary key
        if (index_info->is_primary_key_) {
          pk_rid = found_rids[0];
          // if there is undeleted tuple in table heap, do abort.
          if (!table_info->table_->GetTupleMeta(pk_rid).is_deleted_) {
            txn->SetTainted();
            throw ExecutionException("[InsertExecutor] mapped tuple has existed!");
          }
          is_insert_new_tuple = false;
        } else {
          // TODO(low priority): Compatibility with Project 3
        }
      }
    }
    if (is_insert_new_tuple) {
      auto opt_rid = table_info->table_->InsertTuple({txn->GetTransactionTempTs(), false}, tuple);
      if (!opt_rid.has_value()) {
        return false;
      }
      auto new_rid = opt_rid.value();

      // if index does not exist, insert new index
      for (auto index_info : index_info_vector) {
        auto target_key = tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // bool is_index_inserted = index_info->index_->InsertEntry(target_key, *rid, nullptr);
        if (!index_info->index_->InsertEntry(target_key, new_rid, nullptr)) {
          // detect multiple index map to same tuple
          txn->SetTainted();
          throw ExecutionException("[InsertExecutor] multiple indexes map to same tuple!");
        }
      }

      txn->AppendWriteSet(table_info->oid_, new_rid);
      txn_mgr->UpdateVersionLink(new_rid, std::nullopt);
    } else {
      // if deleted by current txn, there is no log updating because of previous deletion
      auto old_meta_ts = table_info->table_->GetTupleMeta(pk_rid).ts_;
      if (old_meta_ts == txn->GetTransactionId()) {
        // insert new tuple in old rid
        table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, tuple, pk_rid);
        txn->AppendWriteSet(table_info->oid_, pk_rid);
      } else {
        CheckConflictAndLockLink(txn, txn_mgr, table_info, pk_rid, "InsertExecutor");

        // build undo log(empty modified fields because of previous deletion)
        auto column_count = schema.GetColumnCount();
        std::vector<bool> modified_fields(column_count);
        std::vector<Value> values(column_count);
        for (size_t idx = 0; idx < column_count; ++idx) {
          modified_fields[idx] = false;
        }
        old_meta_ts = table_info->table_->GetTupleMeta(pk_rid).ts_;
        auto new_undo_log = UndoLog{true, modified_fields, Tuple{}, old_meta_ts, UndoLink{}};
        std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(pk_rid);
        if (version_link.has_value()) {
          auto prev_link = version_link->prev_;
          new_undo_log.prev_version_ = prev_link;
        }

        // build and update version link
        std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
        auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
        new_version_link->in_progress_ = true;
        table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, tuple, pk_rid);
        txn->AppendWriteSet(table_info->oid_, pk_rid);
        txn_mgr->UpdateVersionLink(pk_rid, new_version_link);

        UnlockVersionLink(txn_mgr, pk_rid);
      }
    }

  } else {
    // TODO(low priority): Compatibility with Project 3
  }

  return true;
}

auto DeleteTuple(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info, RID rid, Tuple &tuple)
    -> void {
  // if tuple is not in table heap, it must be write-write conflict (think carefully!!!)
  if (tuple.GetRid().GetPageId() == INVALID_PAGE_ID) {
    txn->SetTainted();
    throw ExecutionException("[DeleteExecutor] Detect write-write conflict !");
  }

  auto meta_ts = table_info->table_->GetTupleMeta(rid).ts_;
  // check if tuple is being modified
  if (meta_ts == txn->GetTransactionId()) {
    // maintain undo link
    ModifyHeadUndoLog(txn, txn_mgr, table_info->schema_, rid, tuple, tuple);
    // delete old tuple(just set is_deleted to true)
    table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, rid);
    txn->AppendWriteSet(table_info->oid_, rid);
  } else {
    CheckConflictAndLockLink(txn, txn_mgr, table_info, rid, "DeleteExecutor");

    // build undo log(full modified fields)
    std::vector<bool> modified_fields;
    auto column_count = table_info->schema_.GetColumnCount();
    for (uint32_t i = 0; i < column_count; ++i) {
      modified_fields.emplace_back(true);
    }
    meta_ts = table_info->table_->GetTupleMeta(rid).ts_;
    Tuple re_tuple = table_info->table_->GetTuple(rid).second;
    auto new_undo_log = UndoLog{false, modified_fields, re_tuple, meta_ts, UndoLink{}};

    // build and update version link
    std::optional<VersionUndoLink> version_link = txn_mgr->GetVersionLink(rid);
    if (version_link.has_value()) {
      auto prev_link = version_link->prev_;
      new_undo_log.prev_version_ = prev_link;
    }
    std::optional<UndoLink> new_undo_link = txn->AppendUndoLog(new_undo_log);
    auto new_version_link = VersionUndoLink::FromOptionalUndoLink(new_undo_link);
    new_version_link->in_progress_ = true;
    txn_mgr->UpdateVersionLink(rid, new_version_link);
    table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, rid);
    txn->AppendWriteSet(table_info->oid_, rid);

    UnlockVersionLink(txn_mgr, rid);
  }
}
}  // namespace bustub
