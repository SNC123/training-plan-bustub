//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define LOG_LEVEL LOG_LEVEL_OFF
#include "concurrency/transaction_manager.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn->read_ts_.store(last_commit_ts_.load());
  txn->state_ = TransactionState::RUNNING;
  running_txns_.AddTxn(txn_ref->read_ts_);
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  // txn->commit_ts_ = last_commit_ts_.load(std::memory_order_relaxed) + 1;
  txn->commit_ts_ = last_commit_ts_ + 1;
  auto write_set = txn->GetWriteSets();
  // update tuple ts (tmp_ts -> commit_ts)
  for (const auto &ws : write_set) {
    auto table_id = ws.first;
    auto talbe_info = catalog_->GetTable(table_id);
    for (auto rid : ws.second) {
      auto tuple_meta = talbe_info->table_->GetTupleMeta(rid);
      tuple_meta.ts_ = txn->commit_ts_;
      talbe_info->table_->UpdateTupleMeta(tuple_meta, rid);
    }
  }
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  // added at P4T3.2, why update in the end???
  ++last_commit_ts_;
  // WE CAN'T DO THAT, BACAUSE OF RESOURCE RELEASE AND LATER REFERENCE
  // txn_id is unique, so it's right to GC later?
  // txn_map_.erase(txn->txn_id_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
  // txn_map_.erase(txn->txn_id_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t water_mark = GetWatermark();
  std::vector<txn_id_t> delete_txn_ids;
  for (const auto &txn_pair : txn_map_) {
    auto txn = txn_pair.second;
    auto txn_id = txn->GetTransactionId();
    auto write_set = txn->write_set_;
    // check it is possible to be used in the future(via watermark)
    bool is_useful = false;
    LOG_DEBUG("txn_id:%ld", txn_id ^ TXN_START_ID);

    if (txn->GetTransactionState() == TransactionState::RUNNING ||
        txn->GetTransactionState() == TransactionState::TAINTED) {
      continue;
    }
    if (txn->undo_logs_.empty()) {
      delete_txn_ids.emplace_back(txn_id);
      continue;
    }

    for (const auto &ws_pair : write_set) {
      auto rids = ws_pair.second;
      for (auto rid : rids) {
        // bool is_past_exist = false;
        // check table heap
        auto table_info = catalog_->GetTable(ws_pair.first);
        auto table_heap_ts = table_info->table_->GetTupleMeta(rid).ts_;
        if (table_heap_ts <= water_mark) {
          // is_past_exist = true;
          continue;
        }
        // check log
        auto current_link = GetUndoLink(rid);
        txn_id_t current_txn_id;
        UndoLog current_log;
        if (current_link.has_value() && current_link->IsValid()) {
          do {
            current_txn_id = current_link->prev_txn_;
            current_log = GetUndoLog(current_link.value());
            LOG_DEBUG("log ts %ld", current_log.ts_);
            LOG_DEBUG("log txn id: %ld", current_txn_id ^ TXN_START_ID);
            if (current_txn_id == txn_id) {
              is_useful = true;
            }
            if (current_log.ts_ <= water_mark) {
              // is_past_exist = true;
              break;
            }
            current_link = current_log.prev_version_;
          } while (current_link->IsValid());
          // LOG_DEBUG("txn commit ts %ld", txn->GetCommitTs());
          // if(!is_past_exist || txn->GetCommitTs() >= current_log.ts_) {
          //   is_useful = true;
          // }
        }
      }
    }
    if (!is_useful) {
      delete_txn_ids.emplace_back(txn_id);
    }
  }
  for (const auto &id : delete_txn_ids) {
    LOG_DEBUG("delete txn %ld", id ^ TXN_START_ID);
    txn_map_.erase(id);
  }
}

}  // namespace bustub
