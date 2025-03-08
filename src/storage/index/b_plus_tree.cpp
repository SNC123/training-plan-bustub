#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx{};
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ctx.root_page_id_ = root_page_id;

  ReadPageGuard leaf_page_guard = FindLeafPageForRead(key, root_page_id, &ctx);
  const auto *current_page_as_leaf = leaf_page_guard.As<LeafPage>();
  BUSTUB_ENSURE(current_page_as_leaf != nullptr, "current page should be leaf page");
  return current_page_as_leaf->GetValueByKey(key, result, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForRead(const KeyType &key, page_id_t root_page_id, Context *ctx) -> ReadPageGuard {
  ctx->read_set_.emplace_back(bpm_->ReadPage(root_page_id));
  const auto *current_page = ctx->read_set_.back().As<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    const auto *current_page_as_internal = ctx->read_set_.back().As<InternalPage>();
    BUSTUB_ENSURE(current_page_as_internal != nullptr, "current page should be iternal page");
    page_id_t target_page_id = current_page_as_internal->GetTargetPageIdByKey(key, comparator_);

    ctx->read_set_.emplace_back(bpm_->ReadPage(target_page_id));
    current_page = ctx->read_set_.back().As<BPlusTreePage>();
  }

  ReadPageGuard leaf_page_guard = std::move(ctx->read_set_.back());
  ctx->read_set_.pop_back();
  return leaf_page_guard;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageForWrite(const KeyType &key, page_id_t root_page_id, Context *ctx) -> WritePageGuard {
  ctx->write_set_.emplace_back(bpm_->WritePage(root_page_id));
  const auto *current_page = ctx->write_set_.back().AsMut<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    const auto *current_page_as_internal = ctx->write_set_.back().As<InternalPage>();
    BUSTUB_ENSURE(current_page_as_internal != nullptr, "current page should be iternal page");
    page_id_t target_page_id = current_page_as_internal->GetTargetPageIdByKey(key, comparator_);

    ctx->write_set_.emplace_back(bpm_->WritePage(target_page_id));
    current_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  }

  WritePageGuard leaf_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  return leaf_page_guard;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx{};
  std::vector<ValueType> *result{};
  if (GetValue(key, result)) {
    return false;
  }

  WritePageGuard leaf_page_guard{};
  if (IsEmpty()) {
    leaf_page_guard = CreateLeafPage();
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = leaf_page_guard.GetPageId();
  } else {
    leaf_page_guard = FindLeafPageForWrite(key, GetRootPageId(), &ctx);
  }

  auto *leaf_page = leaf_page_guard.AsMut<LeafPage>();
  BUSTUB_ENSURE(leaf_page != nullptr, "current page should be leaf page");

  leaf_page->InsertKV(key, value, comparator_);

  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    SpiltLeafPage(std::move(leaf_page_guard), &ctx);
  }
  return true;
}

// added insert help function
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateLeafPage() -> WritePageGuard {
  page_id_t new_page_id = bpm_->NewPage();
  WritePageGuard new_leaf_page_guard = bpm_->WritePage(new_page_id);
  new_leaf_page_guard.AsMut<LeafPage>()->Init(leaf_max_size_);
  return new_leaf_page_guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateInternalPage() -> WritePageGuard {
  page_id_t new_page_id = bpm_->NewPage();
  WritePageGuard new_leaf_page_guard = bpm_->WritePage(new_page_id);
  new_leaf_page_guard.AsMut<InternalPage>()->Init(internal_max_size_);
  return new_leaf_page_guard;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SpiltLeafPage(WritePageGuard inserted_page_guard, Context *ctx) -> void {
  /* ======= Create new leaf page and migrate a half of elements ======== */
  WritePageGuard right_page_guard = CreateLeafPage();
  auto left_page = inserted_page_guard.AsMut<LeafPage>();
  auto right_page = right_page_guard.AsMut<LeafPage>();
  BUSTUB_ENSURE(left_page->GetSize() == left_page->GetMaxSize(), "spilted leaf page size should be maxsize ");

  right_page->SetNextPageId(left_page->GetNextPageId());
  left_page->SetNextPageId(right_page_guard.GetPageId());

  auto left_size = left_page->GetSize() / 2;
  auto right_size = left_page->GetSize() - left_size;
  for (auto idx = 0; idx < right_size; idx++) {
    right_page->InsertKV(left_page->KeyAt(left_size + idx), left_page->ValueAt(left_size + idx), comparator_);
  }
  left_page->IncreaseSize(-1 * right_size);
  /* ======= Create new leaf page and migrate a half of elements ======== */

  /* ======================= Upsert internal page ======================= */
  if (ctx->write_set_.empty()) {
    ctx->write_set_.emplace_back(CreateInternalPage());
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    LOG_DEBUG("new root page id : %d", ctx->write_set_.back().GetPageId());
    header_page->root_page_id_ = ctx->write_set_.back().GetPageId();
  }
  WritePageGuard internal_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto internal_page = internal_page_guard.AsMut<InternalPage>();

  internal_page->InsertLvKeyRv(inserted_page_guard.GetPageId(), right_page->KeyAt(0), right_page_guard.GetPageId(),
                               comparator_);
  if (internal_page->GetSize() > internal_page->GetMaxSize()) {
    SpiltInternalPage(std::move(internal_page_guard), ctx);
  }
  /* ======================= Upsert internal page ======================= */
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SpiltInternalPage(WritePageGuard inserted_page_guard, Context *ctx) -> void {
  /* ===== Create new internal page and migrate a half of elements ====== */
  WritePageGuard right_page_guard = CreateInternalPage();
  auto left_page = inserted_page_guard.AsMut<InternalPage>();
  auto right_page = right_page_guard.AsMut<InternalPage>();
  BUSTUB_ENSURE(left_page->GetSize() == left_page->GetMaxSize() + 1,
                "spilted internal page size should be maxsize + 1 ");

  auto right_size = (left_page->GetSize() - 1) / 2;
  auto left_size = (left_page->GetSize() - 1) - right_size - 1;
  for (auto idx = 0; idx < right_size; idx++) {
    right_page->InsertLvKeyRv(left_page->ValueAt(1 + left_size + idx), left_page->KeyAt(2 + left_size + idx),
                              left_page->ValueAt(2 + left_size + idx), comparator_);
  }
  left_page->IncreaseSize(-1 * right_size);
  /* ===== Create new internal page and migrate a half of elements ====== */

  /* ======================= Upsert internal page ======================= */
  if (ctx->write_set_.empty()) {
    ctx->write_set_.emplace_back(CreateInternalPage());
    auto header_page_guard = bpm_->WritePage(header_page_id_);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = ctx->write_set_.back().GetPageId();
  }
  WritePageGuard internal_page_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  auto internal_page = internal_page_guard.AsMut<InternalPage>();
  internal_page->InsertLvKeyRv(inserted_page_guard.GetPageId(), left_page->KeyAt(left_page->GetSize() - 1),
                               right_page_guard.GetPageId(), comparator_);
  left_page->IncreaseSize(-1);
  if (internal_page->GetSize() > internal_page->GetMaxSize()) {
    SpiltInternalPage(std::move(internal_page_guard), ctx);
  }
  /* ======================= Upsert internal page ======================= */
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  ReadPageGuard current_page_guard = bpm_->ReadPage(GetRootPageId());
  const auto *current_page = current_page_guard.As<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    const auto *current_page_as_internal = current_page_guard.As<InternalPage>();
    BUSTUB_ENSURE(current_page_as_internal != nullptr, "current page should be iternal page");
    page_id_t target_page_id = current_page_as_internal->ValueAt(0);

    current_page_guard = bpm_->ReadPage(target_page_id);
    current_page = current_page_guard.As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(current_page_guard.GetPageId(), 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  Context ctx{};
  ReadPageGuard leaf_page_guard = FindLeafPageForRead(key, GetRootPageId(), &ctx);
  auto leaf_page = leaf_page_guard.As<LeafPage>();
  for (auto idx = 0; idx < leaf_page->GetSize(); idx++) {
    if (comparator_(leaf_page->KeyAt(idx), key) > 0) {
      return INDEXITERATOR_TYPE(leaf_page_guard.GetPageId(), idx - 1, bpm_);
    }
  }
  return End();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::filesystem::path &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::filesystem::path &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::filesystem::path &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  if (root_page_id != INVALID_PAGE_ID) {
    auto guard = bpm->ReadPage(root_page_id);
    PrintTree(guard.GetPageId(), guard.template As<BPlusTreePage>());
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->ReadPage(internal->ValueAt(i));
      PrintTree(guard.GetPageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::filesystem::path &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->ReadPage(root_page_id);
  ToGraph(guard.GetPageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->ReadPage(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.GetPageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->ReadPage(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.GetPageId() << " " << internal_prefix
              << child_guard.GetPageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.GetPageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.GetPageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.GetPageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->ReadPage(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
