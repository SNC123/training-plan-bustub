#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto current_node_ptr = root_;

  // special case: empty key
  if(key.empty()) {
      auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current_node_ptr.get());
      if( value_node == nullptr ) { return nullptr; }
      return value_node->value_.get();
  }

  // todo: is suitable to use int?
  for(int pos = 0; pos < static_cast<int>(key.size()); pos++) {
    auto next_node_iter = current_node_ptr->children_.find(key[pos]);
    if( next_node_iter == root_->children_.end() ) {
      return nullptr;
    }
    current_node_ptr = next_node_iter->second;
    // if walk to the last char
    if( pos == static_cast<int>( key.size()-1 ) ) {
      // value don't exist
      if(!current_node_ptr->is_value_node_) { return nullptr; }
      // todo: learn usage of shared_ptr
      auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current_node_ptr.get());
      if( value_node == nullptr ) { return nullptr; }
      return value_node->value_.get();
    }
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  // how to build new CoW trie:
  // 1.call Trie::Get to varify whether node exists or not (maybe hash is better) or just overwirte????
  // 2.create nodes in key path
  // 3.pointer to the other original nodes

  auto new_root = TrieNode();
  auto current_old_node_ptr = root_;
  auto current_new_node_ptr = std::make_shared<const TrieNode>(new_root);

  // call Trie::Get to varify whether node exists or not
  // todo: how to solve comparision on MoveBlocked? 
  // if( *Trie::Get<T>(key) == value) {

  // }

  // special case: empty trie
  if(current_old_node_ptr == nullptr) {
    for(int pos = 0; pos < static_cast<int>(key.size()); pos++) {
     current_new_node_ptr->children_[key[pos]] = std::make_shared<const TrieNode>(TrieNode());
     current_new_node_ptr = current_new_node_ptr->children_[key[pos]];
    }
  }else{

  }




  // }
  return Trie(std::make_shared<const TrieNode>(new_root));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
