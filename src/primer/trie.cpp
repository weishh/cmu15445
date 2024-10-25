#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  std::shared_ptr<const TrieNode> root = GetRoot();
  if (root == nullptr) {
    return nullptr;
  }
  int len = key.size();
  for (int i = 0; i < len; ++i) {
    char c = key[i];
    if (root->children_.find(c) == root->children_.end()) {
      return nullptr;
    }
    root = root->children_.find(c)->second;
  }
  // 判断该节点是值节点且类型一致，也即能够进行指针的类型转换，const TrieNode 转为 const TrieNodeWithValue<T>
  if (root->is_value_node_ && std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root)) {
    auto ptr = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root);
    return ptr->value_.get();
  }
  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  int key_sz = key.size();
  std::shared_ptr<const TrieNode> this_root = GetRoot();
  std::shared_ptr<const TrieNode> new_root;
  if (this_root != nullptr) {
    // key = ""
    if (key_sz) {
      new_root = std::shared_ptr<const TrieNode>(this_root->Clone());
    } else {
      new_root =
          std::make_shared<const TrieNodeWithValue<T>>(this_root->children_, std::make_shared<T>(std::move(value)));
      return Trie(new_root);
    }
  } else {
    if (key_sz) {
      new_root = std::make_shared<const TrieNode>();
    } else {
      new_root = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      return Trie(new_root);
    }
  }
  Trie result(new_root);

  // 到这里的keysz都是大于0

  for (int i = 0; i < key_sz - 1; ++i) {
    auto child_mp = new_root->children_;
    if (child_mp.find(key[i]) != child_mp.end()) {
      auto finded_pair = child_mp.find(key[i]);
      new_root->children_.find(key[i])->second = std::shared_ptr<const TrieNode>(finded_pair->second->Clone());
    } else {
      new_root->children_.insert({key[i], std::make_shared<const TrieNode>()});
    }
    new_root = new_root->children_.find(key[i])->second;
  }

  // key的最后一个字符，如果是能找到表示要复制children，反之直接创建一个新的TrieNodeWithValue
  if (new_root->children_.find(key[key_sz - 1]) != new_root->children_.end()) {
    new_root->children_.find(key[key_sz - 1])->second = std::make_shared<const TrieNodeWithValue<T>>(
        new_root->children_.find(key[key_sz - 1])->second->children_, std::make_shared<T>(std::move(value)));
  } else {
    new_root->children_.insert(
        {key[key_sz - 1], std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))});
  }
  return result;
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  if (!GetRoot()) {
    return Trie{};
  }
  std::shared_ptr<const TrieNode> root = std::shared_ptr<const TrieNode>(GetRoot()->Clone());
  if (key.empty()) {
    if (root->is_value_node_) {
      return Trie(std::make_shared<const TrieNode>(root->children_));
    }
    return Trie(root);
  }
  root = RemoveHelp(root, key, 0);
  return Trie(root);

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

auto Trie::RemoveHelp(std::shared_ptr<const TrieNode> root, std::string_view key, int index) const
    -> std::shared_ptr<const TrieNode> {
  if (index < static_cast<int>(key.size()) && (root->children_.count(key[index]) != 0U)) {
    root->children_.find(key[index])->second =
        std::shared_ptr<const TrieNode>(root->children_.find(key[index])->second->Clone());
    root->children_.find(key[index])->second = RemoveHelp(root->children_.find(key[index])->second, key, index + 1);
    if (root->children_.find(key[index])->second == nullptr) {
      root->children_.erase(key[index]);
    }
  } else {  //
    if (root->is_value_node_) {
      if (!root->children_.empty()) {
        return std::make_shared<const TrieNode>(root->children_);
      }
      return nullptr;
    }
    return root;
  }
  // std::cout << root.get() << std::endl;
  if (!root->children_.empty() || root->is_value_node_) {
    return root;
  }
  return nullptr;
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
