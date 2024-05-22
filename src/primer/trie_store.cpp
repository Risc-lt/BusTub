#include "primer/trie_store.h"
#include <mutex>
#include <optional>
#include "common/exception.h"
#include "primer/trie.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // throw NotImplementedException("TrieStore::Get is not implemented.");

  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  Trie cur_root;
  { // scope for root_guard to guard the root_ from being modified
    std::lock_guard<std::mutex> root_guard(root_lock_);
    cur_root = root_;
  }

  // (2) Lookup the value in the trie.
  auto val = cur_root.Get<T>(key);

  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  if(val != nullptr){
    return ValueGuard<T>(cur_root, *val);
  } else {
    return std::nullopt;
  }

}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // throw NotImplementedException("TrieStore::Put is not implemented.");

  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.

  // Get write_lock_ to avoid being interrupted by other writers
  std::lock_guard<std::mutex> w_guard{write_lock_};

  // Get the root_ to maintain the current state of the trie 
  Trie local_root;
  {
    std::lock_guard<std::mutex> guard{root_lock_};
    local_root = root_;
  }

  // Put the new key-value pair and update the current root to modified version
  auto new_root = local_root.Put(key, std::move(value));
  {
    std::lock_guard<std::mutex> r_guard{root_lock_};
    root_ = new_root;
  }
}

void TrieStore::Remove(std::string_view key) {
  // throw NotImplementedException("TrieStore::Remove is not implemented.");

  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  
  // Get write_lock_ to avoid being interrupted by other writers
  std::lock_guard<std::mutex> w_guard{write_lock_};

  // Get the root_ to maintain the current state of the trie 
  Trie local_root;
  {
    std::lock_guard<std::mutex> guard{root_lock_};
    local_root = root_;
  }

  // Remove the target key and update the current root to modified version
  auto new_root = local_root.Remove(key);
  {
    std::lock_guard<std::mutex> r_guard{root_lock_};
    root_ = new_root;
  }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
