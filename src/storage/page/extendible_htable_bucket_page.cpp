//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  auto defmaxsz = HTableBucketArraySize(sizeof(std::pair<K, V>));
  max_size_ = std::min(max_size, static_cast<uint32_t>(defmaxsz));
  memset(array_, 0, sizeof(array_));
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (uint32_t i = 0; i < Size(); ++i) {
    if (!cmp(key, array_[i].first)) {
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (IsFull()) {
    return false;
  }
  for (uint32_t i = 0; i < Size(); ++i) {
    if (!cmp(key, array_[i].first)) {
      return false;
    }
  }
  auto idx = Size();
  ++size_;
  array_[idx] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  uint32_t l = 0;
  bool flag = false;
  for (uint32_t r = 0; r < Size(); ++r) {
    if (cmp(key, array_[r].first) != 0) {
      array_[l++] = array_[r];
    } else {
      flag = true;
    }
  }
  if (flag) {
    --size_;
  }
  return flag;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  if (bucket_idx >= Size()) {
    return;
  }
  uint32_t l = bucket_idx;
  for (uint32_t r = bucket_idx + 1; r < Size(); ++r) {
    array_[l++] = array_[r];
  }
  --size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  if (bucket_idx >= Size()) {
    return K{};
  }
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  if (bucket_idx >= Size()) {
    return V{};
  }
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  if (bucket_idx >= Size()) {
    throw Exception("invalid bucket_idx");
  }
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return max_size_ == size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ > 0 ? false : true;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
