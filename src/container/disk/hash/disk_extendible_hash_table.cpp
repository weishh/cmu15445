//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  auto header_page = bpm_->NewPageGuarded(&header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  auto header_page = bpm_->FetchPageRead(header_page_id_).template As<ExtendibleHTableHeaderPage>();
  auto hash = hash_fn_.GetHash(key);
  auto directory_pgid = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  if (directory_pgid == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page = bpm_->FetchPageRead(directory_pgid).template As<ExtendibleHTableDirectoryPage>();
  auto bucket_pgid = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
  if (bucket_pgid == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_page = bpm_->FetchPageRead(bucket_pgid).template As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto header_page = bpm_->FetchPageWrite(header_page_id_).template AsMut<ExtendibleHTableHeaderPage>();
  auto hash = hash_fn_.GetHash(key);
  auto directory_pgid = header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  // 目录页没有，进行创建
  if (directory_pgid == INVALID_PAGE_ID) {
    auto dir_pg = bpm_->NewPageGuarded(&directory_pgid);
    if (directory_pgid == INVALID_PAGE_ID) {
      return false;
    }
    header_page->SetDirectoryPageId(header_page->HashToDirectoryIndex(hash), directory_pgid);
    auto directory_page = dir_pg.UpgradeWrite().template AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    auto bucket_pgid = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
    auto bk_pg = bpm_->NewPageGuarded(&bucket_pgid);
    if (bucket_pgid == INVALID_PAGE_ID) {
      return false;
    }
    directory_page->SetBucketPageId(directory_page->HashToBucketIndex(hash), bucket_pgid);
    auto bucket_page = bk_pg.UpgradeWrite().template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }
    return false;
  }
  // 目录页有但是bucket页没有
  auto directory_page = bpm_->FetchPageWrite(directory_pgid).template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_pgid = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
  if (bucket_pgid == INVALID_PAGE_ID) {
    auto bk_pg = bpm_->NewPageGuarded(&bucket_pgid);
    if (bucket_pgid == INVALID_PAGE_ID) {
      return false;
    }
    directory_page->SetBucketPageId(directory_page->HashToBucketIndex(hash), bucket_pgid);
    directory_page->SetLocalDepth(directory_page->HashToBucketIndex(hash), directory_page->GetGlobalDepth());
    auto bucket_page = bk_pg.UpgradeWrite().template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }
    return false;
  }
  // 都存在
  auto bucket_page = bpm_->FetchPageWrite(bucket_pgid).template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  V tmpvalue;
  if (bucket_page->Lookup(key, tmpvalue, cmp_)) {
    std::cout << "key already exist" << std::endl;
    return false;
  }

  if (!bucket_page->IsFull()) {
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }
    return false;
  }
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  // 分裂操作,获得旧的localdepth
  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  if (local_depth == directory_page->GetGlobalDepth()) {
    // 该目录页已经满了，无法创建新的bucket
    if (directory_page->GetGlobalDepth() == directory_page->GetMaxDepth()) {
      return false;
    }
    directory_page->IncrLocalDepth(bucket_idx);
    directory_page->IncrGlobalDepth();
  }
  auto local_depth_mask = (1U << local_depth) - 1;
  page_id_t new_bck_pgid = INVALID_PAGE_ID;
  auto new_bucket_page = bpm_->NewPageGuarded(&new_bck_pgid);
  if (new_bck_pgid == INVALID_PAGE_ID) {
    std::cout << "无法分配bucket page页面" << std::endl;
    return false;
  }
  auto new_bkg = new_bucket_page.UpgradeWrite().template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  for (uint32_t idx = 0; idx < 1U << directory_page->GetGlobalDepth(); ++idx) {
    if ((idx & local_depth_mask) == (hash & local_depth_mask)) {
      directory_page->SetLocalDepth(idx, local_depth + 1);
      if ((idx >> local_depth) & 1U) {
        directory_page->SetBucketPageId(idx, new_bck_pgid);
      }
    }
  }
  std::vector<K> remove_key;
  for (uint32_t idx = 0; idx < bucket_page->Size(); ++idx) {
    auto tmpidx = directory_page->HashToBucketIndex(hash_fn_.GetHash(bucket_page->KeyAt(idx)));
    auto tmppair = bucket_page->EntryAt(idx);
    if ((tmpidx >> local_depth) & 1U) {
      new_bkg->Insert(tmppair.first, tmppair.second, cmp_);
      remove_key.push_back(bucket_page->KeyAt(idx));
    }
  }

  for (auto key : remove_key) {
    bucket_page->Remove(key, cmp_);
  }

  //分裂完后新数据要插入
  Insert(key,value, transaction);

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
