//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  auto defmaxdep = HTABLE_DIRECTORY_MAX_DEPTH;
  max_depth_ = std::min(static_cast<uint32_t>(defmaxdep), max_depth);
  global_depth_ = 0;
  std::fill(std::begin(local_depths_), std::end(local_depths_), 0);
  std::fill(std::begin(bucket_page_ids_), std::end(bucket_page_ids_), INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1U << GetGlobalDepth()) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1U << GetLocalDepth(bucket_idx)) - 1;
}
// 增加全局深度和减小全局深度都需要对多出来和减少的目录项进行处理
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() { global_depth_ += 1; }

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_ -= 1; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  auto maxlocaldp = GetLocalDepth(0);
  auto globaldp = GetGlobalDepth();
  for (uint64_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; ++i) {
    maxlocaldp = std::max(maxlocaldp, GetLocalDepth(i));
  }
  return globaldp > maxlocaldp;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << GetGlobalDepth(); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx] += 1; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx] -= 1; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return HTABLE_DIRECTORY_MAX_DEPTH; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << GetMaxDepth(); }

}  // namespace bustub
