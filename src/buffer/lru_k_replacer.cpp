//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 还需要加锁
// 涉及修改公共变量：node_store_中LRUNODE的历史链表，LRUNODE中的可驱逐标志
// 涉及访问公共变量：node_store_,
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t fid = -1;
  std::vector<std::pair<frame_id_t, size_t>> v1;
  std::vector<std::pair<frame_id_t, size_t>> v2;
  std::cout << "k value: " << k_ << std::endl;
  for (auto &[fid, node] : node_store_) {
    std::cout << "Frame " << fid << ":\n";
    std::cout << "Evictable: " << node.IsEvictable() << "\n";
    std::cout << "History: ";
    // 需要在LRUKNode中添加一个方法来访问history_
    for (auto &ts : node.GetHistory()) {
      std::cout << ts << " ";
    }
    std::cout << "\n";
  }
  std::cout << "===========" << std::endl;

  for (auto &p : node_store_) {
    if (!p.second.IsEvictable()) {
      continue;
    }
    auto k_distance = p.second.GetBKD();
    if (k_distance == SIZE_MAX) {
      v2.emplace_back(p.first, p.second.GetLeast());
    } else {
      v1.emplace_back(p.first, k_distance);
    }
  }
  auto cmp = [&](std::pair<frame_id_t, size_t> p1, std::pair<frame_id_t, size_t> p2) { return p1.second < p2.second; };

  if (!v2.empty()) {
    std::sort(v2.begin(), v2.end(), cmp);
    fid = v2[0].first;
  } else if (!v1.empty()) {
    std::sort(v1.begin(), v1.end(), cmp);
    fid = v1[0].first;
  } else {
    return false;
  }

  *frame_id = fid;
  node_store_[fid].ClearHistory();
  node_store_[fid].SetEvictable(false);
  --curr_size_;
  return true;
}

// 需要添加锁
// 修改：nodestore，currenttimestamp， lrunode中的历史纪录
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 这里的=号需要考虑
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    throw Exception("Invalid frame_id");
  }
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (node_store_.count(frame_id) == 0U) {
      node_store_.insert({frame_id, LRUKNode(k_)});  // bug
    }
    node_store_[frame_id].RecordAccess(current_timestamp_);  // bug
    ++current_timestamp_;
  }
}

// 修改：nodestore中lrunode的可驱逐标记， lru的size， 访问nodestore
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    throw Exception("Invalid frame id: out of range");
  }
  if (node_store_.count(frame_id) == 0U) {
    throw Exception("Invalid frame id: not found");
  }
  // TT，FF情况，size不变；T，f-t ++size； f，t-f --size
  bool flag = node_store_[frame_id].SetEvictable(set_evictable);
  if (flag) {
    set_evictable ? ++curr_size_ : --curr_size_;
  }
}

// 修改：nodestore中lrunode的可驱逐标记
void LRUKReplacer::Remove(frame_id_t frame_id) {
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_) || node_store_.count(frame_id) == 0U) {
      return;
    }
    if (!node_store_[frame_id].IsEvictable()) {
      return;
    }
    node_store_[frame_id].ClearHistory();
    node_store_[frame_id].SetEvictable(false);
    --curr_size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

auto LRUKReplacer::GetTimestamp() -> size_t { return current_timestamp_; }

auto bustub::LRUKNode::GetBKD() -> size_t {
  if (history_.size() < k_) {
    return SIZE_MAX;
  }
  auto iter = history_.begin();
  std::advance(iter, k_ - 1);
  return *iter;
}

auto bustub::LRUKNode::IsEvictable() -> bool { return is_evictable_; }

auto bustub::LRUKNode::GetLeast() -> size_t { return history_.back(); }

auto bustub::LRUKNode::RecordAccess(size_t time) -> void { history_.push_front(time); }

auto bustub::LRUKNode::SetEvictable(bool set_evictable) -> bool {
  bool flag = is_evictable_;
  is_evictable_ = set_evictable;
  return flag ^ is_evictable_;
}
auto bustub::LRUKNode::ClearHistory() -> void { history_.clear(); }

}  // namespace bustub
auto bustub::LRUKNode::GetHistory() -> std::list<size_t> & { return history_; }