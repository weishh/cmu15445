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
  frame_id_t max_frame_id = 0;
  size_t min_access = SIZE_MAX;
  size_t tmp_bkd = 0;
  auto cmp = [](std::pair<frame_id_t, size_t> p1, std::pair<frame_id_t, size_t> p2) -> bool {
    return p1.second > p2.second;
  };
  std::priority_queue<std::pair<frame_id_t, size_t>, std::vector<std::pair<frame_id_t, size_t>>, decltype(cmp)> pq(cmp);
  for (auto &p : node_store_) {
    if (!p.second.isEvictable()) {
      continue;
    }
    tmp_bkd = p.second.getBKD();
    if (tmp_bkd == SIZE_MAX) {
      pq.push({p.first, p.second.get_least()});
      min_access = 0;
      continue;
    }
    if (tmp_bkd < min_access) {
      min_access = tmp_bkd;
      max_frame_id = p.first;
    }
  }
  // 没有找到可以驱逐的frame
  if (!max_frame_id && min_access == SIZE_MAX) {
    return false;
  }
  // 表示有少于k次访问的frame，采用传统lru选择驱逐frame
  if (min_access == 0) {
    max_frame_id = pq.top().first;
  }
  // 这里的驱逐不能直接erase，只需要清楚history
  *frame_id = max_frame_id;

  node_store_[max_frame_id].clearHistory();
  SetEvictable(max_frame_id, false);

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
    if (!node_store_.count(frame_id)) {  // 这里是否需要判断 > replacer_size_ ？不需要，前面判断frame_id保证了不会超大小
      node_store_.insert({frame_id, LRUKNode(k_)});
    }
    node_store_[frame_id].recordAccess(current_timestamp_);
    ++current_timestamp_;
  }
}

// 修改：nodestore中lrunode的可驱逐标记， lru的size， 访问nodestore
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
      throw Exception("Invalid frame id: out of range");
    }
    if (!node_store_.count(frame_id)) {
      throw Exception("Invalid frame id: not found");
    }
    // TT，FF情况，size不变；T，f-t ++size； f，t-f --size
    bool flag = node_store_[frame_id].setEvictable(set_evictable);
    if (flag) {
      set_evictable ? ++curr_size_ : --curr_size_;
    }
  }
}
// 修改：nodestore中lrunode的可驱逐标记
void LRUKReplacer::Remove(frame_id_t frame_id) {
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
      throw Exception("Invalid frame_id");
    }
    if (!node_store_.count(frame_id) || !node_store_[frame_id].isEvictable()) {
      throw Exception("frame is not exist or unevictable");
    }
    node_store_[frame_id].clearHistory();
    node_store_[frame_id].setEvictable(false);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub

auto bustub::LRUKNode::getBKD() -> size_t {
  if (history_.size() < k_) {
    return SIZE_MAX;
  }
  auto iter = history_.begin();
  std::advance(iter, k_ - 1);
  return *iter;
}

auto bustub::LRUKNode::isEvictable() -> bool { return is_evictable_; }

auto bustub::LRUKNode::get_least() -> size_t { return history_.front(); }

auto bustub::LRUKNode::recordAccess(size_t time) -> void { history_.push_front(time); }

auto bustub::LRUKNode::setEvictable(bool set_evictable) -> bool {
  bool flag = is_evictable_;
  is_evictable_ = set_evictable;
  return flag ^ is_evictable_;
}
auto bustub::LRUKNode::clearHistory() -> void { history_.clear(); }