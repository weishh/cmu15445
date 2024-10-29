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
#include "lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    frame_id_t max_frame_id = 0;
    size_t max_gap = 0;
    size_t tmp_bkd = 0;
    auto cmp = [](std::pair<frame_id_t, size_t> p1, std::pair<frame_id_t, size_t> p2) -> bool {
        return p1.second < p2.second;
    };
    std::priority_queue<std::pair<frame_id_t, size_t>, std::vector<std::pair<frame_id_t, size_t>>, decltype(cmp)> pq(cmp);
    for(auto p : node_store_){
        if(!p.second.isEvictable()){
            continue;
        }
        tmp_bkd = p.second.getBKD();
        if(tmp_bkd == -1){
            pq.push({p.first, current_timestamp_ - p.second.get_least()});
            max_gap = -1;
            continue;
        }
        if(current_timestamp_ - tmp_bkd > max_gap){
            max_gap = current_timestamp_ - tmp_bkd;
            max_frame_id = p.first;
        }
    }
    if(!max_frame_id){
        return false;
    }
    if(max_gap == -1){
        max_frame_id = pq.top().first;
    }
    node_store_.erase(max_frame_id);
    return true;
    
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub

auto bustub::LRUKNode::getBKD() -> size_t {
    if(history_.size() < k_){
        return -1;
    }
    auto iter = history_.begin();
    std::advance(iter, k_-1);
    return *iter;
}

auto bustub::LRUKNode::isEvictable() -> bool { return is_evictable_; }

auto bustub::LRUKNode::get_least() -> size_t {return history_.front();}
