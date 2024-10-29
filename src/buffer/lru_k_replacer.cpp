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
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    frame_id_t max_frame_id = 0;
    size_t max_gap = 0;
    size_t tmp_bkd = 0;
    auto cmp = [](std::pair<frame_id_t, size_t> p1, std::pair<frame_id_t, size_t> p2) -> bool {
        return p1.second > p2.second;
    };
    std::priority_queue<std::pair<frame_id_t, size_t>, std::vector<std::pair<frame_id_t, size_t>>, decltype(cmp)> pq(cmp);
    for(auto p : node_store_){
        if(!p.second.isEvictable()){
            continue;
        }
        tmp_bkd = p.second.getBKD();
        if(tmp_bkd == -1U){
            pq.push({p.first, p.second.get_least()});
            max_gap = -1U;
            continue;
        }
        if(current_timestamp_ - tmp_bkd > max_gap){
            max_gap = current_timestamp_ - tmp_bkd;
            max_frame_id = p.first;
        }
    }
    // 没有找到可以驱逐的frame
    if(!max_frame_id && !max_gap){
        return false;
    }
    // 表示有少于k次访问的frame，采用传统lru选择驱逐frame
    if(max_gap == -1U){
        max_frame_id = pq.top().first;
    }
    // 这里的驱逐不能直接erase，只需要清楚history
    node_store_.erase(max_frame_id);
    return true;
    
}
// 需要添加锁
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    // 这里的=号需要考虑
    if(frame_id < 0 || frame_id >= replacer_size_){
        throw Exception("Invalid frame_id");
    }
    if(!node_store_.count(frame_id)){ // 这里是否需要判断 > replacer_size_ ？不需要，前面判断frame_id保证了不会超大小
        node_store_.insert({frame_id, LRUKNode(k_)});
    }
    node_store_[frame_id].recordAcess(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(frame_id < 0 || frame_id >= replacer_size_) {
        throw Exception("Invalid frame id: out of range");
    }
    if(!node_store_.count(frame_id)) {
        throw Exception("Invalid frame id: not found");
    }
    // TT，FF情况，size不变；T，f-t ++size； f，t-f --size
    bool flag = node_store_[frame_id].setEvictable(set_evictable);
    if(!flag){
        return;
    }
    set_evictable ? ++curr_size_ : --curr_size_;
}

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

auto bustub::LRUKNode::recordAcess(size_t time) -> void {history_.push_front(time);}

auto bustub::LRUKNode::setEvictable(bool set_evictable) -> bool {
    bool flag = is_evictable_;
    is_evictable_ = set_evictable;
    return flag ^ is_evictable_;
}