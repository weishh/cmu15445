//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager),
      disk_manager(disk_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  Page *ptr = nullptr;
  page_id_t pid = INVALID_PAGE_ID;
  {
    std::lock_guard<std::mutex> lock(latch_);
    frame_id_t frame_id = -1;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    replacer_->RecordAccess(frame_id);
    // holding a frameid
    pid = AllocatePage();
    ptr = &pages_[frame_id];
    page_table_.erase(ptr->page_id_);
    page_table_.insert({pid, frame_id});
    ++ptr->pin_count_;
  }
  ptr->WLatch();
  if (ptr->is_dirty_) {
    disk_manager->WritePage(ptr->page_id_, ptr->data_);
  }
  ptr->page_id_ = pid;
  ptr->ResetMemory();
  ptr->is_dirty_ = false;
  ptr->WUnlatch();
  *page_id = ptr->page_id_;
  return ptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  Page *ptr = nullptr;
  {
    std::lock_guard<std::mutex> lock(latch_); 
    if(page_table_.count(page_id)){
      ptr = &pages_[page_table_[page_id]];
      return ptr;
    }
    
  }

}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return false; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
