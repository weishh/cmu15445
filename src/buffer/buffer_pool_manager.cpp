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
      disk_manager_(disk_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k); //bug

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
    replacer_->RecordAccess(frame_id); //bug
    // holding a frameid
    pid = AllocatePage();
    ptr = &pages_[frame_id];
    page_table_.erase(ptr->page_id_);
    page_table_.insert({pid, frame_id}); //bug
    ++ptr->pin_count_;
  }
  ptr->WLatch();
  if (ptr->is_dirty_) {
    disk_manager_->WritePage(ptr->page_id_, ptr->data_);
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
  if (page_id >= next_page_id_ || page_id < 0) {
    return nullptr;
  }
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.count(page_id) != 0U) {
      ptr = &pages_[page_table_[page_id]];
      return ptr;
    }
    frame_id_t frame_id = -1;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    ptr = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    page_table_.erase(ptr->page_id_);
    page_table_.insert({page_id, frame_id});
    ++ptr->pin_count_;
  }
  char data[BUSTUB_PAGE_SIZE];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, data, page_id, std::move(promise)});
  ptr->WLatch();
  if (ptr->is_dirty_) {
    disk_manager_->WritePage(ptr->page_id_, ptr->data_);
  }
  ptr->page_id_ = page_id;
  ptr->ResetMemory();
  ptr->is_dirty_ = false;
  if (future.get()) {
    memcpy(ptr->data_, data, sizeof(data));
  }
  ptr->WUnlatch();
  return ptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  Page *ptr = nullptr;
  frame_id_t frame_id = -1;
  {
    std::lock_guard<std::mutex> lock(latch_);
    auto tmp = page_table_.find(page_id);
    if (tmp == page_table_.end() || pages_[tmp->second].pin_count_ == 0) {
      return false;
    }
    frame_id = tmp->second;
    ptr = &pages_[frame_id];
    --ptr->pin_count_;
  }
  ptr->WLatch();
  ptr->is_dirty_ = is_dirty;
  if (ptr->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  ptr->WUnlatch();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  Page *ptr = nullptr;
  frame_id_t frame_id = -1;
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.count(page_id) == 0U) {
      return false;
    }
  }
  frame_id = page_table_[page_id];
  ptr = &pages_[frame_id];
  ptr->WLatch();
  disk_manager_->WritePage(page_id, ptr->data_);
  ptr->is_dirty_ = false;
  ptr->WUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ptr = nullptr;
  for (size_t i = 0; i < pool_size_; ++i) {
    ptr = &pages_[i];
    if (ptr->page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    ptr->WLatch();
    disk_manager_->WritePage(ptr->page_id_, ptr->data_);
    ptr->is_dirty_ = false;
    ptr->WUnlatch();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ptr = nullptr;
  frame_id_t frame_id = -1;
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  frame_id = page_table_[page_id];
  ptr = &pages_[frame_id];
  if (ptr->pin_count_ != 0) {
    return false;
  }
  page_table_.erase(page_id);
  ptr->WLatch();
  ptr->page_id_ = INVALID_PAGE_ID;
  ptr->ResetMemory();
  ptr->is_dirty_ = false;
  ptr->WUnlatch();
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
