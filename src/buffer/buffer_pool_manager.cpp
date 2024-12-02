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
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  std::cout << "######################" << std::endl;
  std::cout << "pool size: " << pool_size << std::endl;
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
    replacer_->SetEvictable(frame_id, false);
    // holding a frameid
    pid = AllocatePage();
    // std::cout << "new page called" << std::endl;
    // std::cout << "page id " << pid << std::endl;
    // std::cout << "frameid " << frame_id << std::endl;
    // std::cout << "===========" << std::endl;
    ptr = &pages_[frame_id];
    page_table_.erase(ptr->page_id_);
    page_table_.insert({pid, frame_id});
    ptr->pin_count_ = 1;
    if (ptr->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, ptr->GetData(), ptr->GetPageId(), std::move(promise)});
      future.get();
    }
    ptr->page_id_ = pid;
    ptr->ResetMemory();
    ptr->is_dirty_ = false;
    *page_id = ptr->page_id_;
    return ptr;
  }
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
      replacer_->RecordAccess(page_table_[page_id]);
      // 这一行代码不能少
      replacer_->SetEvictable(page_table_[page_id], false);
      ++ptr->pin_count_;
      return ptr;
    }
    frame_id_t frame_id = -1;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 旧页面写入disk，新页面读入内存
    ptr = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    page_table_.erase(ptr->page_id_);
    page_table_.insert({page_id, frame_id});
    ptr->pin_count_ = 1;
    // std::cout << "fetch page called" << std::endl;
    // std::cout << "page id " << page_id << std::endl;
    // std::cout << "frameid " << frame_id << std::endl;
    // std::cout << "===========" << std::endl;
    if (ptr->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, ptr->GetData(), ptr->GetPageId(), std::move(promise)});
      future.get();
      ptr->is_dirty_ = false;
    }
    ptr->page_id_ = page_id;
    ptr->ResetMemory();
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, ptr->GetData(), ptr->GetPageId(), std::move(promise)});
    future.get();
    return ptr;
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  Page *ptr = nullptr;
  frame_id_t frame_id = -1;
  {
    // std::cout << "unpin page called" << std::endl;
    // std::cout << "page id " << page_id << std::endl;
    std::lock_guard<std::mutex> lock(latch_);
    auto tmp = page_table_.find(page_id);
    if (tmp == page_table_.end()) {
      return false;
    }
    frame_id = tmp->second;
    ptr = &pages_[frame_id];
    // std::cout << "unpin page called" << std::endl;
    // std::cout << "page id " << page_id << std::endl;
    // std::cout << "frameid " << frame_id << std::endl;
    // std::cout << "===========" << std::endl;
    if (ptr->GetPinCount() == 0) {
      return false;
    }
    --ptr->pin_count_;
    if (is_dirty) {
      ptr->is_dirty_ = true;
    }
    if (ptr->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  Page *ptr = nullptr;
  frame_id_t frame_id = -1;
  {
    std::lock_guard<std::mutex> lock(latch_);
    if (page_table_.count(page_id) == 0U) {
      return false;
    }
    frame_id = page_table_[page_id];
    ptr = &pages_[frame_id];
    // std::cout << "flush page called" << std::endl;
    // std::cout << "page id " << page_id << std::endl;
    // std::cout << "frameid " << frame_id << std::endl;
    // std::cout << "===========" << std::endl;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, ptr->GetData(), ptr->GetPageId(), std::move(promise)});
    future.get();
    ptr->is_dirty_ = false;
    return true;
  }
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ptr = nullptr;
  for (size_t i = 0; i < pool_size_; ++i) {
    ptr = &pages_[i];
    if (ptr->page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, ptr->GetData(), ptr->GetPageId(), std::move(promise)});
    future.get();
    ptr->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "delete page called" << std::endl;
  // std::cout << "page id " << page_id << std::endl;
  // std::cout << "===========" << std::endl;
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
  ptr->page_id_ = INVALID_PAGE_ID;
  ptr->ResetMemory();
  ptr->is_dirty_ = false;
  ptr->pin_count_ = 0;
  replacer_->Remove(frame_id);
  free_list_.push_front(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::cout << "FetchPageBasic() called" << std::endl;
  auto *pg = FetchPage(page_id);
  auto pg_guard = BasicPageGuard(this, pg);
  return pg_guard;
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::cout << "FetchPageRead() called:" << page_id << std::endl;
  auto *pg = FetchPage(page_id);
  if (pg != nullptr) {
    pg->RLatch();
    return ReadPageGuard{this, pg};
  }
  return {this, nullptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::cout << "FetchPageWrite() called:" << page_id << std::endl;
  auto *pg = FetchPage(page_id);
  if (pg != nullptr) {
    pg->WLatch();
    return WritePageGuard{this, pg};
  }
  return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // std::cout << "NewPageGuarded() called1111" << std::endl;
  auto *page = NewPage(page_id);
  auto page_guard = BasicPageGuard(this, page);
  return page_guard;
}

}  // namespace bustub
