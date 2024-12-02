#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // std::cout << "BasicPageGuard move construct called" << std::endl;
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  // std::cout << "BasicPageGuard drop() called" << std::endl;
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // std::cout << "BasicPageGuard move operator called" << std::endl;
  if (this != &that) {
    this->Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // std::cout << "~BasicPageGuard() called" << std::endl;
  Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // std::cout << "UpgradeRead() called" << std::endl;
  if (page_ != nullptr) {
    page_->RLatch();
  }
  ReadPageGuard res(bpm_, page_);
  this->page_ = nullptr;
  Drop();
  return res;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  // std::cout << "UpgradeWrite() called" << std::endl;
  if (page_ != nullptr) {
    page_->WLatch();
  }
  WritePageGuard res(bpm_, page_);
  this->page_ = nullptr;
  Drop();
  return res;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
    that.guard_.Drop();
  }

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // std::cout << "ReadPageGuard drop() called: " << guard_.page_->GetPageId() << std::endl;
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // std::cout << "~ReadPageGuard() called" << std::endl;
  Drop();

}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
    that.guard_.Drop();
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    // std::cout << "WritePageGuard drop() called: " << guard_.page_->GetPageId() << std::endl;
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  // std::cout << "~WritePageGuard() called" << std::endl;
  Drop();
}  // NOLINT

}  // namespace bustub
