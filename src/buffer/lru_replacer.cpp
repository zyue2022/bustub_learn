//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock{latch_};

  if (lru_list_.empty()) {
    return false;
  }

  auto out_frame = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(out_frame);
  *frame_id = out_frame;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{latch_};

  if (lru_map_.find(frame_id) == lru_map_.end()) {
    return;
  }

  lru_list_.erase(lru_map_[frame_id]);
  lru_map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{latch_};

  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;
  }

  if (lru_list_.size() >= capacity_) {
    return;
  }

  lru_list_.emplace_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

auto LRUReplacer::Size() -> size_t {
  std::scoped_lock lock{latch_};

  return lru_list_.size();
}

}  // namespace bustub
