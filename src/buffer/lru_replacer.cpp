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

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    latch_.lock();
    if (lruList_.empty()) {
        latch_.unlock();
        return false;
    }

    auto outFrame = lruList_.back();
    lruList_.pop_back();
    lruMap_.erase(outFrame);
    *frame_id = outFrame;
    latch_.unlock();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    if (lruMap_.find(frame_id) == lruMap_.end()) {
        latch_.unlock();
        return;
    }
    lruList_.erase(lruMap_[frame_id]);
    lruMap_.erase(frame_id);
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();
    if (lruMap_.find(frame_id) != lruMap_.end()) {
        latch_.unlock();
        return;
    }
    while (static_cast<int>(Size()) >= capacity_) {
        auto outFrame = lruList_.back();
        lruList_.pop_back();
        lruMap_.erase(outFrame);
    }
    lruList_.emplace_front(frame_id);
    lruMap_[frame_id] = lruList_.begin();
    latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return lruList_.size(); }

}  // namespace bustub
