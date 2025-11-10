#ifndef HASH_SET_REFINABLE_H
#define HASH_SET_REFINABLE_H

#include <algorithm>   // std::find
#include <atomic>      // std::atomic
#include <cassert>
#include <cstddef>     // size_t
#include <functional>  // std::hash
#include <mutex>       // std::mutex, std::unique_lock
#include <thread>      // std::this_thread::yield, std::thread::id
#include <utility>     // std::move
#include <vector>      // std::vector

#include "src/hash_set_base.h"

// Refinable hash set: one lock per bucket.
// Lock array is resized along with the bucket array.
template <typename T>
class HashSetRefinable : public HashSetBase<T> {
 public:
  explicit HashSetRefinable(size_t initial_capacity)
      : buckets_(
            std::max<size_t>(NormalizeCapacity(initial_capacity), kMinBuckets)),
        size_(0),
  locks_(buckets_.size()),
  version_(0),
  resizing_(false),
  owner_tid_hash_(0) {}

  // Insert by locking the bucket; retry if a resize intervenes.
  bool Add(T elem) final {
    size_t used_cap = 0;
    while (true) {
      // Avoid starting an operation while another thread is resizing.
      WaitIfResizingByOther();
      size_t ver_before = version_.load(std::memory_order_acquire);
      size_t cap = buckets_.size();
      size_t i = Index(elem);

      std::unique_lock<std::mutex> bucket_lk(locks_[i]);

      // Check if resize happened after we computed index but before we locked.
      if (version_.load(std::memory_order_acquire) != ver_before) {
        continue;
      }

      // Hash set add logic.
      auto& b = buckets_[i];
      if (std::find(b.begin(), b.end(), elem) != b.end()) {
        return false;
      }
      b.push_back(std::move(elem));
      size_.fetch_add(1, std::memory_order_relaxed);
      used_cap = cap;
      break;
    }

    // Estimate load factor using the capacity we operated under to trigger resizes.
    double lf = static_cast<double>(size_.load(std::memory_order_relaxed)) /
                static_cast<double>(used_cap);
    if (!resizing_.load(std::memory_order_acquire) && lf > kMaxLoadFactor) {
      Resize(used_cap * 2);
    }

    return true;
  }

  // Remove by locking the bucket; retry if a resize intervenes.
  bool Remove(T elem) final {
    while (true) {
      // Avoid starting an operation while another thread is resizing.
      WaitIfResizingByOther();
      size_t ver_before = version_.load(std::memory_order_acquire);
      size_t i = Index(elem);

      std::unique_lock<std::mutex> bucket_lk(locks_[i]);

      // Check if resize happened after we computed index but before we locked.
      if (version_.load(std::memory_order_acquire) != ver_before) {
        continue;
      }

      // Hash set remove logic.
      auto& b = buckets_[i];
      auto item = std::find(b.begin(), b.end(), elem);
      if (item == b.end()) {
        return false;
      }
      b.erase(item);
      size_.fetch_sub(1, std::memory_order_relaxed);
      break;
    }
    return true;
  }

  // Check elem by locking the bucket; retry if a resize intervenes.
  [[nodiscard]] bool Contains(T elem) final {
    while (true) {
      // Avoid starting an operation while another thread is resizing.
      WaitIfResizingByOther();
      size_t ver_before = version_.load(std::memory_order_acquire);
      size_t i = Index(elem);

      std::unique_lock<std::mutex> bucket_lk(locks_[i]);

      // Check if resize happened after we computed index but before we locked.
      if (version_.load(std::memory_order_acquire) != ver_before) {
        continue;
      }

      auto& b = buckets_[i];
      return std::find(b.begin(), b.end(), elem) != b.end();
    }
  }

  // No synchronization needed; size_ is atomic.
  [[nodiscard]] size_t Size() const final {
    return size_.load(std::memory_order_relaxed);
  }

 private:
  std::vector<std::vector<T>> buckets_;
  std::atomic<size_t> size_;
  std::hash<T> hasher_;
  std::vector<std::mutex> locks_; // One lock per bucket; size always equals buckets_.size().

  std::mutex resize_mutex_; // Separate mutex exclusively for resizing.
  std::atomic<size_t> version_; // version stamp for resize detection
  std::atomic<bool> resizing_; // resizing flag
  std::atomic<size_t> owner_tid_hash_; // resizing operation owner
  
  // Keep old lock arrays alive to avoid premature mutex destruction.
  std::vector<std::vector<std::mutex>> old_lock_arrays_;

  static constexpr size_t kMinBuckets = 4;
  static constexpr double kMaxLoadFactor = 4.0;
  static constexpr double kMinLoadFactor = 1.0;

  static size_t NormalizeCapacity(size_t cap) {
    return cap == 0 ? kMinBuckets : cap;
  }

  size_t Index(const T& elem) const { return hasher_(elem) % buckets_.size(); }

  void Resize(size_t new_capacity) {
    // Ensure only one resizer runs; normal ops will spin while a
    // resizer owned by another thread is active.
    std::unique_lock<std::mutex> resizer_lock(resize_mutex_);

    new_capacity = std::max(kMinBuckets, NormalizeCapacity(new_capacity));

    // Check if another thread already resized to the desired capacity.
    if (new_capacity == buckets_.size()) {
      return;
    }

    const size_t me = std::hash<std::thread::id>{}(std::this_thread::get_id());
    owner_tid_hash_.store(me, std::memory_order_release);
    resizing_.store(true, std::memory_order_release);

    std::vector<std::vector<T>> new_buckets(new_capacity);
    std::vector<std::mutex> new_locks(new_capacity);

    // Acquire ALL locks from the old array before proceeding.
    // This ensures no thread is in the middle of an operation on old buckets.
    std::vector<std::unique_lock<std::mutex>> all_locks;
    all_locks.reserve(locks_.size());
    for (size_t i = 0; i < locks_.size(); ++i) {
      all_locks.emplace_back(locks_[i]);
    }

    // With all old locks held, migrate elements to new buckets.
    for (size_t i = 0; i < buckets_.size(); ++i) {
      auto& old_bucket = buckets_[i];
      for (auto& v : old_bucket) {
        size_t j = hasher_(v) % new_capacity;
        new_buckets[j].push_back(std::move(v));
      }
      old_bucket.clear();
    }

    // Swap buckets and locks while still holding all old locks.
    buckets_.swap(new_buckets);
    locks_.swap(new_locks);

    version_.fetch_add(1, std::memory_order_acq_rel);

    for (auto& lk : all_locks) {
      if (lk.owns_lock()) {
        lk.unlock();
      }
    }
    
    resizing_.store(false, std::memory_order_release);
    owner_tid_hash_.store(0, std::memory_order_release);

    // Detach unique_locks from mutexes to avoid destructor issues.
    for (auto& lk : all_locks) {
      lk.release();
    }
    all_locks.clear();
    
    // Move old locks to storage to keep them alive (they're now in new_locks).
    // This prevents premature mutex destruction.
    old_lock_arrays_.push_back(std::move(new_locks));
  }

  // Simple spinlock optimization to pause on long waits.
  void WaitIfResizingByOther() const {
    int spins = 0;
    while (resizing_.load(std::memory_order_acquire)) {
      if (++spins < 32) {
        // quick pause
      } else {
        std::this_thread::yield();
      }
    }
  }
};

#endif  // HASH_SET_REFINABLE_H
