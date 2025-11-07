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

// Refinable hash set: one lock per bucket, and the lock array resizes
// together with the bucket array. Synchronization follows the refinable
// approach: operations take only the corresponding bucket lock, and detect
// concurrent resizes via a version stamp; resizing grabs all bucket locks in
// a fixed order (but no global read/write lock is used for normal ops).
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
      size_t i = hasher_(elem) % cap;
      std::unique_lock<std::mutex> bucket_lk(locks_[i]);
      // If a resize started or completed, retry with the new layout.
      if (ver_before != version_.load(std::memory_order_acquire) ||
          IsResizingByOther()) {
        continue;  // Table changed; retry with current layout.
      }
      auto& b = buckets_[i];
      if (std::find(b.begin(), b.end(), elem) != b.end()) {
        return false;
      }
      b.push_back(std::move(elem));
      size_.fetch_add(1, std::memory_order_relaxed);
      used_cap = cap;
      break;
    }
    // Estimate load factor using the capacity we operated under to avoid
    // racing with concurrent resize.
    double lf = static_cast<double>(size_.load(std::memory_order_relaxed)) /
                static_cast<double>(used_cap);
    if (!IsResizingByOther() && lf > kMaxLoadFactor) {
      Resize(used_cap * 2);
    }
    return true;
  }

  bool Remove(T elem) final {
    size_t used_cap = 0;
    while (true) {
      WaitIfResizingByOther();
      size_t ver_before = version_.load(std::memory_order_acquire);
      size_t cap = buckets_.size();
      size_t i = hasher_(elem) % cap;
      std::unique_lock<std::mutex> bucket_lk(locks_[i]);
      if (ver_before != version_.load(std::memory_order_acquire) ||
          IsResizingByOther()) {
        continue;  // Table changed; retry with current layout.
      }
      auto& b = buckets_[i];
      auto it = std::find(b.begin(), b.end(), elem);
      if (it == b.end()) {
        return false;
      }
      b.erase(it);
      size_.fetch_sub(1, std::memory_order_relaxed);
      used_cap = cap;
      break;
    }
    double lf = static_cast<double>(size_.load(std::memory_order_relaxed)) /
                static_cast<double>(used_cap);
    if (!IsResizingByOther() && lf < kMinLoadFactor) {
      Resize(std::max(kMinBuckets, used_cap / 2));
    }
    return true;
  }

  [[nodiscard]] bool Contains(T elem) final {
    while (true) {
      WaitIfResizingByOther();
      size_t ver_before = version_.load(std::memory_order_acquire);
      size_t cap = buckets_.size();
      size_t i = hasher_(elem) % cap;
      std::unique_lock<std::mutex> bucket_lk(locks_[i]);
      if (ver_before != version_.load(std::memory_order_acquire) ||
          IsResizingByOther()) {
        continue;  // Table changed; retry with current layout.
      }
      auto& b = buckets_[i];
      return std::find(b.begin(), b.end(), elem) != b.end();
    }
  }

  // Atomic size is sufficient; table/bucket locks protect structural changes.
  [[nodiscard]] size_t Size() const final {
    return size_.load(std::memory_order_relaxed);
  }

 private:
  std::vector<std::vector<T>> buckets_;
  std::atomic<size_t> size_;
  std::hash<T> hasher_;
  // One lock per bucket; size always equals buckets_.size().
  std::vector<std::mutex> locks_;
  // Serialize resizes and versioning; not used by normal ops.
  std::mutex resize_mutex_;
  std::atomic<size_t> version_;
  std::atomic<bool> resizing_;
  std::atomic<size_t> owner_tid_hash_;

  static constexpr size_t kMinBuckets = 4;
  static constexpr double kMaxLoadFactor = 4.0;
  static constexpr double kMinLoadFactor = 1.0;

  static size_t NormalizeCapacity(size_t cap) {
    return cap == 0 ? kMinBuckets : cap;
  }

  size_t Index(const T& elem) const { return hasher_(elem) % buckets_.size(); }

  double LoadFactor() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) /
           static_cast<double>(buckets_.size());
  }

  void Resize(size_t new_capacity) {
    // Ensure only one resizer runs; normal ops will wait (spin) while a
    // resizer owned by another thread is active.
    std::unique_lock<std::mutex> resizer_lock(resize_mutex_);

    new_capacity = std::max(kMinBuckets, NormalizeCapacity(new_capacity));
    if (new_capacity == buckets_.size()) {
      return;  // Nothing to do.
    }

    // Publish that a resize is in progress and mark the owner.
    const size_t me = std::hash<std::thread::id>{}(std::this_thread::get_id());
    owner_tid_hash_.store(me, std::memory_order_release);
    resizing_.store(true, std::memory_order_release);

    // Prepare new arrays.
    std::vector<std::vector<T>> new_buckets(new_capacity);
    std::vector<std::mutex> new_locks(new_capacity);

    // Move each bucket under its own lock to avoid holding many locks.
    for (size_t i = 0; i < locks_.size(); ++i) {
      std::unique_lock<std::mutex> lk(locks_[i]);
      auto& old_bucket = buckets_[i];
      for (auto& v : old_bucket) {
        size_t j = hasher_(v) % new_capacity;
        new_buckets[j].push_back(std::move(v));
      }
      old_bucket.clear();
      // lk releases at end of iteration.
    }

    // Publish new structure and version.
    buckets_.swap(new_buckets);
    locks_.swap(new_locks);
    version_.fetch_add(1, std::memory_order_acq_rel);

    // Clear resize state.
    resizing_.store(false, std::memory_order_release);
    owner_tid_hash_.store(0, std::memory_order_release);
  }

  // Helpers for coordinating with resize without a global lock.
  bool IsResizingByOther() const {
    if (!resizing_.load(std::memory_order_acquire)) return false;
    const size_t me = std::hash<std::thread::id>{}(std::this_thread::get_id());
    return owner_tid_hash_.load(std::memory_order_acquire) != me;
  }

  void WaitIfResizingByOther() const {
    // Spin briefly while a different thread is resizing.
    int spins = 0;
    while (IsResizingByOther()) {
      if (++spins < 32) {
        // quick pause
      } else {
        std::this_thread::yield();
      }
    }
  }
};

#endif  // HASH_SET_REFINABLE_H
