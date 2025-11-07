#ifndef HASH_SET_COARSE_GRAINED_H
#define HASH_SET_COARSE_GRAINED_H

#include <algorithm>  // std::find
#include <cassert>
#include <cstddef>     // size_t
#include <functional>  // std::hash
#include <mutex>       // std::mutex, std::scoped_lock
#include <utility>     // std::move
#include <vector>      // std::vector

#include "src/hash_set_base.h"

// One global mutex protects the entire table for Add/Remove/Contains/Size.
template <typename T>
class HashSetCoarseGrained : public HashSetBase<T> {
 public:
  explicit HashSetCoarseGrained(size_t initial_capacity)
      : buckets_(
            std::max<size_t>(NormalizeCapacity(initial_capacity), kMinBuckets)),
        size_(0) {}

  // Entire operation under the global lock.
  bool Add(T elem) final {
    std::scoped_lock lock(mutex_);
    size_t i = Index(elem);
    auto& b = buckets_[i];
    if (std::find(b.begin(), b.end(), elem) != b.end()) {
      return false;
    }
    b.push_back(std::move(elem));
    ++size_;
    if (LoadFactor() > 4.0) {
      Resize(buckets_.size() * 2);
    }
    return true;
  }

  // Entire operation under the global lock.
  bool Remove(T elem) final {
    std::scoped_lock lock(mutex_);
    size_t i = Index(elem);
    auto& b = buckets_[i];
    auto it = std::find(b.begin(), b.end(), elem);
    if (it == b.end()) {
      return false;
    }
    b.erase(it);
    --size_;
    if (LoadFactor() < 1.0) {
      Resize(buckets_.size() / 2);
    }
    return true;
  }
  // Entire operation under the global lock.
  [[nodiscard]] bool Contains(T elem) final {
    std::scoped_lock lock(mutex_);
    size_t i = Index(elem);
    auto& b = buckets_[i];
    return std::find(b.begin(), b.end(), elem) != b.end();
  }
  // Entire operation under the global lock.
  [[nodiscard]] size_t Size() const final {
    std::scoped_lock lock(mutex_);
    return size_;
  }

 private:
  mutable std::mutex mutex_;  // Global lock guarding all state
  std::vector<std::vector<T>> buckets_;
  size_t size_;
  std::hash<T> hasher_;

  static constexpr size_t kMinBuckets = 4;
  static constexpr double kMaxLoadFactor = 4.0;

  static size_t NormalizeCapacity(size_t cap) {
    return cap == 0 ? kMinBuckets : cap;
  }

  size_t Index(const T& elem) const { return hasher_(elem) % buckets_.size(); }

  double LoadFactor() const {
    return static_cast<double>(size_) / static_cast<double>(buckets_.size());
  }

  // Resize assumes the caller already holds mutex_ (no re-entrant locking).
  void Resize(size_t new_capacity) {
    std::vector<std::vector<T>> new_buckets(new_capacity);
    for (auto& bucket : buckets_) {
      for (auto& v : bucket) {
        size_t i = hasher_(v) % new_capacity;
        new_buckets[i].push_back(std::move(v));
      }
    }
    buckets_.swap(new_buckets);
  }
};

#endif  // HASH_SET_COARSE_GRAINED_H
