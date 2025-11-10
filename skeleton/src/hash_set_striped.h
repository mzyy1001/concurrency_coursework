#ifndef HASH_SET_STRIPED_H
#define HASH_SET_STRIPED_H

#include <cassert>
#include <cstddef>     // size_t
#include <functional>  // std::hash
#include <mutex>       // std::mutex, std::scoped_lock
#include <utility>     // std::move
#include <vector>      // std::vector

#include "src/hash_set_base.h"

// Fixed number of mutexes (locks_), independent from the number of buckets.
// Each bucket maps to a stripe: stripe = bucket % locks_.size().
template <typename T>
class HashSetStriped : public HashSetBase<T> {
 public:
  explicit HashSetStriped(size_t initial_capacity, size_t stripes = 64)
      : buckets_(
            std::max<size_t>(NormalizeCapacity(initial_capacity), kMinBuckets)),
        size_(0),
        locks_(stripes ? stripes : 64) {}  // avoid zero stripes

  // Insert using the corresponding stripe lock.
  bool Add(T elem) final {
    while (true) {
      size_t cap = buckets_.size();
      size_t i = Index(elem);
      size_t stripe = StripeOfBucket(i);
      
      std::unique_lock<std::mutex> lk(locks_[stripe]);
      
      // Check if resize happened between computing index and acquiring lock.
      if (cap != buckets_.size()) {
        continue;
      }
      
      auto& b = buckets_[i];
      if (std::find(b.begin(), b.end(), elem) != b.end()) {
        return false;
      }
      b.push_back(std::move(elem));
      size_.fetch_add(1, std::memory_order_relaxed);
      break;
    }
    
    if (LoadFactor() > kMaxLoadFactor) {
      Resize(buckets_.size() * 2);
    }
    return true;
  }

  // Insert under the corresponding stripe lock.
  bool Remove(T elem) final {
    while (true) {
      size_t cap = buckets_.size();
      size_t i = Index(elem);
      size_t stripe = StripeOfBucket(i);
      
      std::unique_lock<std::mutex> lk(locks_[stripe]);
      
      // Check if resize happened between computing index and acquiring lock.
      if (cap != buckets_.size()) {
        continue;
      }
      
      auto& b = buckets_[i];
      auto it = std::find(b.begin(), b.end(), elem);
      if (it == b.end()) {
        return false;
      }
      b.erase(it);
      size_.fetch_sub(1, std::memory_order_relaxed);
      break;
    }
    
    if (LoadFactor() < 1.0 && buckets_.size() > kMinBuckets) {
      Resize(buckets_.size() / 2);
    }
    return true;
  }

  // Insert under the corresponding stripe lock.
  [[nodiscard]] bool Contains(T elem) final {
    while (true) {
      size_t cap = buckets_.size();
      size_t i = Index(elem);
      size_t stripe = StripeOfBucket(i);
      
      std::unique_lock<std::mutex> lk(locks_[stripe]);
      
      // Check if resize happened between computing index and acquiring lock.
      if (cap != buckets_.size()) {
        continue;
      }
      
      auto& b = buckets_[i];
      return std::find(b.begin(), b.end(), elem) != b.end();
    }
  }

  // Atomic size is sufficient; stripe locks protect structural changes.
  [[nodiscard]] size_t Size() const final {
    return size_.load(std::memory_order_relaxed);
  }

 private:
  std::vector<std::vector<T>> buckets_;
  std::atomic<size_t> size_;  // Updated inside stripe CS; relaxed is OK
  std::hash<T> hasher_;
  std::vector<std::mutex> locks_;
  std::mutex resize_mutex_;  // Protects resize operations

  static constexpr size_t kMinBuckets = 4;
  static constexpr double kMaxLoadFactor = 4.0;

  static size_t NormalizeCapacity(size_t cap) {
    return cap == 0 ? kMinBuckets : cap;
  }

  size_t Index(const T& elem) const { return hasher_(elem) % buckets_.size(); }

  // Map bucket to a stripe (lock index).
  size_t StripeOfBucket(size_t b) const { return b % locks_.size(); }

  // Approximate load factor; exactness not required for triggering resize.
  double LoadFactor() const {
    return static_cast<double>(size_.load(std::memory_order_relaxed)) /
           static_cast<double>(buckets_.size());
  }

  void Resize(size_t new_capacity) {
    std::unique_lock<std::mutex> resize_lock(resize_mutex_);
    
    new_capacity = std::max(kMinBuckets, NormalizeCapacity(new_capacity));
    
    // Check if another thread already resized.
    if (new_capacity == buckets_.size()) {
      return;
    }
    
    // Acquire all stripe locks in order.
    for (auto& lock : locks_) {
      lock.lock();
    }
    
    std::vector<std::vector<T>> new_buckets(new_capacity);
    for (auto& bucket : buckets_) {
      for (auto& v : bucket) {
        size_t i = hasher_(v) % new_capacity;
        new_buckets[i].push_back(std::move(v));
      }
    }
    buckets_.swap(new_buckets);
    
    // Release all stripe locks.
    for (auto& lock : locks_) {
      lock.unlock();
    }
  }
};
#endif  // HASH_SET_STRIPED_H
