#ifndef HASH_SET_SEQUENTIAL_H
#define HASH_SET_SEQUENTIAL_H


#include <algorithm>
#include <cstddef>
#include <functional>
#include <utility>
#include <vector> 
#include <cassert>
#include "src/hash_set_base.h"

template <typename T>
class HashSetSequential : public HashSetBase<T> {
 public:
  explicit HashSetSequential(size_t initial_capacity)
      // Ensure at least kMinBuckets buckets to avoid modulo-by-zero.
      : buckets_(std::max<size_t>(NormalizeCapacity(initial_capacity), kMinBuckets)),
        size_(0) {}
  // Returns true if elem was newly inserted.
  bool Add(T elem) final {
    size_t i = Index(elem);
    auto& b = buckets_[i];
    if (std::find(b.begin(), b.end(), elem) != b.end()) {
      return false;
    }
    b.push_back(std::move(elem));
    ++size_;

    if (LoadFactor() > kMaxLoadFactor) {
      Resize(buckets_.size() * 2);
    }
    return true;
  }
  //Returns true if elem existed and was removed.
  bool Remove(T elem) final {
    size_t i = Index(elem);
    auto& b = buckets_[i];
    auto it = std::find(b.begin(), b.end(), elem);
    if (it == b.end()) return false;
    b.erase(it);
    --size_;
    return true;
  }

  // Returns true if elem is present.
  [[nodiscard]] bool Contains(T elem) final {
    size_t i = Index(elem);
    const auto& b = buckets_[i];
    return std::find(b.begin(), b.end(), elem) != b.end();
  }

  // Returns the size of the hash set.
  [[nodiscard]] size_t Size() const final { return size_; }

 private:
  static constexpr size_t kMinBuckets = 4;
  static constexpr double kMaxLoadFactor = 4.0;

  static size_t NormalizeCapacity(size_t cap) {
    return cap == 0 ? kMinBuckets : cap;
  }

  size_t Index(const T& x) const { return hasher_(x) % buckets_.size(); }
  double LoadFactor() const {
    return static_cast<double>(size_) / static_cast<double>(buckets_.size());
  }


  // Rehash all elements into a table with new_cap buckets.
  void Resize(size_t new_cap) {
    std::vector<std::vector<T>> new_buckets(new_cap);
    for (auto& bucket : buckets_) {
      for (auto& v : bucket) {
        size_t i = hasher_(v) % new_cap;
        new_buckets[i].push_back(std::move(v));
      }
    }
    buckets_.swap(new_buckets);
  }

  std::vector<std::vector<T>> buckets_;
  size_t size_;
  std::hash<T> hasher_;
};

#endif  // HASH_SET_SEQUENTIAL_H
