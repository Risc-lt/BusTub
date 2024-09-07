#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    throw Exception("read ts not found");
  }

  iter->second--;
  if (iter->second == 0) {
    current_reads_.erase(iter);
  }

  for (; watermark_ < commit_ts_; watermark_++) {
    if (current_reads_.find(watermark_) != current_reads_.end()) {
      break;
    }
  }
}

}  // namespace bustub
