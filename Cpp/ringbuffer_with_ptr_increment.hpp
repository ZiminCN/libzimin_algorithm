// Copyright (c) 2025 Zimin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#ifndef __RINGBUFFER_HPP__
#define __RINGBUFFER_HPP__

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <vector>

template <typename DataFormat> class RingBuffer {
public:
  RingBuffer(int buffer_size) {
    this->ringbuffer.buffer_size = buffer_size;
    this->ringbuffer.read_partition.head_index = 0;
    this->ringbuffer.read_partition.tail_index = 0;
    this->ringbuffer.read_partition.base_index = 0;
    this->ringbuffer.read_partition.size = 0;
    this->ringbuffer.write_partition.head_index = this->ringbuffer.buffer_size;
    this->ringbuffer.write_partition.tail_index = 0;
    this->ringbuffer.write_partition.base_index = 0;
    this->ringbuffer.write_partition.size = this->ringbuffer.buffer_size;

    this->_ring_buffer.resize(this->ringbuffer.buffer_size);
  };

  ~RingBuffer() {
    this->ringbuffer.buffer_size = 0;
    this->ringbuffer.read_partition.head_index = 0;
    this->ringbuffer.read_partition.tail_index = 0;
    this->ringbuffer.read_partition.base_index = 0;
    this->ringbuffer.read_partition.size = 0;
    this->ringbuffer.write_partition.head_index = 0;
    this->ringbuffer.write_partition.tail_index = 0;
    this->ringbuffer.write_partition.base_index = 0;
    this->ringbuffer.write_partition.size = 0;
    this->_ring_buffer.clear();
  };

  bool is_buffer_full() {
    return (this->ringbuffer.write_partition.head_index ==
            this->ringbuffer.read_partition.tail_index);
  }

  void clear_ringbuffer() {
    this->_ring_buffer.clear();
    this->ringbuffer.read_partition.head_index = 0;
    this->ringbuffer.read_partition.tail_index = 0;
    this->ringbuffer.read_partition.base_index = 0;
    this->ringbuffer.read_partition.size = 0;
    this->ringbuffer.write_partition.head_index = this->ringbuffer.buffer_size;
    this->ringbuffer.write_partition.tail_index = 0;
    this->ringbuffer.write_partition.base_index = 0;
    this->ringbuffer.write_partition.size = this->ringbuffer.buffer_size;
  }

  uint64_t get_buffer_free_size() {
    return this->ringbuffer.write_partition.size;
  }

  uint64_t get_buffer_used_size() {
    return this->ringbuffer.read_partition.size;
  }

  // write: blocking when full
  bool write(std::vector<DataFormat> &data) {
    if (data.size() > this->ringbuffer.buffer_size) {
      return false;
    }

    if (data.size() > this->ringbuffer.write_partition.size) {
      return false;
    }

    // write
    this->data_write(data);
    this->ringbuffer.write_partition.tail_index += data.size();
    this->ringbuffer.write_partition.size -= data.size();
    if ((this->ringbuffer.write_partition.tail_index -
         this->ringbuffer.write_partition.base_index) >=
        this->ringbuffer.buffer_size) {
      this->ringbuffer.write_partition.base_index +=
          this->ringbuffer.buffer_size;
    }

    // read
    this->ringbuffer.read_partition.head_index += data.size();
    this->ringbuffer.read_partition.size += data.size();

    if (!ringbuffer_health_check()) {
      this->overbig_force_reset();
    }

    return true;
  }
  // force_write: overwrite when full
  void force_write(std::vector<DataFormat> &data) { (void)data; }
  bool read(std::vector<DataFormat> &data) {
    if (data.size() > this->ringbuffer.buffer_size) {
      return false;
    }

    if (data.size() > this->ringbuffer.read_partition.size) {
      return false;
    }

    // read
    this->data_read(data);
    this->ringbuffer.read_partition.tail_index += data.size();
    this->ringbuffer.read_partition.size -= data.size();
    if ((this->ringbuffer.read_partition.tail_index -
         this->ringbuffer.read_partition.base_index) >=
        this->ringbuffer.buffer_size) {
      this->ringbuffer.read_partition.base_index +=
          this->ringbuffer.buffer_size;
    }

    // write
    this->ringbuffer.write_partition.head_index += data.size();
    this->ringbuffer.write_partition.size += data.size();

    if (!ringbuffer_health_check()) {
      this->overbig_force_reset();
    }

    return true;
  }

  bool check_without_readout(std::vector<DataFormat> &data) {
    if (data.size() > this->ringbuffer.buffer_size) {
      return false;
    }

    if (data.size() > this->ringbuffer.read_partition.size) {
      return false;
    }

    this->data_read(data);
    return true;
  }

  void test_query_ringbuffer() {
    std::cout << "Ring buffer real size: " << this->_ring_buffer.size()
              << std::endl;
    std::cout << "Read head is :"
              << static_cast<int>(this->ringbuffer.read_partition.head_index)
              << std::endl;
    std::cout << "Read tail is : "
              << static_cast<int>(this->ringbuffer.read_partition.tail_index)
              << std::endl;
    std::cout << "Read size is : "
              << static_cast<int>(this->ringbuffer.read_partition.size)
              << std::endl;
    std::cout << "Read base is : "
              << static_cast<int>(this->ringbuffer.read_partition.base_index)
              << std::endl;

    std::cout << "Write head is :"
              << static_cast<int>(this->ringbuffer.write_partition.head_index)
              << std::endl;
    std::cout << "Write tail is : "
              << static_cast<int>(this->ringbuffer.write_partition.tail_index)
              << std::endl;
    std::cout << "Write size is : "
              << static_cast<int>(this->ringbuffer.write_partition.size)
              << std::endl;
    std::cout << "Write base is : "
              << static_cast<int>(this->ringbuffer.write_partition.base_index)
              << std::endl;

    for (size_t i = 0; i < this->_ring_buffer.size(); ++i) {
      std::cout << static_cast<int>(this->_ring_buffer[i]) << " ";
    }
    std::cout << std::endl;
  }

private:
  std::vector<DataFormat> _ring_buffer;
  struct ringbuffer_partition_t {
    uint64_t head_index;
    uint64_t tail_index;
    uint64_t base_index;
    uint64_t size;
  };

  //! ring-buffer = write-able partition + read-able partition
  struct ringbuffer_t {
    uint64_t buffer_size;
    mutable std::mutex rw_mutex_;
    ringbuffer_partition_t read_partition;
    ringbuffer_partition_t write_partition;
  };
  ringbuffer_t ringbuffer;

private:
  void data_write(std::vector<DataFormat> &data) {
    uint64_t write_offset_index = this->ringbuffer.write_partition.tail_index -
                                  this->ringbuffer.write_partition.base_index;
    uint64_t vector_continuous_writeable_size =
        this->ringbuffer.buffer_size - write_offset_index;

    if (data.size() < vector_continuous_writeable_size) {
      std::cout << "data_write step 1" << std::endl;
      std::copy(data.begin(), data.end(),
                this->_ring_buffer.begin() + write_offset_index);
    } else {
      std::cout << "data_write step 2" << std::endl;
      std::copy(data.begin(), data.begin() + vector_continuous_writeable_size,
                this->_ring_buffer.begin() + write_offset_index);
      std::copy(data.begin() + vector_continuous_writeable_size, data.end(),
                this->_ring_buffer.begin());
    }
  }

  void data_read(std::vector<DataFormat> &data) {
    uint64_t read_offset_index = this->ringbuffer.read_partition.tail_index -
                                 this->ringbuffer.read_partition.base_index;
    uint64_t vector_continuous_readable_size =
        this->ringbuffer.buffer_size - read_offset_index;

    // data read wrap around
    if (data.size() < vector_continuous_readable_size) {
      // std::cout << "data_read step 1" << std::endl;
      std::copy(this->_ring_buffer.begin() + read_offset_index,
                this->_ring_buffer.begin() + read_offset_index + data.size(),
                data.begin());
    } else {
      // std::cout << "data_read step 2" << std::endl;
      std::copy(this->_ring_buffer.begin() + read_offset_index,
                this->_ring_buffer.begin() + read_offset_index +
                    vector_continuous_readable_size,
                data.begin());
      std::copy(this->_ring_buffer.begin(),
                this->_ring_buffer.begin() + data.size() -
                    vector_continuous_readable_size,
                data.begin() + vector_continuous_readable_size);
    }
  }

  // Even at a writing speed of 5 billion bytes per second, it would take 1,200
  // years to reach the upper limit of the uint64_t range, and this would hardly
  // trigger anything.
  bool ringbuffer_health_check() {
    // head is always bigger or equal than tail
    if (this->ringbuffer.read_partition.head_index <
        this->ringbuffer.read_partition.tail_index) {
      return false;
    }

    if (this->ringbuffer.write_partition.head_index <
        this->ringbuffer.write_partition.tail_index) {
      return false;
    }

    return true;
  }

  void overbig_force_reset() { clear_ringbuffer(); }
};

#endif // __RINGBUFFER_HPP__