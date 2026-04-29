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
#ifndef __RINGBUFFER_H__
#define __RINGBUFFER_H__

#define unlikely(x) __builtin_expect(!!(x), 0)

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

typedef struct{
        // tail <-> head
        uint64_t head; // Record the monotonically increasing pointer index, starting from 0
        uint64_t tail; // Record the monotonically increasing pointer index, starting from 0
        uint64_t base; // Record the starting index of the array's element indices, starting from 0
        uint16_t size; // R/W Partition size
}RW_Part_T;

typedef struct {
        RW_Part_T w_able_part;
        RW_Part_T r_able_part;
        uint16_t ringbuffer_size;
        uint8_t *ringbuffer_ptr;
}RingBuffer_T;

RingBuffer_T init_ringbuffer(uint16_t ringbuffer_size)
{
        RingBuffer_T ringbuffer;
        ringbuffer.ringbuffer_size = ringbuffer_size;

        ringbuffer.r_able_part.head = 0;
        ringbuffer.r_able_part.tail = 0;
        ringbuffer.r_able_part.base = 0;
        ringbuffer.r_able_part.size = 0;
        ringbuffer.w_able_part.head = ringbuffer.ringbuffer_size;
        ringbuffer.w_able_part.tail = 0;
        ringbuffer.w_able_part.base = 0;
        ringbuffer.w_able_part.size = ringbuffer_size;

        ringbuffer.ringbuffer_ptr = (uint8_t *)malloc(ringbuffer_size);
        memset(ringbuffer.ringbuffer_ptr, 0x00, ringbuffer_size);

        return ringbuffer;
}

void deinit_ringbuffer(RingBuffer_T *ringbuffer)
{
        free(ringbuffer->ringbuffer_ptr);
        ringbuffer->ringbuffer_ptr = NULL;
        ringbuffer->ringbuffer_size = 0;
        ringbuffer->r_able_part.head = 0;
        ringbuffer->r_able_part.tail = 0;
        ringbuffer->r_able_part.size = 0;
        ringbuffer->r_able_part.base = 0;
        ringbuffer->w_able_part.head = 0;
        ringbuffer->w_able_part.tail = 0;
        ringbuffer->w_able_part.size = 0;
        ringbuffer->w_able_part.base = 0;
}

bool is_buffer_full(RingBuffer_T *ringbuffer)
{
        return (ringbuffer->w_able_part.head == ringbuffer->r_able_part.tail);
}

uint64_t get_buffer_free_size(RingBuffer_T *ringbuffer)
{
        return (ringbuffer->w_able_part.size);
}

uint64_t get_buffer_used_size(RingBuffer_T *ringbuffer)
{
        return (ringbuffer->r_able_part.size);
}

// Even at a writing speed of 5 billion bytes per second, it would take 1,200 years to reach the upper limit of the uint64_t range, and this would hardly trigger anything.
bool ringbuffer_health_check(RingBuffer_T *ringbuffer)
{
        // head is always bigger or equal than tail
        if(ringbuffer->r_able_part.head < ringbuffer->r_able_part.tail){
                return false;
        }

        if(ringbuffer->w_able_part.head < ringbuffer->w_able_part.tail){
                return false;
        }

        return true;
}

void overbig_force_reset(RingBuffer_T *ringbuffer)
{
        memset(ringbuffer->ringbuffer_ptr, 0x00, ringbuffer->ringbuffer_size);
        ringbuffer->ringbuffer_size = 0;
        ringbuffer->r_able_part.head = 0;
        ringbuffer->r_able_part.tail = 0;
        ringbuffer->r_able_part.size = 0;
        ringbuffer->r_able_part.base = 0;
        ringbuffer->w_able_part.head = 0;
        ringbuffer->w_able_part.tail = 0;
        ringbuffer->w_able_part.size = 0;
        ringbuffer->w_able_part.base = 0;
}

bool put(uint8_t *data, uint16_t data_len, RingBuffer_T *ringbuffer)
{
        if(data_len > ringbuffer->ringbuffer_size){
                return false;
        }

        if(data_len > ringbuffer->w_able_part.size){
                return false;
        }

        // write
        uint64_t put_offset_index = ringbuffer->w_able_part.tail - ringbuffer->w_able_part.base;
        uint64_t continue_w_able_size = ringbuffer->ringbuffer_size - put_offset_index;

        if(data_len < continue_w_able_size){
                memcpy(&ringbuffer->ringbuffer_ptr[put_offset_index], data, data_len);
        }else{
                memcpy(&ringbuffer->ringbuffer_ptr[put_offset_index], data, continue_w_able_size);
                memcpy(&ringbuffer->ringbuffer_ptr[0], data + continue_w_able_size, data_len - continue_w_able_size);
        }

        ringbuffer->w_able_part.tail += data_len;
        ringbuffer->w_able_part.size -= data_len;
        if((ringbuffer->w_able_part.tail - ringbuffer->w_able_part.base) >= ringbuffer->ringbuffer_size){
                ringbuffer->w_able_part.base += ringbuffer->ringbuffer_size;
        }

        // read
        ringbuffer->r_able_part.head += data_len;
        ringbuffer->r_able_part.size += data_len;

        if(unlikely(!ringbuffer_health_check(ringbuffer))){
                overbig_force_reset(ringbuffer);
                return false;
        }

        return true;
}

bool get(uint8_t *data, uint16_t data_len, RingBuffer_T *ringbuffer)
{
        if(data_len > ringbuffer->ringbuffer_size){
                return false;
        }

        if(data_len > ringbuffer->r_able_part.size){
                return false;
        }

        // read
        uint64_t get_offset_index = ringbuffer->r_able_part.tail - ringbuffer->r_able_part.base;
        uint64_t continuous_r_able_size = ringbuffer->ringbuffer_size - get_offset_index;

        if(data_len < continuous_r_able_size){
                memcpy(data, &ringbuffer->ringbuffer_ptr[get_offset_index], data_len);
        }else{
                memcpy(data, &ringbuffer->ringbuffer_ptr[get_offset_index], continuous_r_able_size);
                memcpy(data + continuous_r_able_size, &ringbuffer->ringbuffer_ptr[0], data_len - continuous_r_able_size);
        }

        ringbuffer->r_able_part.tail += data_len;
        ringbuffer->r_able_part.size -= data_len;
        if((ringbuffer->r_able_part.tail - ringbuffer->r_able_part.base) >= ringbuffer->ringbuffer_size){
                ringbuffer->r_able_part.base += ringbuffer->ringbuffer_size;
        }

        // write
        ringbuffer->w_able_part.head += data_len;
        ringbuffer->w_able_part.size += data_len;

        if(unlikely(!ringbuffer_health_check(ringbuffer))){
                overbig_force_reset(ringbuffer);
                return false;
        }

        return true;
}

#ifdef __cplusplus
}
#endif

#endif // __RINGBUFFER_H__