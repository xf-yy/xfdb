/*************************************************************************
Copyright (C) 2022 The xfdb Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
***************************************************************************/

#include "bloom_filter.h"

namespace xfutil
{

BloomFilter::BloomFilter(uint32_t bitnum_perkey) : m_bitnum_perkey(bitnum_perkey)
{
	m_k_num = (uint32_t)(bitnum_perkey * 0.69314);
	if(m_k_num < 1)
	{
		m_k_num = 1;
	}
	else if (m_k_num > 30)
	{
		m_k_num = 30;
	}
}


/**申请空间，返回数据指针*/
bool BloomFilter::Create(const uint32_t* hash_ptr, uint32_t count)
{
	if(count == 0)
	{
		return false;
	}
	uint64_t total_bits = (uint64_t)count * m_bitnum_perkey;

	//对齐到字节
	size_t total_bytes = (total_bits + 7) / 8;
	total_bits = total_bytes * 8;

	m_data.resize(total_bytes);

	byte_t* data = (byte_t*)m_data.data();
	for(uint32_t i = 0; i < count; ++i) 
	{
		uint32_t hc = hash_ptr[i];

		for (uint32_t j = 0; j < m_k_num; ++j) 
		{
			const uint32_t bit_pos = hc % total_bits;
			data[bit_pos / 8] |= (1 << (bit_pos % 8));

			hc = (hc >> 1)  | (hc << 31);
		}
	}
	return true;
}

bool BloomFilter::Create(std::deque<uint32_t>& hashs)
{
	if(hashs.empty())
	{
		return false;
	}
	uint64_t total_bits = (uint64_t)hashs.size() * m_bitnum_perkey;

	//对齐到字节
	size_t total_bytes = (total_bits + 7) / 8;
	total_bits = total_bytes * 8;

	m_data.resize(total_bytes);

	byte_t* data = (byte_t*)m_data.data();
	while(!hashs.empty())
	{
		uint32_t hc = hashs.front();
		hashs.pop_front();

		for (uint32_t j = 0; j < m_k_num; ++j) 
		{
			const uint32_t bit_pos = hc % total_bits;
			data[bit_pos / 8] |= (1 << (bit_pos % 8));

			hc = (hc >> 1)  | (hc << 31);
		}
	}
	return true;
}

/**清除所有数据*/
bool BloomFilter::Check(uint32_t hc)
{
	if(m_data.empty())
	{
		return true;
	}
	const byte_t* data = (byte_t*)m_data.data();
	uint64_t total_bits = (uint64_t)m_data.size() * 8;

	for (size_t j = 0; j < m_k_num; ++j) 
	{
		const uint32_t bit_pos = hc % total_bits;
		if((data[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) 
		{
			return false;
		}
		hc = (hc >> 1)  | (hc << 31);
	}
	return true;		
}


}


