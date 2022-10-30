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

#ifndef __xfutil_lru_cache_h__
#define __xfutil_lru_cache_h__

#include <memory>
#include <mutex>
#include <unordered_map>
#include <list>
#include "xfdb/util_types.h"

namespace xfutil 
{

template<class Key, class Value>
struct Node
{
	Key key;
	Value value;
	size_t value_size;
};

template < class Key, class Value >
class LruCache
{
	typedef Node<Key, Value> LruNode;
	typedef typename std::list<LruNode>::iterator NodeIterator;

public:
	LruCache(size_t max_size) : m_max_hot_size(max_size*0.6), m_max_cold_size(max_size-m_max_hot_size)
	{
		m_hit_count = 0;
		m_miss_count = 0;

		m_hot_size = 0;
		m_cold_size = 0;
	}
	~LruCache()
	{

	}
	
public:	
	void Add(const Key& key, const Value& value, size_t value_size)
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		ReserveColdList(value_size);

		//判断是否已存在，存在则不操作
		const auto it = m_cold_map.find(key);
		if (it == m_cold_map.end())
		{
			LruNode node = {key, value, value_size};

			m_cold_list.push_front(node);
			m_cold_map[key] = m_cold_list.begin();
			m_cold_size += value_size;
		}
	}

	bool Get(const Key& key, Value& value)
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		const auto it = m_hot_map.find(key);
		if (it != m_hot_map.end())
		{
			value = it->second->value;
			++m_hit_count;	

			LruNode node = *it->second;
			m_hot_list.erase(it->second);
			m_hot_list.push_front(node);
			it->second = m_hot_list.begin();

			return true;
		}
		const auto it2 = m_cold_map.find(key);
		if(it2 != m_cold_map.end())
		{
			value = it2->second->value;
			++m_hit_count;

			LruNode node = *it2->second;

			m_cold_size -= it2->second->value_size;
			m_cold_list.erase(it2->second);
			m_cold_map.erase(it2);

			ReserveHotList(node.value_size);

			m_hot_list.push_front(node);
			m_hot_map[node.key] = m_hot_list.begin();

			return true;
		}

		++m_miss_count;
		return false;
	}

	bool Delete(const Key& key)
	{
		std::lock_guard<std::mutex> lock(m_mutex);

		const auto it = m_hot_map.find(key);
		if (it != m_hot_map.end())
		{
			m_hot_size -= it->second->value_size;
			m_hot_list.erase(it->second);
			m_hot_map.erase(it);
			return true;
		}
		else
		{
			const auto it = m_cold_map.find(key);
			if(it != m_cold_map.end())
			{
				m_cold_size -= it->second->value_size;
				m_cold_list.erase(it->second);
				m_cold_map.erase(it);
				return true;
			}
		}

		return false;
	}	
	
	size_t Size()
	{
		std::lock_guard<std::mutex> lock(m_mutex);
		return m_hot_size + m_cold_size;		
	}

private:
	void ReserveColdList(size_t value_size)
	{
		while(!m_cold_list.empty() && m_cold_size + value_size > m_max_cold_size)
		{
			auto node = m_cold_list.back();

			m_cold_size -= node.value_size;
			m_cold_map.erase(node.key);
			m_cold_list.pop_back();
		}
	}

	void ReserveHotList(size_t value_size)
	{
		while(!m_hot_list.empty() && m_hot_size + value_size > m_max_hot_size)
		{
			LruNode node = m_hot_list.back();

			m_hot_size -= node.value_size;
			m_hot_map.erase(node.key);
			m_hot_list.pop_back();

			m_cold_size += node.value_size;
			m_cold_list.push_front(node);
			auto it = m_cold_map.insert(std::make_pair(node.key, m_cold_list.begin()));
			if(!it.second)
			{
				m_cold_size -= it.first->second->value_size;
				m_cold_list.erase(it.first->second);

				it.first->second = m_cold_list.begin();
			}
		}
	}

private:
	const size_t m_max_hot_size;
	const size_t m_max_cold_size;

	std::mutex m_mutex;

	size_t m_hit_count;
	size_t m_miss_count;

	size_t m_hot_size;
	std::list<LruNode> m_hot_list;
	std::unordered_map<Key, NodeIterator> m_hot_map;

	size_t m_cold_size;
	std::list<LruNode> m_cold_list;
	std::unordered_map<Key, NodeIterator> m_cold_map;
	
private:
	LruCache(const LruCache&) = delete;
	LruCache& operator=(const LruCache&) = delete;
};

} 

#endif


