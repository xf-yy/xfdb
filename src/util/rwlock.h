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

#ifndef __xfutil_rwlock_h__
#define __xfutil_rwlock_h__

#ifdef __linux__
#include <pthread.h>
#endif


#ifdef __linux__
typedef pthread_rwlock_t rwlock_t;
static inline bool rwlock_init(rwlock_t* lock)
{
	return (pthread_rwlock_init(lock, nullptr) == 0);
}
static inline bool rwlock_destroy(rwlock_t* lock)
{
	return (pthread_rwlock_destroy(lock) == 0);
}
static inline bool read_lock(rwlock_t* lock)
{
	return (pthread_rwlock_rdlock(lock) == 0);
}
static inline bool read_trylock(rwlock_t* lock)
{
	return (pthread_rwlock_tryrdlock(lock) == 0);
}

static inline bool read_unlock(rwlock_t* lock)
{
	return (pthread_rwlock_unlock(lock) == 0);
}

static inline bool write_lock(rwlock_t* lock)
{
	return (pthread_rwlock_wrlock(lock) == 0);
}
static inline bool write_trylock(rwlock_t* lock)
{
	return (pthread_rwlock_trywrlock(lock) == 0);
}

static inline bool write_unlock(rwlock_t* lock)
{
	return (pthread_rwlock_unlock(lock) == 0);
}

#else

#error "not support this platform"
#endif

namespace xfutil
{

class ReadWriteLock
{
public:
	ReadWriteLock()
	{
		rwlock_init(&m_bucket_rwlock);
	}
	~ReadWriteLock()
	{
		rwlock_destroy(&m_bucket_rwlock);
	}
	
	bool ReadLock()
	{
		return read_lock(&m_bucket_rwlock);
	}
	bool ReadUnlock()
	{
		return read_unlock(&m_bucket_rwlock);
	}
	bool WriteLock()
	{
		return write_lock(&m_bucket_rwlock);
	}

	bool WriteUnlock()
	{
		return write_unlock(&m_bucket_rwlock);
	}
	
private:
	rwlock_t m_bucket_rwlock;
	
private:
	ReadWriteLock(const ReadWriteLock&) = delete;
	ReadWriteLock& operator=(const ReadWriteLock&) = delete;
};

class ReadLockGuard
{
public:
	ReadLockGuard(ReadWriteLock& rwlock) : m_rwlock(rwlock)
	{
		rwlock.ReadLock();
	}
	~ReadLockGuard()
	{
		m_rwlock.ReadUnlock();
	}
	
private:
	ReadWriteLock& m_rwlock;
	
private:
	ReadLockGuard(const ReadLockGuard&) = delete;
	ReadLockGuard& operator=(const ReadLockGuard&) = delete;
};

class WriteLockGuard
{
public:
	WriteLockGuard(ReadWriteLock& rwlock) : m_rwlock(rwlock)
	{
		rwlock.WriteLock();
	}
	~WriteLockGuard()
	{
		m_rwlock.WriteUnlock();
	}
	
private:
	ReadWriteLock& m_rwlock;
	
private:
	WriteLockGuard(const WriteLockGuard&) = delete;
	WriteLockGuard& operator=(const WriteLockGuard&) = delete;
};


}

#endif

