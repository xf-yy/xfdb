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

#ifndef __xfdb_spinlock_h__
#define __xfdb_spinlock_h__

#ifdef __linux__

#include <pthread.h>
typedef pthread_spinlock_t spinlock_t;
static inline bool spinlock_init(spinlock_t* lock)
{
	return (pthread_spin_init(lock, PTHREAD_PROCESS_PRIVATE) == 0);
}
static inline bool spinlock_destroy(spinlock_t* lock)
{
	return (pthread_spin_destroy(lock) == 0);
}
static inline bool spinlock_lock(spinlock_t* lock)
{
	return (pthread_spin_lock(lock) == 0);
}
static inline bool spinlock_trylock(spinlock_t* lock)
{
	return (pthread_spin_trylock(lock) == 0);
}
static inline bool spinlock_unlock(spinlock_t* lock)
{
	return (pthread_spin_unlock(lock) == 0);
}

#else
#endif

namespace xfutil
{
class SpinLockGuard
{
public:
	SpinLockGuard(spinlock_t& lock) : m_lock(lock)
	{
		spinlock_lock(&m_lock);
	}
	~SpinLockGuard()
	{
		spinlock_unlock(&m_lock);
	}
	
private:
	spinlock_t& m_lock;
	
private:
	SpinLockGuard(const SpinLockGuard&) = delete;
	SpinLockGuard& operator=(const SpinLockGuard&) = delete;
};

}

#endif

