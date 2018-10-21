
#ifndef  __CACHE_UTILITY_H_
#define  __CACHE_UTILITY_H_

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>

const unsigned NILL = 0xFFFFFFFF;

const int CACHE_NORMAL = 0;		/**< 正常返回     */
const int CACHE_NONE = 1;		/**< 什么也不做	*/
const int CACHE_NOTIN = 2;		/**< 不在cache中  */
const int CACHE_EXPIRATE = 3;	/**< 在cache中但是失效 */
const int CACHE_EXIST = 4;		/**< 在cache中且有效 */
const int CACHE_ERROR = -1;		/**< 操作错误     */
const int CACHE_READERR = -2;	/**< cache数据读取错误       */
const int CACHE_STAERR = -3;	/**< cache状态错误		*/
const int CACHE_NOT_ENOUGH = -4;	/**< 输入buf大小不够	*/
const int CACHE_TIMEOUT = -5;   /**< timeout */


const int ONLY_MEMORY = 0x01;	/**< 只使用内存       */
const int ONLY_DISK = 0x02;		/**< 只使用硬盘       */
const int MEMORY_DISK = 0x3;	/**< 内存-硬盘都使用       */
const int TIME_CACHE = 0x4;      /**< time cache   */

const int NOT_MUTEX = 0x10;		/**< 不使用互斥锁       */
const int USE_MUTEX = 0x20;		/**< 使用互斥锁       */
const int USE_TOMUTEX = 0x40;	/**< 使用超时互斥锁       */

#define SFREE(a) \
	if ((a)) { \
		free((a)); \
		(a) = NULL; \
	}

#define LOCK_MD_CACHE(pcache) \
	if (is_mutex((pcache)->cache_mode)) \
	{\
		pthread_mutex_lock(&((pcache)->mutex)); \
	}

#define MULLOCK_MD_CACHE(pcache, ret) \
	(ret) = 0; \
	if (is_only_mutex((pcache)->cache_mode)) \
	{ \
		pthread_mutex_lock(&((pcache)->mutex)); \
	} \
	else if (is_tomutex((pcache)->cache_mode)) \
	{ \
		(ret) = pthread_mutex_timedlock(&((pcache)->mutex), &((pcache)->timeout)); \
	}

#define UNLOCK_MD_CACHE(pcache) \
	if (is_mutex((pcache)->cache_mode)) \
	{\
		pthread_mutex_unlock(&((pcache)->mutex)); \
	}

inline bool is_only_memory(int mode)
{
	mode &= 0x03;
	return (mode == ONLY_MEMORY);
}

inline bool is_memory_mode(int mode)
{
	return (mode & ONLY_MEMORY);
}

inline bool is_only_disk(int mode)
{
	mode &= 0x03;
	return (mode == ONLY_DISK);
}

inline bool is_disk_mode(int mode)
{
	return (mode & ONLY_DISK);
}

inline bool is_md_mode(int mode)
{
	mode &= 0x03;
	return (mode == MEMORY_DISK);
}

inline bool is_twindow_mode(int mode)
{
    return (mode & TIME_CACHE);
}

inline bool is_nomutex(int mode)
{
	return (mode & NOT_MUTEX);
}

inline bool is_only_mutex(int mode)
{
	return (mode & USE_MUTEX);
}

inline bool is_tomutex(int mode)
{
	return (mode & USE_TOMUTEX);
}

inline bool is_mutex(int mode)
{
	return (is_only_mutex(mode) || is_tomutex(mode));
}

#endif  //__CACHE_UTILITY_H_


