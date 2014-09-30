#define _CRT_SECURE_NO_DEPRECATE
#define _SCL_SECURE_NO_WARNINGS

#define SAFE_FREE(ptr) if (ptr != nullptr) {free(ptr); ptr = nullptr;}