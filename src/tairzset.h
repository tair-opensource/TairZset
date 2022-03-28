/*
 * Copyright 2021 Alibaba Tair Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "redismodule.h"
#include "skiplist.h"
#include "dict.h"
#include "util.h"

#include <string.h>
typedef struct TairZsetObj {
    dict *dict;
    m_zskiplist *zsl;
} TairZsetObj;

uint64_t dictModuleStrHash(const void *key) {
    size_t len;
    const char *buf = RedisModule_StringPtrLen(key, &len);
    return m_dictGenHashFunction(buf, (int)len);
}

int dictModuleStrKeyCompare(void *privdata, const void *key1,
                            const void *key2) {
    size_t l1, l2;
    DICT_NOTUSED(privdata);

    const char *buf1 = RedisModule_StringPtrLen(key1, &l1);
    const char *buf2 = RedisModule_StringPtrLen(key2, &l2);
    if (l1 != l2) return 0;
    return memcmp(buf1, buf2, l1) == 0;
}

m_dictType tairZsetDictType = {
    dictModuleStrHash,       /* hash function */
    NULL,                    /* key dup */
    NULL,                    /* val dup */
    dictModuleStrKeyCompare, /* key compare */
    NULL,                    /* Note: RedisModleString shared & freed by skiplist */
    NULL                     /* val destructor */
};

int parse_score(const char * buf, size_t len, scoretype *score);
int score_cmp(double *s1, double *s2, unsigned char num);