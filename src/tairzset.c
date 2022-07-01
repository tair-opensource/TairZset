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

#include "tairzset.h"
#include "adlist.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#define TAIRZSET_ENCVER_VER_1 0

static RedisModuleType *TairZsetType;

static struct TairZsetObj *createTairZsetTypeObject(int score_num) {
    TairZsetObj *obj = RedisModule_Calloc(1, sizeof(TairZsetObj));
    obj->dict = m_dictCreate(&tairZsetDictType, NULL);
    obj->zsl = m_zslCreate(score_num);
    return obj;
}

static void TairZsetTypeReleaseObject(struct TairZsetObj *obj) {
    if (!obj) {
        return;
    }

    m_dictRelease(obj->dict);
    m_zslFree(obj->zsl);
    RedisModule_Free(obj);
}

static int mstringcasecmp(const RedisModuleString *rs1, const char *s2) {
    size_t n1 = strlen(s2);
    size_t n2;
    const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
    if (n1 != n2) {
        return -1;
    }
    return strncasecmp(s1, s2, n1);
}

static unsigned long exZsetLength(const TairZsetObj *zobj) {
    return zobj->zsl->length;
}


/* ========================= "tairzset" set operations =======================*/

/* Sets operations codes */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2
/* Sets aggregation types */
#define AGGR_SUM 1
#define AGGR_MIN 2
#define AGGR_MAX 3

typedef struct {
    TairZsetObj *subject;
    double weight;
    struct _zset_iter {
        TairZsetObj *zs;
        m_zskiplistNode *node;
    } zset_iter;
} zsetopsrc;

typedef struct _zset_iter iterzset;

/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
typedef struct {
    RedisModuleString *ele;
    scoretype *score;
} zsetopval;

/* uid means union, inter, diff */

unsigned long exZuidLength(zsetopsrc *op) {
    if (op->subject == NULL) {
        return 0;
    }
    return exZsetLength(op->subject);
}

int exZuidCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = exZuidLength((zsetopsrc *)s1);
    unsigned long second = exZuidLength((zsetopsrc *)s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

void exZuidInitIterator(zsetopsrc *op) {
    if (op->subject == NULL) {
        return;
    }
    iterzset *it = &op->zset_iter;
    it->zs = op->subject;
    it->node = it->zs->zsl->tail;
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
int exZuidNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL) {
        return 0;
    }
    memset(val, 0, sizeof(zsetopval));
    iterzset *it = &op->zset_iter;
    if (it->node == NULL)
        return 0;
    val->ele = it->node->ele;
    val->score = it->node->score;

    /* Move to next element. (going backwards, see exZuidInitIterator) */
    it->node = it->node->backward;
    return 1;
}

inline static void exZunionInterAggregate(scoretype *target, scoretype *score, int aggregate) {
    if (aggregate == AGGR_SUM) {
        mscoreAddIgnoreNan(target, score);
    } else if (aggregate == AGGR_MIN && mscoreCmp(target, score) > 0) {
        mscoreAssign(target, score);   
    } else if (aggregate == AGGR_MAX && mscoreCmp(target, score) < 0) {
        mscoreAssign(target, score);   
    }
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in 'score' (deep copy). Return 0 otherwise. */
int exZuidFind(zsetopsrc *op, zsetopval *val, scoretype *score) {
    assert(score != NULL);

    if (op->subject == NULL) {
        return 0;
    }

    TairZsetObj *zobj = op->subject;
    m_dictEntry *de;
    if ((de = m_dictFind(zobj->dict, val->ele)) != NULL) {
        mscoreAssign(score, dictGetVal(de));
        return 1;
    } 
    return 0;
}

/* ========================= "tairzset" common functions =======================*/
static int exZsetScore(TairZsetObj *obj, RedisModuleString *member, scoretype **score) {
    if (!obj || !member) {
        return C_ERR;
    }

    m_dictEntry *de = m_dictFind(obj->dict, member);
    if (de == NULL) {
        return C_ERR;
    }

    *score = (scoretype *)dictGetVal(de);

    return C_OK;
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
void exGenericZrangebylexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    m_zlexrangespec range;
    RedisModuleString *key = argv[1];
    TairZsetObj *zobj = NULL;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    int minidx, maxidx;

    if (reverse) {
        maxidx = 2;
        minidx = 3;
    } else {
        minidx = 2;
        maxidx = 3;
    }

    if (m_zslParseLexRange(argv[minidx], argv[maxidx], &range) != C_OK) {
        RedisModule_ReplyWithError(ctx, "ERR min or max not valid string range item");
        return;
    }

    if (argc > 4) {
        int remaining = argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 3 && !mstringcasecmp(argv[pos], "limit")) {
                if ((RedisModule_StringToLongLong(argv[pos + 1], (long long *)&offset) != REDISMODULE_OK) || 
                (RedisModule_StringToLongLong(argv[pos + 2], (long long *)&limit) != REDISMODULE_OK)) {
                    RedisModule_ReplyWithError(ctx, "ERR value is out of range");
                    return;
                }
                pos += 3;
                remaining -= 3;
            } else {
                m_zslFreeLexRange(&range);
                RedisModule_ReplyWithError(ctx, "ERR syntax error");
                return;
            }
        }
    }

    RedisModuleKey *real_key = NULL;
    real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
    int type = RedisModule_KeyType(real_key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(real_key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    } else {
        zobj = RedisModule_ModuleTypeGetValue(real_key);
    }

    m_zskiplist *zsl = zobj->zsl;
    m_zskiplistNode *ln;

    if (reverse) {
        ln = m_zslLastInLexRange(zsl, &range);
    } else {
        ln = m_zslFirstInLexRange(zsl, &range);
    }

    if (ln == NULL) {
        RedisModule_ReplyWithArray(ctx, 0);
        m_zslFreeLexRange(&range);
        return;
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    while (ln && offset--) {
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }

    while (ln && limit--) {
        if (reverse) {
            if (!m_zslLexValueGteMin(ln->ele, &range)) break;
        } else {
            if (!m_zslLexValueLteMax(ln->ele, &range)) break;
        }

        rangelen++;
        RedisModule_ReplyWithString(ctx, ln->ele);

        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }

    m_zslFreeLexRange(&range);
    RedisModule_ReplySetArrayLength(ctx, rangelen);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
static void exGenericZrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    m_zrangespec range;
    RedisModuleString *key = argv[1];
    TairZsetObj *zobj = NULL;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    int minidx, maxidx;

    if (reverse) {
        maxidx = 2;
        minidx = 3;
    } else {
        minidx = 2;
        maxidx = 3;
    }

    range.max = NULL;
    range.min = NULL;

    if (m_zslParseRange(argv[minidx], argv[maxidx], &range) != C_OK) {
        RedisModule_ReplyWithError(ctx, "ERR min or max is not a float");
        goto fee_range;
    }

    if (argc > 4) {
        int remaining = argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 1 && !mstringcasecmp(argv[pos], "withscores")) {
                pos++;
                remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !mstringcasecmp(argv[pos], "limit")) {
                if ((RedisModule_StringToLongLong(argv[pos + 1], (long long *)&offset) != REDISMODULE_OK) || 
                (RedisModule_StringToLongLong(argv[pos + 2], (long long *)&limit) != REDISMODULE_OK)) {
                    RedisModule_ReplyWithError(ctx, "ERR value is out of range");
                    goto fee_range;
                }

                pos += 3;
                remaining -= 3;
            } else {
                RedisModule_ReplyWithError(ctx, "ERR syntax error");
                goto fee_range;
            }
        }
    }

    RedisModuleKey *real_key = NULL;
    real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
    int type = RedisModule_KeyType(real_key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(real_key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        goto fee_range;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 0);
        goto fee_range;
    } else {
        zobj = RedisModule_ModuleTypeGetValue(real_key);
    }

    if (range.max->score_num != zobj->zsl->score_num || range.min->score_num != zobj->zsl->score_num) {
        RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
        goto fee_range;
    }

    m_zskiplist *zsl = zobj->zsl;
    m_zskiplistNode *ln;

    if (reverse) {
        ln = m_zslLastInRange(zsl, &range);
    } else {
        ln = m_zslFirstInRange(zsl, &range);
    }

    if (ln == NULL) {
        RedisModule_ReplyWithArray(ctx, 0);
        goto fee_range;
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    while (ln && offset--) {
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }

    while (ln && limit--) {
        if (reverse) {
            if (!m_zslValueGteMin(ln->score, &range)) break;
        } else {
            if (!m_zslValueLteMax(ln->score, &range)) break;
        }

        rangelen++;
        RedisModule_ReplyWithString(ctx, ln->ele);

        if (withscores) {
            sds score_str = mscore2String(ln->score);
            RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
            m_sdsfree(score_str);
        }

        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }

    if (withscores) {
        rangelen *= 2;
    }

    RedisModule_ReplySetArrayLength(ctx, rangelen);

fee_range:
    RedisModule_Free(range.max);
    RedisModule_Free(range.min);
}

static int exZsetAdd(TairZsetObj *obj, scoretype *score, RedisModuleString *ele, int *flags, scoretype **newscore) {
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; 
    scoretype *curscore;

    m_zskiplistNode *znode;

    m_dictEntry *de;
    de = m_dictFind(obj->dict, ele);
    if (de != NULL) {
        if (nx) {
            *flags |= ZADD_NOP;
            return 1;
        }

        curscore = (scoretype *)dictGetVal(de);

        if (incr) {
            int ret = mscoreAdd(score, curscore);
            if (ret) {
                *flags |= ZADD_NAN;
                return 0;
            }
            if (newscore) {
                *newscore = score;
            }
        }

        if (mscoreCmp(score, curscore) != 0) {
            znode = m_zslUpdateScore(obj->zsl, curscore, ele, score);
            dictGetVal(de) = znode->score;  
            *flags |= ZADD_UPDATED;
        } else {
            RedisModule_Free(score);
            if (incr && newscore) {
                *newscore = curscore;
            }
        }
        return 1;
    } else if (!xx) {
        ele = RedisModule_CreateStringFromString(NULL, ele);
        znode = m_zslInsert(obj->zsl, score, ele);
        assert(m_dictAdd(obj->dict, ele, znode->score) == DICT_OK);
        *flags |= ZADD_ADDED;
        if (newscore)
            *newscore = score;
        return 1;
    } else {
        *flags |= ZADD_NOP;
        return 1;
    }

    return 0; 
}

static int exZsetRemoveFromSkiplist(TairZsetObj *zobj, RedisModuleString *ele) {
    m_dictEntry *de;
    de = m_dictUnlink(zobj->dict, ele);
    if (de != NULL) {
        scoretype *score = (scoretype *)dictGetVal(de);
        m_dictFreeUnlinkedEntry(zobj->dict, de);
        int retval = m_zslDelete(zobj->zsl, score, ele, NULL);
        assert(retval);

        if (m_htNeedsResize(zobj->dict)) {
            m_dictResize(zobj->dict);
        }
        return 1;
    }

    return 0;
}

int exZsetDel(TairZsetObj *zobj, RedisModuleString *ele) {
    if (exZsetRemoveFromSkiplist(zobj, ele)) {
        return 1;
    }

    return 0; 
}

void exZrangeGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    RedisModuleString *key = argv[1];
    TairZsetObj *zobj = NULL;

    int withscores = 0;
    long start;
    long end;
    long llen;
    long rangelen;

    if ((RedisModule_StringToLongLong(argv[2], (long long *)&start) != REDISMODULE_OK) || 
    (RedisModule_StringToLongLong(argv[3], (long long *)&end) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, "ERR value is out of range");
        return;
    }

    if (argc == 5 && !mstringcasecmp(argv[4], "withscores")) {
        withscores = 1;
    } else if (argc >= 5) {
        RedisModule_ReplyWithError(ctx, "ERR syntax error");
        return;
    }

    RedisModuleKey *real_key = NULL;
    real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
    int type = RedisModule_KeyType(real_key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(real_key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    } else {
        zobj = RedisModule_ModuleTypeGetValue(real_key);
    }

    llen = exZsetLength(zobj);
    if (start < 0) start = llen + start;
    if (end < 0) end = llen + end;
    if (start < 0) start = 0;

    if (start > end || start >= llen) {
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    }
    if (end >= llen) end = llen - 1;
    rangelen = (end - start) + 1;

    RedisModule_ReplyWithArray(ctx, withscores ? (rangelen * 2) : rangelen);

    m_zskiplist *zsl = zobj->zsl;
    m_zskiplistNode *ln;
    RedisModuleString *ele;

    if (reverse) {
        ln = zsl->tail;
        if (start > 0)
            ln = m_zslGetElementByRank(zsl, llen - start);
    } else {
        ln = zsl->header->level[0].forward;
        if (start > 0)
            ln = m_zslGetElementByRank(zsl, start + 1);
    }

    while (rangelen--) {
        ele = ln->ele;
        RedisModule_ReplyWithString(ctx, ele);
        if (withscores) {
            sds score_str = mscore2String(ln->score);
            RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
            m_sdsfree(score_str);
        }
        ln = reverse ? ln->backward : ln->level[0].forward;
    }
}

static void exZaddGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int flags) {
    static char *nanerr = "ERR resulting score is not a number (NaN)";

    RedisModuleString *ele;
    scoretype *score;
    scoretype **scores;
    int j, elements;
    int scoreidx = 0;

    int added = 0;     /* Number of new elements added. */
    int updated = 0;   /* Number of elements with updated score. */
    int processed = 0; /* Number of elements processed, may remain zero with
                           options like XX. */

    scoreidx = 2;
    while (scoreidx < argc) {
        RedisModuleString *opt = argv[scoreidx];
        if (!mstringcasecmp(opt, "nx"))
            flags |= ZADD_NX;
        else if (!mstringcasecmp(opt, "xx"))
            flags |= ZADD_XX;
        else if (!mstringcasecmp(opt, "ch"))
            flags |= ZADD_CH;
        else if (!mstringcasecmp(opt, "incr"))
            flags |= ZADD_INCR;
        else
            break;
        scoreidx++;
    }

    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    int step = 2;

    elements = argc - scoreidx;
    if (elements % step || !elements) {
        RedisModule_ReplyWithError(ctx, "ERR syntax error");
        return;
    }
    elements /= step; 

    if (nx && xx) {
        RedisModule_ReplyWithError(ctx, "ERR XX and NX options at the same time are not compatible");
        return;
    }

    if (incr && elements > 1) {
        RedisModule_ReplyWithError(ctx, "ERR INCR option supports a single increment-element pair");
        return;
    }

    int score_num = 0, last_score_num = 0;
    size_t tmp_score_len;
    const char *tmp_score;
    scores = RedisModule_Calloc(1, sizeof(scoretype *) * elements);
    for (j = 0; j < elements; j++) {
        tmp_score = RedisModule_StringPtrLen(argv[scoreidx + j * step], &tmp_score_len);
        if ((score_num = mscoreParse(tmp_score, tmp_score_len, &scores[j])) <= 0) {
            RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
            goto cleanup;
        }

        if (last_score_num != 0 && last_score_num != score_num) {
            RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
            goto cleanup;
        }

        last_score_num = score_num;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        goto cleanup;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (xx) {
            for (int i = 0; i < elements; i++) {
                RedisModule_Free(scores[i]);
            }
            goto reply_to_client; 
        }

        tair_zset_obj = createTairZsetTypeObject(last_score_num);
        RedisModule_ModuleTypeSetValue(key, TairZsetType, tair_zset_obj);
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
        if (tair_zset_obj->zsl->score_num != last_score_num) {
            RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
            goto cleanup;
        }
    }

    for (j = 0; j < elements; j++) {
        scoretype *new_score = NULL;
        score = scores[j];
        int retflags = flags;

        ele = argv[scoreidx + 1 + j * step];

        int retval = exZsetAdd(tair_zset_obj, score, ele, &retflags, &new_score);
        if (retval == 0) {
            RedisModule_ReplyWithError(ctx, nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_ADDED)
            added++;
        if (retflags & ZADD_UPDATED)
            updated++;
        if (!(retflags & ZADD_NOP))
            processed++;
        if (retflags & ZADD_NOP) {
            RedisModule_Free(score);
        }
        score = new_score;
    }

    RedisModule_ReplicateVerbatim(ctx);

reply_to_client:
    if (incr) { 
        if (processed) {
            sds score_str = mscore2String(score);
            RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
            m_sdsfree(score_str);
        } else {
            RedisModule_ReplyWithNull(ctx);
        }
    } else { 
        RedisModule_ReplyWithLongLong(ctx, ch ? added + updated : added);
    }

    RedisModule_Free(scores);
    return;

cleanup:
    for (int i = 0; i < elements; i++) {
        RedisModule_Free(scores[i]);
    }
    RedisModule_Free(scores);
}

long exZsetRank(TairZsetObj *zobj, RedisModuleString *ele, int reverse, int byscore) {
    unsigned long llen;
    unsigned long rank;

    llen = exZsetLength(zobj);
    m_zskiplist *zsl = zobj->zsl;

    if (byscore) {
        size_t slen;
        int score_num;
        scoretype *score;
        const char *s = RedisModule_StringPtrLen(ele, &slen);
        if ((score_num = mscoreParse(s, slen, &score) <= 0)) {
            return -1;
        }
        rank = m_zslGetRankByScore(zsl, score);
        RedisModule_Free(score);
        if (reverse)
            return llen - rank;
        else
            return rank;
    } else {
        m_dictEntry *de;
        de = m_dictFind(zobj->dict, ele);
        if (de != NULL) {
            scoretype *score = (scoretype *)dictGetVal(de);
            rank = m_zslGetRank(zsl, score, ele);
        } else {
            return -1;
        }
        if (reverse)
            return llen - rank;
        else
            return rank - 1;
    }
}

void exZrankGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse, int byscore) {
    long rank;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    rank = exZsetRank(tair_zset_obj, argv[2], reverse, byscore);
    if (rank >= 0) {
        RedisModule_ReplyWithLongLong(ctx, rank);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
}

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2
void exZremrangeGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int rangetype) {
    TairZsetObj *zobj;
    unsigned long deleted = 0;
    m_zrangespec range;
    m_zlexrangespec lexrange;
    long start, end, llen;

    range.max = NULL;
    range.min = NULL;
    lexrange.max = NULL;
    lexrange.min = NULL;

    if (rangetype == ZRANGE_RANK) {
        if ((RedisModule_StringToLongLong(argv[2], (long long *)&start) != REDISMODULE_OK) || 
        (RedisModule_StringToLongLong(argv[3], (long long *)&end) != REDISMODULE_OK)) {
            RedisModule_ReplyWithError(ctx, "ERR value is out of range");
            goto cleanup;
        }
    } else if (rangetype == ZRANGE_SCORE) {
        if (m_zslParseRange(argv[2], argv[3], &range) != C_OK) {
            RedisModule_ReplyWithError(ctx, "ERR min or max is not a float");
            goto cleanup;
        }
    } else if (rangetype == ZRANGE_LEX) {
        if (m_zslParseLexRange(argv[2], argv[3], &lexrange) != C_OK) {
            RedisModule_ReplyWithError(ctx, "ERR min or max not valid string range item");
            goto cleanup;
        }
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        goto cleanup;
    }

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        goto cleanup;
    } else {
        zobj = RedisModule_ModuleTypeGetValue(key);
    }

    if (rangetype == ZRANGE_SCORE) {
        if (range.max->score_num != zobj->zsl->score_num || range.min->score_num != zobj->zsl->score_num) {
            RedisModule_ReplyWithError(ctx, "score is not a valid format");
            goto cleanup;
        }
    }

    if (rangetype == ZRANGE_RANK) {
        llen = exZsetLength(zobj);
        if (start < 0) start = llen + start;
        if (end < 0) end = llen + end;
        if (start < 0) start = 0;

        if (start > end || start >= llen) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            goto cleanup;
        }
        if (end >= llen) end = llen - 1;
    }

    switch (rangetype) {
        case ZRANGE_RANK:
            deleted = m_zslDeleteRangeByRank(zobj->zsl, start + 1, end + 1, zobj->dict);
            break;
        case ZRANGE_SCORE:
            deleted = m_zslDeleteRangeByScore(zobj->zsl, &range, zobj->dict);
            break;
        case ZRANGE_LEX:
            deleted = m_zslDeleteRangeByLex(zobj->zsl, &lexrange, zobj->dict);
            break;
    }

    if (m_htNeedsResize(zobj->dict)) {
        m_dictResize(zobj->dict);
    }
    if (dictSize(zobj->dict) == 0) {
        RedisModule_DeleteKey(key);
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, deleted);
cleanup:
    if (rangetype == ZRANGE_LEX) m_zslFreeLexRange(&lexrange);
    if (rangetype == ZRANGE_SCORE) {
        RedisModule_Free(range.max);
        RedisModule_Free(range.min);
    }
}

/* Return random element from a non empty exzset.
 * 'ele' will be set to hold the element.
 * The memory in `ele` is not to be freed or modified by the caller.
 * 'score' can be NULL in which case it's not extracted. */
void exZsetRandomElement(TairZsetObj *zobj, RedisModuleString **ele, scoretype **score) {
    m_dictEntry *de =  m_dictGetFairRandomKey(zobj->dict);
    *ele = (RedisModuleString*)dictGetKey(de);
    if (score) {
        *score = (scoretype*)dictGetVal(de);
    }
}

/* How many times bigger should be the zset compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define ZRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
#define ZRANDMEMBER_RANDOM_SAMPLE_LIMIT 1000

void exZrandMemberWithCountCommand(RedisModuleCtx *ctx, TairZsetObj *zobj, long l, int withscores) {
    unsigned long count, size;
    int uniq = 1;

    size =exZsetLength(zobj);

    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        /* RedisModule_ReplyWithEmptyArray is not supported in Redis 5.0 */
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        if (withscores)
            RedisModule_ReplyWithArray(ctx, count*2);
        else
            RedisModule_ReplyWithArray(ctx, count);

        while (count--) {
            m_dictEntry *de = m_dictGetFairRandomKey(zobj->dict);
            RedisModuleString *key = dictGetKey(de);
            RedisModule_ReplyWithString(ctx, key);
            if (withscores) {
                sds score_str = mscore2String(dictGetVal(de));
                RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
                m_sdsfree(score_str);
            }
        }
        return;
    }

    m_zskiplist *zsl = zobj->zsl;
    m_zskiplistNode *ln;
    RedisModuleString *ele;

    /* Initiate reply count. */
    long reply_size = count < size ? count : size;
    if (withscores)
        RedisModule_ReplyWithArray(ctx, reply_size*2);
    else 
        RedisModule_ReplyWithArray(ctx, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the zset: simply return the whole zset. */
    if (count >= size) {
        ln = zsl->header->level[0].forward;
        while (reply_size--) {
            ele = ln->ele;
            RedisModule_ReplyWithString(ctx, ele);
            if (withscores) {
                sds score_str = mscore2String(ln->score);
                RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
                m_sdsfree(score_str);
            }
            ln = ln->level[0].forward;
        }
        return;
    }

    /* CASE 3:
     * The number of elements inside the zset is not greater than
     * ZRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a dict from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count*ZRANDMEMBER_SUB_STRATEGY_MUL > size) {
        dict *d = m_dictCreate(&tairZsetDictType, NULL);
        m_dictExpand(d, size);
        /* Add all the elements into the temporary dictionary. */
        ln = zsl->header->level[0].forward;
        while (ln != NULL) {
            ele = ln->ele;
            m_dictEntry *de = m_dictAddRaw(d, ele, NULL);
            assert(de);
            if (withscores) 
                dictSetVal(d, de, ln->score);
            ln = ln->level[0].forward;
        }
        assert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            m_dictEntry *de;
            de = m_dictGetRandomKey(d);
            m_dictUnlink(d,dictGetKey(de));
            m_dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        m_dictIterator *di;
        m_dictEntry *de;
        di = m_dictGetIterator(d);
        while ((de = m_dictNext(di)) != NULL) {
            RedisModule_ReplyWithString(ctx, dictGetKey(de));
            if (withscores) {
                sds score_str = mscore2String(dictGetVal(de));
                RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
                m_sdsfree(score_str);
            }
        }

        m_dictReleaseIterator(di);
        m_dictRelease(d);
    }

    /* CASE 4: We have a big zset compared to the requested number of elements.
     * In this case we can simply get random elements from the zset and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        /* Hashtable encoding (tair zset implementation) */
        unsigned long added = 0;
        dict *d = m_dictCreate(&tairZsetDictType, NULL);
        m_dictExpand(d, count);

        while (added < count) {
            RedisModuleString *key;
            scoretype *score;
            exZsetRandomElement(zobj, &key, &score);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            if (m_dictAdd(d,key,NULL) != DICT_OK) {
                continue;
            }
            added++;

            RedisModule_ReplyWithString(ctx, key);
            if (withscores) { 
                sds score_str = mscore2String(score);
                RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
                m_sdsfree(score_str);
            }
        }
        /* Release memory */
        m_dictRelease(d);
    }
}

/* This callback is used by exZscanGernericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
void dictScanCallback(void *privdata, const m_dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = pd[0];
    RedisModuleString *key = dictGetKey(de);
    scoretype *val = dictGetVal(de);

    m_listAddNodeTail(keys, key);
    if (val) m_listAddNodeTail(keys, val);
}

/* This command implements EXZSCAN commands.
 *
 * It returns both the members and scores of the scanned tair zset elements. */
void exZscanGernericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, TairZsetObj *zobj, unsigned long cursor) {
    /* Set i to the first option argument. The previous one is cursor. */
    int i = 3, j;
    list *keys = m_listCreate();
    listNode *node, *nextnode;
    long long count = 10;
    const char *pat = NULL;
    size_t patlen;
    int use_pattern = 0;
    dict *ht;

    while (i < argc) {
        j = argc - i;
        if (!mstringcasecmp(argv[i], "count") && j >= 2) {
            if (RedisModule_StringToLongLong(argv[i+1], &count) != REDISMODULE_OK) {
                RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
                goto cleanup;
            }

            if (count < 1) {
                RedisModule_ReplyWithError(ctx, "ERR syntax error");
                goto cleanup;
            }

            i += 2;
        } else if (!mstringcasecmp(argv[i], "match") && j >= 2) {
            pat = RedisModule_StringPtrLen(argv[i+1], &patlen);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } else {
            RedisModule_ReplyWithError(ctx, "ERR syntax error");
            goto cleanup;
        }
    }

    /* Step 2: Iterate the collection. */
    ht = zobj->dict;
    count *= 2;

    void *privdata[1];
    /* We set the max number of iterations to ten times the specified
     * COUNT, so if the hash table is in a pathological state (very
     * sparsely populated) we avoid to block too much time at the cost
     * of returning no or very few elements. */
    long maxiterations = count*10;

    /* We pass two pointers to the callback: the list to which it will
     * add new elements, and the object containing the dictionary so that
     * it is possible to fetch more data in a type-dependent way. */
    privdata[0] = keys;
    do {
        cursor = m_dictScan(ht, cursor, dictScanCallback, NULL, privdata);
    } while (cursor &&
            maxiterations-- &&
            listLength(keys) < (unsigned long)count);

    /* Step 3: Filter elements. */
    node = listFirst(keys);
    while (node) {
        RedisModuleString *member = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;
        size_t member_str_len;
        const char * member_str = RedisModule_StringPtrLen(member, &member_str_len);
        
        /* Filter element if it does not match the pattern. */
        if (!filter && 
            use_pattern && 
            !m_stringmatchlen(pat, patlen, member_str, member_str_len, 0)) {
            filter = 1; 
        }

        /* Remove the element and its associted value if needed. */
        if (filter) {
            /* Remove both member and score */
            m_listDelNode(keys, node);
            node = nextnode;
            nextnode = listNextNode(node);
            m_listDelNode(keys, node);
        } else {
            /* skip the score node associated with the member node */
            node = nextnode;
            nextnode = listNextNode(node);
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    RedisModule_ReplyWithArray(ctx, 2);
    char buf[LONG_STR_SIZE];
    m_ll2string(buf, LONG_STR_SIZE, cursor);
    RedisModule_ReplyWithCString(ctx, buf);
    RedisModule_ReplyWithArray(ctx, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        /* member */
        RedisModuleString *member = listNodeValue(node);
        RedisModule_ReplyWithString(ctx, member);
        m_listDelNode(keys, node);

        /* score */
        node = listFirst(keys);
        scoretype *score = listNodeValue(node);
        sds score_str = mscore2String(score);
        RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
        m_sdsfree(score_str);
        m_listDelNode(keys, node);
    }

cleanup:
    m_listRelease(keys);
}

/* The exZunionInterDiffGenericCommand() function is called in order to implement the
 * following commands: EXZUNION, EXZINTER, EXZDIFF, EXZUNIONSTORE, EXZINTERSTORE, EXZDIFFSTORE.
 *
 * 'numkeysIndex' parameter position of key number. for EXZUNION/EXZINTER/EXZDIFF command,
 * this value is 1, for EXZUNIONSTORE/EXZINTERSTORE/EXZDIFFSTORE command, this value is 2.
 *
 * 'op' SET_OP_INTER, SET_OP_UNION or SET_OP_DIFF.
 */
void exZunionInterDiffGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisModuleKey *dstKey, int numkeysIndex, int op) {
    int i, j;
    long long setnum;
    int aggregate = AGGR_SUM;
    int scorenum = -1;
    zsetopsrc *src;
    zsetopval zval;
    RedisModuleString *tmp;
    scoretype *score;
    TairZsetObj *dstzobj;
    m_zskiplistNode *znode;
    int withscores = 0;

    if (RedisModule_StringToLongLong(argv[numkeysIndex], &setnum) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
        return;
    }

    if (setnum < 1) {
        RedisModule_ReplyWithError(ctx, "ERR at least 1 input key is needed");
        return;
    }

    /* test if the expected number of keys would overflow */
    if (setnum > argc - numkeysIndex - 1) {
        RedisModule_ReplyWithError(ctx, "ERR syntax error");
        return;
    }

    /* read keys to be used for input */
    src = RedisModule_Alloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = numkeysIndex + 1; i < setnum; i++, j++) {
        RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[j], REDISMODULE_READ);
        int type = RedisModule_KeyType(key);
        if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
            RedisModule_Free(src);
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            return;
        }
        TairZsetObj *tair_zset_obj = NULL;
        if (type != REDISMODULE_KEYTYPE_EMPTY) {
            tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
            if (scorenum == -1) {
                scorenum = tair_zset_obj->zsl->score_num;
            } else if (tair_zset_obj->zsl->score_num != scorenum) {
                RedisModule_Free(src);
                RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
                return;
            }
        }
        src[i].subject = tair_zset_obj;
        /* Default all weights to 1. */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    if (j < argc) {
        int remaining = argc - j;
        while (remaining) {
            /* OP_DIFF does not have optional extra arguments */
            if (op != SET_OP_DIFF &&
                remaining >= (setnum + 1) &&
                !mstringcasecmp(argv[j], "WEIGHTS")) {
                j++;
                remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (RedisModule_StringToDouble(argv[j], &src[i].weight) != REDISMODULE_OK) {
                        RedisModule_Free(src);
                        RedisModule_ReplyWithError(ctx, "ERR weight value is not a float");
                        return;
                    }
                }
            } else if (op != SET_OP_DIFF &&
                        remaining >= 2 &&
                        !mstringcasecmp(argv[j], "AGGREGATE")) {
                j++;
                remaining--;
                if (!mstringcasecmp(argv[j], "SUM")) {
                    aggregate = AGGR_SUM;
                } else if (!mstringcasecmp(argv[j], "MIN")) {
                    aggregate = AGGR_MIN;
                } else if (!mstringcasecmp(argv[j], "MAX")) {
                    aggregate = AGGR_MAX;
                } else {
                    RedisModule_Free(src);
                    RedisModule_ReplyWithError(ctx, "ERR syntax error");
                    return;
                }
                j++;
                remaining--;
            } else if (remaining >= 1 &&
                        !dstKey &&
                        !mstringcasecmp(argv[j], "WITHSCORES")) {

                j++;
                remaining--;
                withscores = 1;
            } else {
                RedisModule_Free(src);
                RedisModule_ReplyWithError(ctx, "ERR syntax error");
                return;
            }
        }
    }

    if (op != SET_OP_DIFF) {
        /* sort sets from the smallest to largest, this will improve our
        * algorithm's performance */
        qsort(src, setnum, sizeof(zsetopsrc), exZuidCompareByCardinality);
    }

    dstzobj = createTairZsetTypeObject(scorenum);
    memset(&zval, 0, sizeof(zsetopval));

    if (op == SET_OP_UNION) {
        dict *accumulator = m_dictCreate(&tairZsetDictType, NULL);
        m_dictIterator *di;
        m_dictEntry *de, *existing = NULL;
        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            m_dictExpand(accumulator, exZuidLength(&src[setnum - 1]));
        }
        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        for (i = 0; i < setnum; i++) {
            if (exZuidLength(&src[i]) == 0) continue;

            exZuidInitIterator(&src[i]);
            while (exZuidNext(&src[i], &zval)) {
                /* Initialize value */
                score = mnewScore(scorenum);
                mscoreMulWithWeight(score, zval.score, src[i].weight);

                /* Search for this element in the accumulating dictionary. */
                de = m_dictAddRaw(accumulator, zval.ele, &existing);
                /* If we don't have it, we need to create a new entry. */
                if (!existing) {
                    /* the first arg cannot be ctx, 
                     * or the memory will be managed by Redis, we can't let this happen. 
                     * If needed, it will be release by TairZsetTypeReleaseObject() later */
                    tmp = RedisModule_CreateStringFromString(NULL, zval.ele);
                    /* Update the element with its initial score. */
                    dictSetKey(accumulator, de, tmp);
                    dictSetVal(accumulator, de, score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set. */
                    exZunionInterAggregate(existing->v.val, score, aggregate);
                    RedisModule_Free(score);
                }
            }
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        di = m_dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        m_dictExpand(dstzobj->dict, dictSize(accumulator));

        /* We don't use exZsetAdd() because we don't need to call m_dictFind() */
        while((de = m_dictNext(di)) != NULL) {
            RedisModuleString *ele = dictGetKey(de);
            scoretype *score = dictGetVal(de);
            znode = m_zslInsert(dstzobj->zsl, score, ele);
            m_dictAdd(dstzobj->dict, ele, znode->score);
        }
        m_dictReleaseIterator(di);
        m_dictRelease(accumulator);
    } else if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        if (exZuidLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            exZuidInitIterator(&src[0]);
            scoretype *value = mnewScore(scorenum);    /* Temporary value for computation */
            while (exZuidNext(&src[0], &zval)) {
                /* Initialize value */
                score = mnewScore(scorenum);    /* Store in the zset */   
                mscoreMulWithWeight(score, zval.score, src[0].weight);
                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the tair zset we are
                     * iterating, so explicitly check for equal object. */
                    if (src[j].subject == src[0].subject) {
                        mscoreMulWithWeight(value, zval.score, src[j].weight);
                        exZunionInterAggregate(score, value, aggregate);
                    } else if (exZuidFind(&src[j], &zval, value)) {
                        mscoreMulWithWeight(value, value, src[j].weight);
                        exZunionInterAggregate(score, value, aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                if (j == setnum) {
                    tmp = RedisModule_CreateStringFromString(NULL, zval.ele);
                    znode = m_zslInsert(dstzobj->zsl, score, tmp);
                    m_dictAdd(dstzobj->dict, tmp, znode->score);
                } else {
                    RedisModule_Free(score);
                }
            }
            RedisModule_Free(value);
        }
    } else if (op == SET_OP_DIFF) {
        /* TODO */
    }

    if (dstKey) {
        unsigned long length = dstzobj->zsl->length;
        /* overwrite if dstkey already exists */
        if (length) {
            RedisModule_ModuleTypeSetValue(dstKey, TairZsetType, dstzobj);
        } else {
            RedisModule_DeleteKey(dstKey);
        }
        RedisModule_ReplyWithLongLong(ctx, length);
        RedisModule_ReplicateVerbatim(ctx);
    } else {
        unsigned long length = dstzobj->zsl->length;
        m_zskiplist *zsl = dstzobj->zsl;
        m_zskiplistNode *zn = zsl->header->level[0].forward;

        if (withscores)
            RedisModule_ReplyWithArray(ctx, length * 2);
        else
            RedisModule_ReplyWithArray(ctx, length);

        while (zn != NULL) {
            RedisModule_ReplyWithString(ctx, zn->ele);
            if (withscores) {
                sds score_str = mscore2String(zn->score);
                RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
                m_sdsfree(score_str);
            } 
            zn = zn->level[0].forward;
        }

        TairZsetTypeReleaseObject(dstzobj);
    }

    RedisModule_Free(src);
}

/* ========================= "tairzset" type commands =======================*/

/* EXZADD key [NX|XX] [CH] [INCR] score member [score member ...] */
int TairZsetTypeZadd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZaddGenericCommand(ctx, argv, argc, ZADD_NONE);
    return REDISMODULE_OK;
}

/* EXZINCRBY key increment member */
int TairZsetTypeZincrby_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZaddGenericCommand(ctx, argv, argc, ZADD_INCR);
    return REDISMODULE_OK;
}

/* EXZSCORE key member */
int TairZsetTypeZscore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    scoretype *score;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (exZsetScore(tair_zset_obj, argv[2], &score) == C_ERR) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        sds score_str = mscore2String(score);
        RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
        m_sdsfree(score_str);
    }
    return REDISMODULE_OK;
}

/* EXZRANGE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
int TairZsetTypeZrange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZrangeGenericCommand(ctx, argv, argc, 0);
    return REDISMODULE_OK;
}

/* EXZREVRANGE <key> <min> <max> [WITHSCORES] */
int TairZsetTypeZrevrange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZrangeGenericCommand(ctx, argv, argc, 1);
    return REDISMODULE_OK;
}

/* EXZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
int TairZsetTypeZrangebyscore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exGenericZrangebyscoreCommand(ctx, argv, argc, 0);
    return REDISMODULE_OK;
}

/* EXZREVRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
int TairZsetTypeZrevrangebyscore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exGenericZrangebyscoreCommand(ctx, argv, argc, 1);
    return REDISMODULE_OK;
}

/* EXZRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
int TairZsetTypeZrangebylex_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    exGenericZrangebylexCommand(ctx, argv, argc, 0);
    return REDISMODULE_OK;
}

/* EXZREVRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
int TairZsetTypeZrevrangebylex_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    exGenericZrangebylexCommand(ctx, argv, argc, 1);
    return REDISMODULE_OK;
}

/* EXZREM key member [member ...] */
int TairZsetTypeZrem_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    int deleted = 0, j;

    for (j = 2; j < argc; j++) {
        if (exZsetDel(tair_zset_obj, argv[j])) {
            deleted++;
        }

        if (exZsetLength(tair_zset_obj) == 0) {
            RedisModule_DeleteKey(key);
            break;
        }
    }

    if (deleted) {
        RedisModule_ReplicateVerbatim(ctx);
    }
    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* EXZREMRANGEBYSCORE key min max  */
int TairZsetTypeZremrangebyscore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZremrangeGenericCommand(ctx, argv, argc, ZRANGE_SCORE);
    return REDISMODULE_OK;
}

/* EXZREMRANGEBYRANK key start stop  */
int TairZsetTypeZremrangebyrank_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZremrangeGenericCommand(ctx, argv, argc, ZRANGE_RANK);
    return REDISMODULE_OK;
}

/* EXZREMRANGEBYLEX key min max  */
int TairZsetTypeZremrangebylex_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    exZremrangeGenericCommand(ctx, argv, argc, ZRANGE_LEX);
    return REDISMODULE_OK;
}

/* EXZCARD key */
int TairZsetTypeZcard_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithLongLong(ctx, exZsetLength(tair_zset_obj));
    return REDISMODULE_OK;
}

/* EXZRANK key member */
int TairZsetTypeZrank_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    exZrankGenericCommand(ctx, argv, argc, 0, 0);
    return REDISMODULE_OK;
}

/* EXZREVRANK key member */
int TairZsetTypeZrevrank_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    exZrankGenericCommand(ctx, argv, argc, 1, 0);
    return REDISMODULE_OK;
}

/* EXZRANKBYSCORE key score */
int TairZsetTypeZrankByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    exZrankGenericCommand(ctx, argv, argc, 0, 1);
    return REDISMODULE_OK;
}

/* EXZREVRANKBYSCORE key score */
int TairZsetTypeZrevrankByScore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    exZrankGenericCommand(ctx, argv, argc, 1, 1);
    return REDISMODULE_OK;
}

/* EXZCOUNT key min max */
int TairZsetTypeZcount_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    m_zrangespec range;
    range.max = NULL;
    range.min = NULL;
    unsigned long count = 0;

    if (m_zslParseRange(argv[2], argv[3], &range) != C_OK) {
        RedisModule_ReplyWithError(ctx, "ERR min or max is not a float");
        goto free_range;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        goto free_range;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        goto free_range;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (range.max->score_num != tair_zset_obj->zsl->score_num || range.min->score_num != tair_zset_obj->zsl->score_num) {
        RedisModule_ReplyWithError(ctx, "ERR score is not a valid format");
        goto free_range;
    }

    m_zskiplist *zsl = tair_zset_obj->zsl;
    m_zskiplistNode *zn;
    unsigned long rank;

    zn = m_zslFirstInRange(zsl, &range);

    if (zn != NULL) {
        rank = m_zslGetRank(zsl, zn->score, zn->ele);
        count = (zsl->length - (rank - 1));

        zn = m_zslLastInRange(zsl, &range);
        if (zn != NULL) {
            rank = m_zslGetRank(zsl, zn->score, zn->ele);
            count -= (zsl->length - rank);
        }
    }

    RedisModule_ReplyWithLongLong(ctx, count);

free_range:
    RedisModule_Free(range.max);
    RedisModule_Free(range.min);
    return REDISMODULE_OK;
}

/* EXZLEXCOUNT key min max */
int TairZsetTypeZlexcount_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    m_zlexrangespec range;
    unsigned long count = 0;

    if (m_zslParseLexRange(argv[2], argv[3], &range) != C_OK) {
        RedisModule_ReplyWithError(ctx, "ERR min or max not valid string range item");
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        m_zslFreeLexRange(&range);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        m_zslFreeLexRange(&range);
        return REDISMODULE_ERR;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    m_zskiplist *zsl = tair_zset_obj->zsl;
    m_zskiplistNode *zn;
    unsigned long rank;

    zn = m_zslFirstInLexRange(zsl, &range);

    if (zn != NULL) {
        rank = m_zslGetRank(zsl, zn->score, zn->ele);
        count = (zsl->length - (rank - 1));

        zn = m_zslLastInLexRange(zsl, &range);
        if (zn != NULL) {
            rank = m_zslGetRank(zsl, zn->score, zn->ele);
            count -= (zsl->length - rank);
        }
    }

    m_zslFreeLexRange(&range);
    RedisModule_ReplyWithLongLong(ctx, count);
    return REDISMODULE_OK;
}

/* EXZMSCORE key member [member ...] */
int TairZsetTypeZmscore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, argc - 2);

    scoretype *score;
    for (int j = 2; j < argc; j++) {
        if (tair_zset_obj == NULL || exZsetScore(tair_zset_obj, argv[j], &score) == C_ERR) {
            RedisModule_ReplyWithNull(ctx);
        } else {
            sds score_str = mscore2String(score);
            RedisModule_ReplyWithStringBuffer(ctx, score_str, sdslen(score_str));
            m_sdsfree(score_str);
        }
    }
    return REDISMODULE_OK;
}

/* EXZRANDMEMBER key [count [WITHSCORES]] */
int TairZsetTypeZrandmember_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_OK;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    long l;
    int withscores = 0;
    if (argc >= 3) {
        if ((RedisModule_StringToLongLong(argv[2], (long long *)&l) != REDISMODULE_OK)) {
            RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
            return REDISMODULE_OK;
        }
        if (argc > 4 || (argc == 4 && mstringcasecmp(argv[3], "withscores"))) {
            RedisModule_ReplyWithError(ctx, "ERR syntax error");
            return REDISMODULE_OK;
        } else if (argc == 4) {
            withscores = 1;
        }
        if (tair_zset_obj == NULL) {
            /* RedisModule_ReplyWithEmptyArray is not supported in Redis 5.0 */
            RedisModule_ReplyWithArray(ctx, 0);
            return REDISMODULE_OK;
        }
        exZrandMemberWithCountCommand(ctx, tair_zset_obj, l, withscores);
        return REDISMODULE_OK;
    }

    /* Handle variant without <count> argument (Only <key> argument). Reply with simple bulk string */
    if (tair_zset_obj == NULL) {
        RedisModule_ReplyWithNull(ctx);
        return REDISMODULE_OK;
    }
    RedisModuleString *ele;
    exZsetRandomElement(tair_zset_obj, &ele, NULL);
    RedisModule_ReplyWithString(ctx, ele);
    return REDISMODULE_OK;
}

/* EXZSCAN key cursor [MATCH pattern] [COUNT count] */
int TairZsetTypeZscan_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }    
    /* We need an *unsigned* long, 
     * and RedisModule_StringToLongLong() does not cover the whole cursor space. 
     * So we use RedisModule_StringPtrLen() to get the cursor string first,
     * and use strtoul() to get the unsigned long cursor */
    size_t cursor_str_len;
    unsigned long cursor;
    const char *cursor_str = RedisModule_StringPtrLen(argv[2], &cursor_str_len);
    errno = 0;
    char *eptr;
    cursor = strtoul(cursor_str, &eptr, 10);
    if (isspace(cursor_str[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
        RedisModule_ReplyWithError(ctx, "ERR invalid cursor");
        return REDISMODULE_OK;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairZsetType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    TairZsetObj *tair_zset_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithCString(ctx, "0");
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_zset_obj = RedisModule_ModuleTypeGetValue(key);
    }

    exZscanGernericCommand(ctx, argv, argc, tair_zset_obj, cursor);

    return REDISMODULE_OK;
}

/* EXZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] */
int TairZsetTypeZunionstore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    exZunionInterDiffGenericCommand(ctx, argv, argc, key, 2, SET_OP_UNION);
    return REDISMODULE_OK;
}

/* EXZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES] */
int TairZsetTypeZunion_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    exZunionInterDiffGenericCommand(ctx, argv, argc, NULL, 1, SET_OP_UNION);
    return REDISMODULE_OK;
}

/* EXZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] */
int TairZsetTypeZinterstore_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);

    exZunionInterDiffGenericCommand(ctx, argv, argc, key, 2, SET_OP_INTER);
    return REDISMODULE_OK;
}

/* EXZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES] */
int TairZsetTypeZinter_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    exZunionInterDiffGenericCommand(ctx, argv, argc, NULL, 1, SET_OP_INTER);
    return REDISMODULE_OK;
}

/* ========================== "exstrtype" type methods =======================*/
void *TairZsetTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    REDISMODULE_NOT_USED(encver);

    size_t i, score_num;
    unsigned long length;

    length = RedisModule_LoadUnsigned(rdb);
    score_num = RedisModule_LoadUnsigned(rdb);

    TairZsetObj *o = createTairZsetTypeObject(score_num);

    while (length--) {
        RedisModuleString *ele;
        scoretype *score = RedisModule_Calloc(1, sizeof(scoretype) + sizeof(double) * score_num);
        score->score_num = score_num;

        ele = RedisModule_LoadString(rdb);

        for (i = 0; i < score_num; i++) {
            score->scores[i] = RedisModule_LoadDouble(rdb);
        }

        m_zslInsert(o->zsl, score, ele);
        m_dictAdd(o->dict, ele, score);
    }

    return o;
}

void TairZsetTypeRdbSave(RedisModuleIO *rdb, void *value) {
    TairZsetObj *o = (TairZsetObj *)value;
    m_zskiplist *zsl = o->zsl;
    size_t i, score_num;

    score_num = zsl->score_num;

    RedisModule_SaveUnsigned(rdb, zsl->length);
    RedisModule_SaveUnsigned(rdb, score_num);

    m_zskiplistNode *zn = zsl->tail;
    while (zn != NULL) {
        RedisModule_SaveString(rdb, zn->ele);
        for (i = 0; i < score_num; i++) {
            RedisModule_SaveDouble(rdb, zn->score->scores[i]);
        }

        zn = zn->backward;
    }
}

#define AOF_REWRITE_ITEMS_PER_CMD 64
void TairZsetTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    TairZsetObj *o = (TairZsetObj *)value;

    long long count = 0, i;

    size_t array_size = 0;

    RedisModuleString **string_array = RedisModule_Calloc(AOF_REWRITE_ITEMS_PER_CMD * 2, sizeof(RedisModuleString *));

    m_dictIterator *di = m_dictGetIterator(o->dict);
    m_dictEntry *de;
    while ((de = m_dictNext(di)) != NULL) {
        RedisModuleString *ele = dictGetKey(de);
        scoretype *score = (scoretype *)dictGetVal(de);
        sds score_str = mscore2String(score);
        string_array[array_size++] = RedisModule_CreateString(NULL, score_str, sdslen(score_str));
        m_sdsfree(score_str);
        string_array[array_size++] = RedisModule_CreateStringFromString(NULL, ele);

        if (++count == AOF_REWRITE_ITEMS_PER_CMD) {
            RedisModule_EmitAOF(aof, "EXZADD", "sv", key, string_array, array_size);
            count = 0;
            for (i = 0; i < array_size; i++) {
                RedisModule_FreeString(NULL, string_array[i]);
                string_array[i] = NULL;
            }
            array_size = 0;
        }
    }
    m_dictReleaseIterator(di);

    if (array_size) {
        RedisModule_EmitAOF(aof, "EXZADD", "sv", key, string_array, array_size);
        for (i = 0; i < array_size; i++) {
            RedisModule_FreeString(NULL, string_array[i]);
            string_array[i] = NULL;
        }
    }

    RedisModule_Free(string_array);
}

size_t TairZsetTypeMemUsage(const void *value) {
    TairZsetObj *o = (TairZsetObj *)value;

    size_t asize = 0, elesize;

    m_zskiplist *zsl = o->zsl;
    m_zskiplistNode *znode = zsl->header->level[0].forward;

    asize = sizeof(*o) + sizeof(m_zskiplist) + sizeof(dict) + (sizeof(struct m_dictEntry *) * dictSlots(o->dict));

    while (znode != NULL) {
        RedisModule_StringPtrLen(znode->ele, &elesize);
        asize += sizeof(*znode) + elesize + znode->score->score_num * sizeof(double);
        znode = znode->level[0].forward;
    }

    return asize;
}

void TairZsetTypeFree(void *value) { TairZsetTypeReleaseObject(value); }

void TairZsetTypeDigest(RedisModuleDigest *md, void *value) {
    TairZsetObj *o = (TairZsetObj *)value;

    m_dictIterator *di = m_dictGetIterator(o->dict);
    m_dictEntry *de;

    size_t elelen = 0;
    RedisModuleString *ele;
    scoretype *score;

    while ((de = m_dictNext(di)) != NULL) {
        ele = dictGetKey(de);
        score = dictGetVal(de);
        const char *eleptr = RedisModule_StringPtrLen(ele, &elelen);
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)eleptr, elelen);
        sds score_str = mscore2String(score);
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)score_str, sdslen(score_str));
        m_sdsfree(score_str);
        RedisModule_DigestEndSequence(md);
    }

    m_dictReleaseIterator(di);
}

size_t TairZsetTypeFreeEffort(RedisModuleString *key, const void *value) {
    REDISMODULE_NOT_USED(key);
    TairZsetObj *o = (TairZsetObj *)value;
    return exZsetLength(o); 
}

int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr)                                                       \
    do {                                                                                  \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, 1, 1, 1) != REDISMODULE_OK) { \
            return REDISMODULE_ERR;                                                       \
        }                                                                                 \
    } while (0);

#define CREATE_WRCMD(name, tgt) CREATE_CMD(name, tgt, "write deny-oom")
#define CREATE_ROCMD(name, tgt) CREATE_CMD(name, tgt, "readonly fast")

    /* write cmd */
    CREATE_WRCMD("exzadd", TairZsetTypeZadd_RedisCommand)
    CREATE_WRCMD("exzincrby", TairZsetTypeZincrby_RedisCommand)
    CREATE_WRCMD("exzrem", TairZsetTypeZrem_RedisCommand)
    CREATE_WRCMD("exzremrangebyscore", TairZsetTypeZremrangebyscore_RedisCommand)
    CREATE_WRCMD("exzremrangebyrank", TairZsetTypeZremrangebyrank_RedisCommand)
    CREATE_WRCMD("exzremrangebylex", TairZsetTypeZremrangebylex_RedisCommand)
    CREATE_WRCMD("exzunionstore", TairZsetTypeZunionstore_RedisCommand)
    CREATE_WRCMD("exzinterstore", TairZsetTypeZinterstore_RedisCommand)

    /* read cmd */
    CREATE_ROCMD("exzscore", TairZsetTypeZscore_RedisCommand)
    CREATE_ROCMD("exzrange", TairZsetTypeZrange_RedisCommand)
    CREATE_ROCMD("exzrevrange", TairZsetTypeZrevrange_RedisCommand)
    CREATE_ROCMD("exzrangebyscore", TairZsetTypeZrangebyscore_RedisCommand)
    CREATE_ROCMD("exzrevrangebyscore", TairZsetTypeZrevrangebyscore_RedisCommand)
    CREATE_ROCMD("exzrangebylex", TairZsetTypeZrangebylex_RedisCommand)
    CREATE_ROCMD("exzrevrangebylex", TairZsetTypeZrevrangebylex_RedisCommand)
    CREATE_ROCMD("exzcard", TairZsetTypeZcard_RedisCommand)
    CREATE_ROCMD("exzrank", TairZsetTypeZrank_RedisCommand)
    CREATE_ROCMD("exzrevrank", TairZsetTypeZrevrank_RedisCommand)
    CREATE_ROCMD("exzrankbyscore", TairZsetTypeZrankByScore_RedisCommand)
    CREATE_ROCMD("exzrevrankbyscore", TairZsetTypeZrevrankByScore_RedisCommand)
    CREATE_ROCMD("exzcount", TairZsetTypeZcount_RedisCommand)
    CREATE_ROCMD("exzlexcount", TairZsetTypeZlexcount_RedisCommand)
    CREATE_ROCMD("exzmscore", TairZsetTypeZmscore_RedisCommand)
    CREATE_ROCMD("exzrandmember", TairZsetTypeZrandmember_RedisCommand)
    CREATE_ROCMD("exzscan", TairZsetTypeZscan_RedisCommand)
    CREATE_ROCMD("exzunion", TairZsetTypeZunion_RedisCommand)
    CREATE_ROCMD("exzinter", TairZsetTypeZinter_RedisCommand)

    return REDISMODULE_OK;
}

int __attribute__ ((visibility ("default"))) RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "tairzset", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    shared_minstring = RedisModule_CreateString(ctx, "minstring", strlen("minstring"));
    shared_maxstring = RedisModule_CreateString(ctx, "maxstring", strlen("maxstring"));

    RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                                 .rdb_load = TairZsetTypeRdbLoad,
                                 .rdb_save = TairZsetTypeRdbSave,
                                 .aof_rewrite = TairZsetTypeAofRewrite,
                                 .mem_usage = TairZsetTypeMemUsage,
                                 .free = TairZsetTypeFree,
                                 .digest = TairZsetTypeDigest,
                                 .free_effort = TairZsetTypeFreeEffort};

    TairZsetType = RedisModule_CreateDataType(ctx, "tairzset_", TAIRZSET_ENCVER_VER_1, &tm);
    if (TairZsetType == NULL) {
        return REDISMODULE_ERR;
    }

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
