#pragma once

#include "../src/redismodule.h"
#include "sds.h"
#include "dict.h"

#define ZSKIPLIST_MAXLEVEL 64 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1 << 0) /* Increment the score instead of setting it. */
#define ZADD_NX (1 << 1)   /* Don't touch elements not already existing. */
#define ZADD_XX (1 << 2)   /* Only touch elements already existing. */

/* Output flags. */
#define ZADD_NOP (1 << 3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1 << 4)     /* Only touch elements already existing. */
#define ZADD_ADDED (1 << 5)   /* The element was new and was added. */
#define ZADD_UPDATED (1 << 6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1 << 16) /* Return num of elements added or updated. */

#define SCORE_DELIMITER '#'
#define MAX_SCORE_NUM 255
typedef struct scoretype {
    unsigned char score_num;
    double scores[0];
} scoretype;
typedef struct m_zskiplistNode {
    RedisModuleString *ele;
    scoretype *score;
    struct m_zskiplistNode *backward;
    struct zskiplistLevel {
        struct m_zskiplistNode *forward;
        unsigned long span;
    } level[];
} m_zskiplistNode;

typedef struct m_zskiplist {
    struct m_zskiplistNode *header, *tail;
    unsigned long length;
    int level;
    size_t score_num;  // schema
} m_zskiplist;

typedef struct {
    scoretype *min, *max;
    int minex, maxex; /* are min or max exclusive? */
} m_zrangespec;

typedef struct {
    RedisModuleString *min, *max; /* May be set to shared.(minstring|maxstring) */
    int minex, maxex;             /* are min or max exclusive? */
} m_zlexrangespec;

extern RedisModuleString *shared_minstring;
extern RedisModuleString *shared_maxstring;

m_zskiplist *m_zslCreate(unsigned char score_num);
void m_zslFree(m_zskiplist *zsl);
m_zskiplistNode *m_zslInsert(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele);
unsigned char *m_zzlInsert(unsigned char *zl, RedisModuleString *ele, scoretype *score);
int m_zslDelete(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele, m_zskiplistNode **node);
unsigned long m_zslGetRank(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele);
unsigned long m_zslGetRankByScore(m_zskiplist *zsl, scoretype *score);
m_zskiplistNode *m_zslUpdateScore(m_zskiplist *zsl, scoretype *curscore, RedisModuleString *ele, scoretype *newscore);
m_zskiplistNode *m_zslGetElementByRank(m_zskiplist *zsl, unsigned long rank);
int m_zslParseRange(RedisModuleString *min, RedisModuleString *max, m_zrangespec *spec);
void m_zslFreeLexRange(m_zlexrangespec *spec);
int m_zslParseLexRange(RedisModuleString *min, RedisModuleString *max, m_zlexrangespec *spec);
int m_zslValueGteMin(scoretype *value, m_zrangespec *spec);
int m_zslValueLteMax(scoretype *value, m_zrangespec *spec);
int m_zslIsInRange(m_zskiplist *zsl, m_zrangespec *range);
m_zskiplistNode *m_zslFirstInRange(m_zskiplist *zsl, m_zrangespec *range);
m_zskiplistNode *m_zslLastInRange(m_zskiplist *zsl, m_zrangespec *range);
int m_zslLexValueLteMax(RedisModuleString *value, m_zlexrangespec *spec);
int m_zslLexValueGteMin(RedisModuleString *value, m_zlexrangespec *spec);
m_zskiplistNode *m_zslLastInLexRange(m_zskiplist *zsl, m_zlexrangespec *range);
m_zskiplistNode *m_zslFirstInLexRange(m_zskiplist *zsl, m_zlexrangespec *range);
unsigned long m_zslDeleteRangeByScore(m_zskiplist *zsl, m_zrangespec *range, dict *dict);
unsigned long m_zslDeleteRangeByRank(m_zskiplist *zsl, unsigned int start, unsigned int end, dict *dict);
unsigned long m_zslDeleteRangeByLex(m_zskiplist *zsl, m_zlexrangespec *range, dict *dict);

int mscoreGetNum(const char *s, size_t slen);
int mscoreParse(const char *s, size_t slen, scoretype **score);
int mscoreCmp(scoretype *s1, scoretype *s2);
sds mscore2String(scoretype *score);
int mscoreAdd(scoretype *s1, scoretype *s2);