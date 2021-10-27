#include "skiplist.h"

#include <assert.h>
#include <math.h>
#include <stdlib.h>

#include "util.h"

static inline void *rm_malloc(size_t n) {
    return RedisModule_Alloc(n);
}

static inline void *rm_calloc(size_t nelem, size_t elemsz) {
    return RedisModule_Calloc(nelem, elemsz);
}

static inline void rm_free(void *p) {
    RedisModule_Free(p);
}

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
m_zskiplistNode *m_zslCreateNode(int level, scoretype *score, RedisModuleString *ele) {
    m_zskiplistNode *zn = rm_malloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
m_zskiplist *m_zslCreate(unsigned char score_num) {
    int j;
    m_zskiplist *zsl;

    zsl = rm_malloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    scoretype *score = rm_calloc(1, sizeof(scoretype) + score_num * sizeof(double));
    score->score_num = score_num;
    zsl->header = m_zslCreateNode(ZSKIPLIST_MAXLEVEL, score, NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    zsl->score_num = score_num;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
void m_zslFreeNode(m_zskiplistNode *node) {
    if (node->ele) {
        RedisModule_FreeString(NULL, node->ele);
    }
    rm_free(node->score);
    rm_free(node);
}

/* Free a whole skiplist. */
void m_zslFree(m_zskiplist *zsl) {
    m_zskiplistNode *node = zsl->header->level[0].forward, *next;

    rm_free(zsl->header->ele);
    rm_free(zsl->header->score);
    rm_free(zsl->header);
    while (node) {
        next = node->level[0].forward;
        m_zslFreeNode(node);
        node = next;
    }
    rm_free(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int m_zslRandomLevel(void) {
    int level = 1;
    while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
m_zskiplistNode *m_zslInsert(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];
        while (x->level[i].forward && 
                (mscoreCmp(x->level[i].forward->score, score) < 0 || 
                (mscoreCmp(x->level[i].forward->score, score) == 0 && 
                RedisModule_StringCompare(x->level[i].forward->ele, ele) < 0))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of m_zslInsert() should test in the hash table if the element is
     * already inside or not. */
    level = m_zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }
    x = m_zslCreateNode(level, score, ele);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    zsl->length++;
    return x;
}

/* Internal function used by m_zslDelete, zslDeleteByScore and zslDeleteByRank */
void m_zslDeleteNode(m_zskiplist *zsl, m_zskiplistNode *x, m_zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }
    while (zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by m_zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
int m_zslDelete(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele, m_zskiplistNode **node) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
                (mscoreCmp(x->level[i].forward->score, score) < 0 || 
                (mscoreCmp(x->level[i].forward->score, score) == 0 && 
                RedisModule_StringCompare(x->level[i].forward->ele, ele) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && mscoreCmp(score, x->score) == 0 && RedisModule_StringCompare(x->ele, ele) == 0) {
        m_zslDeleteNode(zsl, x, update);
        if (!node)
            m_zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

/* Update the score of an elmenent inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */
m_zskiplistNode *m_zslUpdateScore(m_zskiplist *zsl, scoretype *curscore, RedisModuleString *ele, scoretype *newscore) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
        (mscoreCmp(x->level[i].forward->score, curscore) < 0 || 
        (mscoreCmp(x->level[i].forward->score, curscore) == 0 && RedisModule_StringCompare(x->level[i].forward->ele, ele) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    x = x->level[0].forward;
    assert(x && mscoreCmp(curscore, x->score) == 0 && RedisModule_StringCompare(x->ele, ele) == 0);
    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */
    if ((x->backward == NULL || mscoreCmp(x->backward->score, newscore) < 0) && (x->level[0].forward == NULL || mscoreCmp(x->level[0].forward->score, newscore) > 0)) {
        rm_free(x->score);
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    m_zslDeleteNode(zsl, x, update);
    m_zskiplistNode *newnode = m_zslInsert(zsl, newscore, x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since m_zslInsert created a new one. */
    x->ele = NULL;
    m_zslFreeNode(x);
    return newnode;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
unsigned long m_zslDeleteRangeByRank(m_zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        m_zskiplistNode *next = x->level[0].forward;
        m_zslDeleteNode(zsl, x, update);
        m_dictDelete(dict, x->ele);
        m_zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank by score.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 0-based due to the span of zsl->header to the first element. */
unsigned long m_zslGetRankByScore(m_zskiplist *zsl, scoretype *score) {
    m_zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (mscoreCmp(x->level[i].forward->score, score) < 0)) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
    }
    return rank;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
unsigned long m_zslGetRank(m_zskiplist *zsl, scoretype *score, RedisModuleString *ele) {
    m_zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
             (mscoreCmp(x->level[i].forward->score, score) < 0 || 
             (mscoreCmp(x->level[i].forward->score, score) == 0 && 
             RedisModule_StringCompare(x->level[i].forward->ele, ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        if (x->ele && RedisModule_StringCompare(x->ele, ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
m_zskiplistNode *m_zslGetElementByRank(m_zskiplist *zsl, unsigned long rank) {
    m_zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
int m_zslParseRange(RedisModuleString *min, RedisModuleString *max, m_zrangespec *spec) {
    spec->minex = spec->maxex = 0;

    size_t max_len, min_len;
    const char *min_ptr = RedisModule_StringPtrLen(min, &min_len);
    const char *max_ptr = RedisModule_StringPtrLen(max, &max_len);
    int ret;
    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */

    if (min_ptr[0] == '(') {
        ret = mscoreParse(min_ptr + 1, min_len - 1, &spec->min);
        if (ret <= 0) return C_ERR;
        spec->minex = 1;
    } else {
        ret = mscoreParse(min_ptr, min_len, &spec->min);
        if (ret <= 0) return C_ERR;
    }

    if (max_ptr[0] == '(') {
        ret = mscoreParse(max_ptr + 1, max_len - 1, &spec->max);
        if (ret <= 0) return C_ERR;
        spec->maxex = 1;
    } else {
        ret = mscoreParse(max_ptr, max_len, &spec->max);
        if (ret <= 0) return C_ERR;
    }
    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */
int m_zslParseLexRangeItem(RedisModuleString *item, RedisModuleString **dest, int *ex) {
    size_t len;
    const char *c = RedisModule_StringPtrLen(item, &len);

    switch (c[0]) {
        case '+':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared_maxstring;
            return C_OK;
        case '-':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared_minstring;
            return C_OK;
        case '(':
            *ex = 1;
            *dest = RedisModule_CreateString(NULL, c + 1, len - 1);
            return C_OK;
        case '[':
            *ex = 0;
            *dest = RedisModule_CreateString(NULL, c + 1, len - 1);
            return C_OK;
        default:
            return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (C_OK returned). */
void m_zslFreeLexRange(m_zlexrangespec *spec) {
    if (spec->min && spec->min != shared_minstring && spec->min != shared_maxstring) {
        RedisModule_FreeString(NULL, spec->min);
    }
    if (spec->max && spec->max != shared_minstring && spec->max != shared_maxstring) {
        RedisModule_FreeString(NULL, spec->max);
    }
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with m_zslFreeLexRange(),
 * otherwise no release is needed. */
int m_zslParseLexRange(RedisModuleString *min, RedisModuleString *max, m_zlexrangespec *spec) {
    spec->min = spec->max = NULL;
    if (m_zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR || m_zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        m_zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to m_sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
int m_mscmplex(RedisModuleString *a, RedisModuleString *b) {
    if (a == b) return 0;
    if (a == shared_minstring || b == shared_maxstring) return -1;
    if (a == shared_maxstring || b == shared_minstring) return 1;
    return RedisModule_StringCompare(a, b);
}

int m_zslLexValueGteMin(RedisModuleString *value, m_zlexrangespec *spec) {
    return spec->minex ? (m_mscmplex(value, spec->min) > 0) : (m_mscmplex(value, spec->min) >= 0);
}

int m_zslLexValueLteMax(RedisModuleString *value, m_zlexrangespec *spec) {
    return spec->maxex ? (m_mscmplex(value, spec->max) < 0) : (m_mscmplex(value, spec->max) <= 0);
}

int m_zslValueGteMin(scoretype *value, m_zrangespec *spec) {
    return spec->minex ? (mscoreCmp(value, spec->min) > 0) : (mscoreCmp(value, spec->min) >= 0);
}

int m_zslValueLteMax(scoretype *value, m_zrangespec *spec) {
    return spec->maxex ? (mscoreCmp(value, spec->max) < 0) : (mscoreCmp(value, spec->max) <= 0);
}

/* Returns if there is a part of the zset is in range. */
int m_zslIsInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (mscoreCmp(range->min, range->max) > 0 || (mscoreCmp(range->min, range->max) == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !m_zslValueGteMin(x->score, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !m_zslValueLteMax(x->score, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslFirstInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!m_zslIsInRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward && !m_zslValueGteMin(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    assert(x != NULL);

    /* Check if score <= max. */
    if (!m_zslValueLteMax(x->score, range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslLastInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!m_zslIsInRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward && m_zslValueLteMax(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    assert(x != NULL);

    /* Check if score >= min. */
    if (!m_zslValueGteMin(x->score, range)) return NULL;
    return x;
}

unsigned long m_zslDeleteRangeByScore(m_zskiplist *zsl, m_zrangespec *range, dict *dict) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ? mscoreCmp(x->level[i].forward->score, range->min) <= 0 : mscoreCmp(x->level[i].forward->score, range->min) < 0))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && (range->maxex ? mscoreCmp(x->score, range->max) < 0 : mscoreCmp(x->score, range->max) <= 0)) {
        m_zskiplistNode *next = x->level[0].forward;
        m_zslDeleteNode(zsl, x, update);
        m_dictDelete(dict, x->ele);
        m_zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

unsigned long m_zslDeleteRangeByLex(m_zskiplist *zsl, m_zlexrangespec *range, dict *dict) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && !m_zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && m_zslLexValueLteMax(x->ele, range)) {
        m_zskiplistNode *next = x->level[0].forward;
        m_zslDeleteNode(zsl, x, update);
        m_dictDelete(dict, x->ele);
        m_zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Returns if there is a part of the zset is in the lex range. */
int m_zslIsInLexRange(m_zskiplist *zsl, m_zlexrangespec *range) {
    m_zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    int cmp = m_mscmplex(range->min, range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !m_zslLexValueGteMin(x->ele, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !m_zslLexValueLteMax(x->ele, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslFirstInLexRange(m_zskiplist *zsl, m_zlexrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!m_zslIsInLexRange(zsl, range)) {
        return NULL;
    }

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward && !m_zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    assert(x != NULL);

    /* Check if score <= max. */
    if (!m_zslLexValueLteMax(x->ele, range)) {
        return NULL;
    }
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslLastInLexRange(m_zskiplist *zsl, m_zlexrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!m_zslIsInLexRange(zsl, range)) {
        return NULL;
    }

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward && m_zslLexValueLteMax(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    assert(x != NULL);

    /* Check if score >= min. */
    if (!m_zslLexValueGteMin(x->ele, range)) {
        return NULL;
    }
    return x;
}

/* ------------------------ multi scores api ---------------------------- */

int mscoreGetNum(const char *s, size_t slen) {
    int score_num = 0;

    if (slen == 0) {
        return -1;
    }

    if (s[0] == SCORE_DELIMITER || s[slen - 1] == SCORE_DELIMITER) {
        return -1;
    }

    const char *iter = s;
    while (iter <= s + slen) {
        if (*iter == SCORE_DELIMITER || iter == s + slen) {
            score_num++;
            iter++;
            continue;
        }
        iter++;
    }

    return score_num;
}

int mscoreParse(const char *s, size_t slen, scoretype **score) {
    const char *start = s;
    int len = 0, i = 0;
    double tmp_score;
    int score_num = mscoreGetNum(s, slen);
    if (score_num <= 0 || score_num > MAX_SCORE_NUM) {
        return -1;
    }

    scoretype *multi_score = (scoretype *)RedisModule_Calloc(1, sizeof(scoretype) + score_num * sizeof(double));
    multi_score->score_num = score_num;
    const char *iter = s;
    while (iter <= s + slen) {
        if (*iter == SCORE_DELIMITER || iter == s + slen) {
            if (m_string2d(start, len, &tmp_score) != 1) {
                goto fail;
            }

            if (isnan(tmp_score)) {
                goto fail;
            }

            multi_score->scores[i++] = tmp_score;
            start = iter + 1;
            len = 0;
            iter++;
            continue;
        }
        len++;
        iter++;
    }

    *score = multi_score;
    return score_num;
fail:
    RedisModule_Free(multi_score);
    return -1;
}

inline int mscoreCmp(scoretype *s1, scoretype *s2) {
    assert(s1 != NULL);
    assert(s2 != NULL);
    assert(s1->score_num == s1->score_num);

    int num = s1->score_num, i;

    double *s1_p = (double *)s1->scores;
    double *s2_p = (double *)s2->scores;

    for (i = 0; i < num; i++) {
        if (s1_p[i] != s2_p[i]) {
            return s1_p[i] < s2_p[i] ? -1 : 1;
        }
    }

    return 0;
}

sds mscore2String(scoretype *score) {
    assert(score != NULL);
    char scorebuf[128];
    int scorelen;

    sds score_str = m_sdsempty();

    int i = 0;
    for (; i < score->score_num; i++) {
        scorelen = m_d2string(scorebuf, sizeof(scorebuf), score->scores[i]);
        score_str = m_sdscatlen(score_str, scorebuf, scorelen);
        if (i < score->score_num - 1) {
            score_str = m_sdscatlen(score_str, "#", 1);
        }
    }

    return score_str;
}

int mscoreAdd(scoretype *s1, scoretype *s2) {
    assert(s1 != NULL);
    assert(s2 != NULL);
    assert(s1->score_num == s2->score_num);

    int i = 0;
    for (; i < s1->score_num; i++) {
        s1->scores[i] += s2->scores[i];
        if (isnan(s1->scores[i])) {
            return -1;
        }
    }

    return 0;
}