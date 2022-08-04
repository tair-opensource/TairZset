## Command introduction

### EXZADD
#### Grammar and complexity：
> EXZADD key [NX|XX] [CH] [INCR] score member [score member ...]    
> time complexity：O(N)

#### Command Description:
Adds all the specified members with the specified (multi)scores to the tairzset stored at key. It is possible to specify multiple score / member pairs. If a specified member is already a member of the tairzset, the score is updated and the element reinserted at the right position to ensure the correct ordering.

If key does not exist, a new tairzset with the specified members as sole members is created, like if the tairzset was empty. If the key exists but does not hold a tairzset, an error is returned.

The format of score must be `score1#score2#score3#...`, and the score format of all members in a tairzset must be the same. each score values should be the string representation of a double precision floating point number. +inf and -inf values are valid values as well. 

#### Options:
EXZADD supports a list of options, specified after the name of the key and before the first score argument. Options are:

XX: Only update elements that already exist. Don't add new elements.
NX: Only add new elements. Don't update already existing elements.
CH: Modify the return value from the number of new elements added, to the total number of elements changed (CH is an abbreviation of changed). Changed elements are new elements added and elements already existing for which the score was updated. So elements specified in the command line having the same score as they had in the past are not counted. Note: normally the return value of EXZADD only counts the number of new elements added.
INCR: When this option is specified EXZADD acts like EXZINCRBY. Only one score-element pair can be specified in this mode.

#### Return value
Integer reply, specifically:

When used without optional arguments, the number of elements added to the tairzset (excluding score updates).
If the CH option is specified, the number of elements that were changed (added or updated).
If the INCR option is specified, the return value will be Bulk string reply:

The new score of member represented as string (`score1#score2#score3#...`), or nil if the operation was aborted (when called with either the XX or the NX option).

### EXZINCRBY
#### Grammar and complexity：
> EXZINCRBY key increment member    
> time complexity：O(log(N)) 

#### Command Description:

Increments the score of member in the tairzset stored at key by increment. If member does not exist in the tairzset, it is added with increment as its score (as if its previous score was 0.0). If key does not exist, a new tairzset with the specified member as its sole member is created.

An error is returned when key exists but does not hold a tairzset.

The score value should be the string representation of a numeric value, and accepts double precision floating point numbers. It is possible to provide a negative value to decrement the score.
#### Return value
Bulk string reply: the new score of member (`score1#score2#score3#...`), represented as string.
### EXZSCORE
#### Grammar and complexity：
> EXZSCORE key member   
> time complexity：O(1)

#### Command Description:
Returns the score of member in the tairzset at key.

If member does not exist in the tairzset, or key does not exist, nil is returned.
#### Return value
Bulk string reply: the score of member (a double precision floating point number), represented as string.
### EXZRANGE
#### Grammar and complexity：
> EXZRANGE <key> <min> <max> [WITHSCORES]
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements returned.

#### Command Description:
Returns the specified range of elements in the tairzset stored at <key>.

#### Options:
The order of elements is from the lowest to the highest score. Elements with the same score are ordered lexicographically.

The optional REV argument reverses the ordering, so elements are ordered from highest to lowest score, and score ties are resolved by reverse lexicographical ordering.

The optional WITHSCORES argument supplements the command's reply with the scores of elements returned. The returned list contains value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client libraries are free to return a more appropriate data type (suggestion: an array with (value, score) arrays/tuples).

#### Index ranges
By default, the command performs an index range query. The <min> and <max> arguments represent zero-based indexes, where 0 is the first element, 1 is the next element, and so on. These arguments specify an inclusive range, so for example, EXZRANGE myzset 0 1 will return both the first and the second element of the tairzset.

The indexes can also be negative numbers indicating offsets from the end of the tairzset, with -1 being the last element of the tairzset, -2 the penultimate element, and so on.

Out of range indexes do not produce an error.

If <min> is greater than either the end index of the tairzset or <max>, an empty list is returned.

If <max> is greater than the end index of the tairzset, Redis will use the last element of the tairzset.
#### Return value
Array reply: list of elements in the specified range (optionally with their scores, in case the WITHSCORES option is given).
### EXZREVRANGE
#### Grammar and complexity：
> EXZREVRANGE <key> <min> <max> [WITHSCORES]  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements returned.
#### Command Description:
Returns the specified range of elements in the tairzset stored at key. The elements are considered to be ordered from the highest to the lowest score. Descending lexicographical order is used for elements with equal score.

Apart from the reversed ordering, EXZREVRANGE is similar to EXZRANGE.
#### Return value
Array reply: list of elements in the specified range (optionally with their scores).
### EXZRANGEBYSCORE
#### Grammar and complexity：
> EXZRANGEBYSCORE <key> <min> <max> [WITHSCORES]  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
#### Command Description:
Returns all the elements in the tairzset at key with a score between min and max (including elements with score equal to min or max). The elements are considered to be ordered from low to high scores.

The elements having the same score are returned in lexicographical order (this follows from a property of the tairzset implementation in Redis and does not involve further computation).

The optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone.

#### Exclusive intervals and infinity
min and max can be -inf and +inf, so that you are not required to know the highest or lowest score in the tairzset to get all elements from or up to a certain score.

By default, the interval specified by min and max is closed (inclusive). It is possible to specify an open interval (exclusive) by prefixing the score with the character (. For example:

EXZRANGEBYSCORE zset (1 5
Will return all elements with 1 < score <= 5 while:

EXZRANGEBYSCORE zset (5 (10
Will return all the elements with 5 < score < 10 (5 and 10 excluded).
#### Return value
Array reply: list of elements in the specified range (optionally with their scores).
### EXZREVRANGEBYSCORE
#### Grammar and complexity：
> EXZREVRANGEBYSCORE <key> <min> <max> [WITHSCORES]  
> time complexity： O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
#### Command Description:
Returns all the elements in the tairzset at key with a score between max and min (including elements with score equal to max or min). In contrary to the default ordering of tairzsets, for this command the elements are considered to be ordered from high to low scores.

The elements having the same score are returned in reverse lexicographical order.

Apart from the reversed ordering, EXZREVRANGEBYSCORE is similar to EXZRANGEBYSCORE.
#### Return value
Array reply: list of elements in the specified score range (optionally with their scores).
### EXZRANGEBYLEX
#### Grammar and complexity：
> EXZRANGEBYLEX <key> <min> <max> [LIMIT offset count]   
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
#### Command Description:
When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the tairzset at key with a value between min and max.

If the elements in the tairzset have different scores, the returned elements are unspecified.

The elements are considered to be ordered from lower to higher strings as compared byte-by-byte using the memcmp() C function. Longer strings are considered greater than shorter strings if the common part is identical.

The optional LIMIT argument can be used to only get a range of the matching elements (similar to SELECT LIMIT offset, count in SQL). A negative count returns all elements from the offset. Keep in mind that if offset is large, the tairzset needs to be traversed for offset elements before getting to the elements to return, which can add up to O(N) time complexity.

#### Return value
Array reply: list of elements in the specified score range.
### EXZREVRANGEBYLEX
#### Grammar and complexity：
> EXZREVRANGEBYLEX <key> <min> <max> [LIMIT offset count]  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
#### Command Description:
When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering, this command returns all the elements in the tairzset at key with a value between max and min.

Apart from the reversed ordering, EXZREVRANGEBYLEX is similar to EXZRANGEBYLEX.
#### Return value
Array reply: list of elements in the specified score range.
### EXZREM
#### Grammar and complexity：
> EXZREM key member [member ...]  
> time complexity：O(M*log(N)) with N being the number of elements in the tairzset and M the number of elements to be removed.

#### Command Description:
Removes the specified members from the tairzset stored at key. Non existing members are ignored.

An error is returned when key exists and does not hold a tairzset.

#### Return value
Integer reply, specifically:

The number of members removed from the tairzset, not including non existing members.
### EXZREMRANGEBYSCORE
#### Grammar and complexity：
> EXZREMRANGEBYSCORE key min max  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements removed by the operation.
#### Command Description:
Removes all elements in the tairzset stored at key with a score between min and max (inclusive).

#### Return value
Integer reply: the number of elements removed.
### EXZREMRANGEBYRANK
#### Grammar and complexity：
> EXZREMRANGEBYRANK key start stop  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements removed by the operation.

#### Command Description:
Removes all elements in the tairzset stored at key with rank between start and stop. Both start and stop are 0 -based indexes with 0 being the element with the lowest score. These indexes can be negative numbers, where they indicate offsets starting at the element with the highest score. For example: -1 is the element with the highest score, -2 the element with the second highest score and so forth.
#### Return value
Integer reply: the number of elements removed.
### EXZREMRANGEBYLEX
#### Grammar and complexity：
> EXZREMRANGEBYLEX key min max  
> time complexity：O(log(N)+M) with N being the number of elements in the tairzset and M the number of elements removed by the operation.

#### Command Description:
When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering, this command removes all elements in the tairzset stored at key between the lexicographical range specified by min and max.

The meaning of min and max are the same of the EXZRANGEBYLEX command. Similarly, this command actually removes the same elements that EXZRANGEBYLEX would return if called with the same min and max arguments.

#### Return value
Integer reply: the number of elements removed.
### EXZCARD
#### Grammar and complexity：
> EXZCARD key  
> time complexity：O(1)
#### Command Description:
Returns the tairzset cardinality (number of elements) of the tairzset stored at key.

#### Return value
Integer reply: the cardinality (number of elements) of the tairzset, or 0 if key does not exist.
### EXZRANK
> EXZRANK key member  
> time complexity：O(log(N))
#### Command Description:
Returns the rank of member in the tairzset stored at key, with the scores ordered from low to high. The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.

Use EXZREVRANK to get the rank of an element with the scores ordered from high to low.
#### Return value
If member exists in the tairzset, Integer reply: the rank of member.
If member does not exist in the tairzset or key does not exist, Bulk string reply: nil.
### EXZREVRANK
> EXZREVRANK key member  
> time complexity：O(log(N))

#### Command Description:
Returns the rank of member in the tairzset stored at key, with the scores ordered from high to low. The rank (or index) is 0-based, which means that the member with the highest score has rank 0.

Use EXZRANK to get the rank of an element with the scores ordered from low to high.

#### Return value
If member exists in the tairzset, Integer reply: the rank of member.
If member does not exist in the tairzset or key does not exist, Bulk string reply: nil.
### EXZCOUNT
> EXZCOUNT key min max   
> time complexity：O(log(N)) with N being the number of elements in the tairzset.

### EXZMSCORE
> EXZMSCORE key member [member ...]
> time complexity：O(N) where N is the number of members being requested.

#### Command Description:

Returns the scores associated with the specified members in the sorted set stored at key. For every member that does not exist in the sorted set, a nil value is returned.

#### Return value

Array reply: list of scores or nil associated with the specified member values (multiple double-precision floating-point numbers separated by #), represented as strings.

#### Command Description:
Returns the number of elements in the tairzset at key with a score between min and max.

The min and max arguments have the same semantic as described for ZRANGEBYSCORE.

Note: the command has a complexity of just O(log(N)) because it uses elements ranks (see ZRANK) to get an idea of the range. Because of this there is no need to do a work proportional to the size of the range.#### Options:

#### Return value
Integer reply: the number of elements in the specified score range.
### EXZLEXCOUNT
> EXZLEXCOUNT key min max  
> time complexity：O(log(N)) with N being the number of elements in the tairzset.

#### Command Description:
When all the elements in a tairzset are inserted with the same score, in order to force lexicographical ordering, this command returns the number of elements in the tairzset at key with a value between min and max.

The min and max arguments have the same meaning as described for EXZRANGEBYLEX.

Note: the command has a complexity of just O(log(N)) because it uses elements ranks (see ZRANK) to get an idea of the range. Because of this there is no need to do a work proportional to the size of the range.
#### Return value
Integer reply: the number of elements in the specified score range.

### EXZRANDMEMBER
> EXZRANDMEMBER key [count [WITHSCORES]]
> time complexity：O(N) where N is the number of elements returned.

#### Command Description:

When called with just the key argument, return a random element from the sorted set value stored at key.

If the provided count argument is positive, return an array of distinct elements. The array's length is either count or the sorted set's cardinality (EXZCARD), whichever is lower.

If called with a negative count, the behavior changes and the command is allowed to return the same element multiple times. In this case, the number of returned elements is the absolute value of the specified count.

The optional WITHSCORES modifier changes the reply so it includes the respective scores of the randomly selected elements from the sorted set.

#### Return value

Bulk string reply: without the additional count argument, the command returns a Bulk Reply with the randomly selected element, or nil when key does not exist.

Array reply: when the additional count argument is passed, the command returns an array of elements, or an empty array when key does not exist. If the WITHSCORES modifier is used, the reply is a list elements and their scores from the sorted set.
### EXZSCAN
> EXZSCAN key cursor [MATCH pattern] [COUNT count]
> time complexity: O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection.

#### Command Description:

Iterates elements of TairZset types and their associated scores.

EXSCAN is a cursor based iterator. This means that at every call of the command, the server returns an updated cursor that the user needs to use as the cursor argument in the next call. An iteration starts when the cursor is set to 0, and terminates when the cursor returned by the server is 0. 

While EXSCAN does not provide guarantees about the number of elements returned at every iteration, it is possible to empirically adjust the behavior of EXZSCAN using the COUNT option. Basically with COUNT the user specified the amount of work that should be done at every call in order to retrieve elements from the collection. This is *just a hint* for the implementation, however generally speaking this is what you could expect most of the times from the implementation.

It is possible to only iterate elements matching a given glob-style pattern, similarly to the behavior of the KEYS command that takes a pattern as only argument. To do so, just append the MATCH <pattern> arguments at the end of the EXZSCAN command. It is important to note that the MATCH filter is applied after elements are retrieved from the collection, just before returning data to the client. This means that if the pattern matches very little elements inside the collection, EXZSCAN will likely return no elements in most iterations.

More detail information: https://redis.io/commands/scan/

#### Return value

A two elements multi-bulk reply, where the first element is a string representing an unsigned 64 bit number (the cursor), and the second element is a multi-bulk with an array of elements.

### EXZUNIONSTORE

> EXZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]
> time complexity: O(N)+O(M log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set.

#### Command Description:

Computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination. It is mandatory to provide the number of input keys (numkeys) before passing the input keys and the other (optional) arguments.

By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists.

Using the WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set. This means that the score of every element in every input sorted set is multiplied by this factor before being passed to the aggregation function. When WEIGHTS is not given, the multiplication factors default to 1.

With the AGGREGATE option, it is possible to specify how the results of the union are aggregated. This option defaults to SUM, where the score of an element is summed across the inputs where it exists. When this option is set to either MIN or MAX, the resulting set will contain the minimum or maximum score of an element across the inputs where it exists.

If one dimension of the multiple score of a member become NaN during WEIGHTS or AGGREGATE operations, the dimension of the score will be set to 0.

If destination already exists, it is overwritten.

#### Return value

Integer reply: the number of elements in the resulting sorted set at destination.

### EXZUNION

> EXZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
> time complexity: O(N)+O(M*log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set.

#### Command Description:

This command is similar to ZUNIONSTORE, but instead of storing the resulting sorted set, it is returned to the client.

For a description of the WEIGHTS and AGGREGATE options, see ZUNIONSTORE.

#### Return value

Array reply: the result of union (optionally with their scores, in case the WITHSCORES option is given).


### EXZINTERSTORE

> EXZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]
> time complexity: O(NK)+O(Mlog(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set.

#### Command Description:

Computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination. It is mandatory to provide the number of input keys (numkeys) before passing the input keys and the other (optional) arguments.

By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists. Because intersection requires an element to be a member of every given sorted set, this results in the score of every element in the resulting sorted set to be equal to the number of input sorted sets.

For a description of the WEIGHTS and AGGREGATE options, see EXZUNIONSTORE.

If one dimension of the multiple score of a member become NaN during WEIGHTS or AGGREGATE operations, the dimension of the score will be set to 0.

If destination already exists, it is overwritten.

#### Return value

Integer reply: the number of elements in the resulting sorted set at destination.

### EXZINTER

> EXZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX] [WITHSCORES]
> time complexity: O(NK)+O(Mlog(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set.

#### Command Descriptions:

This command is similar to EXZINTERSTORE, but instead of storing the resulting sorted set, it is returned to the client.

For a description of the WEIGHTS and AGGREGATE options, see EXZUNIONSTORE.

#### Return value

Array reply: the result of intersection (optionally with their scores, in case the WITHSCORES option is given).

### EXZINTERCARD

> EXZINTERCARD numkeys key [key ...] [LIMIT limit]
> time complexity: O(N*K) worst case with N being the smallest input sorted set, K being the number of input sorted sets.

#### Command Descriptions:

This command is similar to EXZINTER, but instead of returning the result set, it returns just the cardinality of the result.

Keys that do not exist are considered to be empty sets. With one of the keys being an empty set, the resulting set is also empty (since set intersection with an empty set always results in an empty set).

By default, the command calculates the cardinality of the intersection of all given sets. When provided with the optional LIMIT argument (which defaults to 0 and means unlimited), if the intersection cardinality reaches limit partway through the computation, the algorithm will exit and yield limit as the cardinality. Such implementation ensures a significant speedup for queries where the limit is lower than the actual intersection cardinality.

#### Return value

Integer reply: the number of elements in the resulting intersection.

### EXZDIFFSTORE

> EXZDIFFSTORE destination numkeys key [key ...]
> time complexity: O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set.

#### Command Descriptions:

Computes the difference between the first and all successive input sorted sets and stores the result in destination. The total number of input keys is specified by numkeys.

Keys that do not exist are considered to be empty sets.

If destination already exists, it is overwritten.

#### Return value

Integer reply: the number of elements in the resulting sorted set at destination.

### EXZDIFF

> EXZDIFF numkeys key [key ...] [WITHSCORES]
> time complexity: O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set.

#### Command Descriptions:

This command is similar to EXZDIFFSTORE, but instead of storing the resulting sorted set, it is returned to the client.

#### Return value

Array reply: the result of the difference (optionally with their scores, in case the WITHSCORES option is given).


### EXZPOPMAX

> EXZPOPMAX key [count]
> time complexity: O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped.

#### Command Descriptions:

Removes and returns up to count members with the highest scores in the sorted set stored at key.

When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error. When returning multiple elements, the one with the highest score will be the first, followed by the elements with lower scores.

#### Return value

Array reply: list of popped elements and scores.

### EXZPOPMIN

> EXZPOPMIN key [count]
> time complexity: O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped.

#### Command Descriptions:

Removes and returns up to count members with the lowest scores in the sorted set stored at key.

When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error. When returning multiple elements, the one with the lowest score will be the first, followed by the elements with greater scores.

#### Return value

Array reply: list of popped elements and scores.
