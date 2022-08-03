set testmodule [file normalize your_path/tairzset_module.so]

start_server {tags {"tairzset"} overrides {bind 0.0.0.0}} {
    r module load $testmodule

    proc create_tairzset {key items} {
        r del $key
        foreach {score entry} $items {
            r exzadd $key $score $entry
        }
    }

    proc create_big_tairzset {key item} {
        r del $key
        for {set j 0} {$j < $item} {incr j} {
            r exzadd $key $j#$j#$j $j
        }
    }

    proc create_default_tairzset {} {
        create_tairzset tairzsetkey {-inf a 1 b 2 c 3 d 4 e 5 f +inf g}
    }

    test "EXZADD basic" {
        r del tairzsetkey
        assert {[r exzadd tairzsetkey 10 x 20 y 30 z] == 3}
        assert {[r exzcard tairzsetkey] == 3}
    }

    test "EXZADD schema socore" {
        r del tairzsetkey
        assert {[r exzadd tairzsetkey 1#1 x 2#2 y 3#3 z] == 3}
        assert {[r exzcard tairzsetkey] == 3}

        catch {r exzadd tairzsetkey 1#1 x 2#2# y 3#3 z} e
        assert_match {*score is not a valid format*} $e

        catch {r exzadd tairzsetkey 1#1 x #2#2 y 3#3 z} e
        assert_match {*score is not a valid format*} $e

        catch {r exzadd tairzsetkey 1#1 x 2#x y 3#3 z} e
        assert_match {*score is not a valid format*} $e

        catch {r exzadd tairzsetkey 1 x 2#2 y 3#3 z} e
        assert_match {*score is not a valid format*} $e
    }

    test "EXZRANGE use schema score" {
        r del tairzsetkey
        assert {[r exzadd tairzsetkey 1#1 x 2#2 y 3#3 z] == 3}
        assert {[r exzcard tairzsetkey] == 3}

        assert_equal {x y z} [r exzrange tairzsetkey 0 -1]
        assert_equal {x 1#1 y 2#2 z 3#3} [r exzrange tairzsetkey 0 -1 withscores]

        r exzadd tairzsetkey 1#3 x 2#2 y 3#1 z
        assert_equal {x 1#3 y 2#2 z 3#1} [r exzrange tairzsetkey 0 -1 withscores]

        r exzadd tairzsetkey 1#3 x 1#2 y 1#1 z
        assert_equal {z 1#1 y 1#2 x 1#3} [r exzrange tairzsetkey 0 -1 withscores]
    }

    test "EXZRANK/EXZREVRANK basics" {
        r del tairzsetkey
        r exzadd tairzsetkey 10 x
        r exzadd tairzsetkey 20 y
        r exzadd tairzsetkey 30 z
        assert_equal 0 [r exzrank tairzsetkey x]
        assert_equal 1 [r exzrank tairzsetkey y]
        assert_equal 2 [r exzrank tairzsetkey z]
        assert_equal "" [r exzrank tairzsetkey foo]
        assert_equal 2 [r exzrevrank tairzsetkey x]
        assert_equal 1 [r exzrevrank tairzsetkey y]
        assert_equal 0 [r exzrevrank tairzsetkey z]
        assert_equal "" [r exzrevrank tairzsetkey foo]
    }

    test "EXZRANKBYSCORE/EXZREVRANKBYSCORE basics" {
        r del tairzsetkey
        r exzadd tairzsetkey 10 x
        r exzadd tairzsetkey 20 y
        r exzadd tairzsetkey 30 z
        assert_equal 0 [r exzrankbyscore tairzsetkey -1]
        assert_equal 0 [r exzrankbyscore tairzsetkey 0]
        assert_equal 0 [r exzrankbyscore tairzsetkey 10]
        assert_equal 1 [r exzrankbyscore tairzsetkey 15]
        assert_equal 1 [r exzrankbyscore tairzsetkey 20]
        assert_equal 2 [r exzrankbyscore tairzsetkey 30]
        assert_equal 3 [r exzrevrankbyscore tairzsetkey 0]
        assert_equal 1 [r exzrevrankbyscore tairzsetkey 25]
        assert_equal 1 [r exzrevrankbyscore tairzsetkey 30]
        assert_equal 0 [r exzrevrankbyscore tairzsetkey 31]
    }

    test "EXZRANK - after deletion" {
        r exzrem tairzsetkey y
        assert_equal 0 [r exzrank tairzsetkey x]
        assert_equal 1 [r exzrank tairzsetkey z]
    }

    test "EXZINCRBY - can create a new sorted set" {
        r del tairzsetkey
        r exzincrby tairzsetkey 1 foo
        assert_equal {foo} [r exzrange tairzsetkey 0 -1]
        assert_equal 1 [r exzscore tairzsetkey foo]
    }

    test "EXZINCRBY - increment and decrement" {
        r exzincrby tairzsetkey 2 foo
        r exzincrby tairzsetkey 1 bar
        assert_equal {bar foo} [r exzrange tairzsetkey 0 -1]

        r exzincrby tairzsetkey 10 bar
        r exzincrby tairzsetkey -5 foo
        r exzincrby tairzsetkey -5 bar
        assert_equal {foo bar} [r exzrange tairzsetkey 0 -1]

        assert_equal -2 [r exzscore tairzsetkey foo]
        assert_equal  6 [r exzscore tairzsetkey bar]
    }

    test "EXZINCRBY return value" {
        r del tairzsetkey
        set retval [r zincrby tairzsetkey 1.0 x]
        assert {$retval == 1.0}
    }

    test "EXZINCRBY multi score" {
        r del tairzsetkey
        assert_equal {1#1} [r exzincrby tairzsetkey 1#1 foo]
        assert_equal {2#2} [r exzincrby tairzsetkey 1#1 foo]
        assert_equal {3#2} [r exzincrby tairzsetkey 1#0 foo]
        assert_equal {3#3} [r exzincrby tairzsetkey 0#1 foo]
        assert_equal {3#3} [r exzincrby tairzsetkey 0#0 foo]
        catch {r exzincrby tairzsetkey 1#1#1 foo} e
        assert_match {*score is not a valid format*} $e

        r del tairzsetkey
        assert_equal {1#1#0} [r exzincrby tairzsetkey 1#1#0 foo]
        assert_equal {2#2#0} [r exzincrby tairzsetkey 1#1#0 foo]
        assert_equal {2#2#1} [r exzincrby tairzsetkey 0#0#1 foo]
    }

    test "EXZSET basic EXZADD and score update " {
        r del ztmp
        r exzadd ztmp 10 x
        r exzadd ztmp 20 y
        r exzadd ztmp 30 z
        assert_equal {x y z} [r exzrange ztmp 0 -1]

        r exzadd ztmp 1 y
        assert_equal {y x z} [r exzrange ztmp 0 -1]
    }

    test "EXZSET element can't be set to NaN with EXZADD" {
        assert_error "*score is not a valid format*" {r exzadd myzset nan abc}
    }

    test "EXZSET element can't be set to NaN with EXZINCRBY" {
        assert_error "*score is not a valid format*" {r exzadd myzset nan abc}
    }

    test "EXZADD with options syntax error with incomplete pair" {
        r del ztmp
        catch {r exzadd ztmp xx 10 x 20} err
        set err
    } {ERR*}

    test "EXZADD XX option without key" {
        r del ztmp
        assert {[r exzadd ztmp xx 10 x] == 0}
        assert {[r type ztmp] eq {none}}
    }

    test "EXZADD XX existing key" {
        r del ztmp
        r exzadd ztmp 10 x
        assert {[r exzadd ztmp xx 20 y] == 0}
        assert {[r exzcard ztmp] == 1}
    }

    test "EXZADD XX returns the number of elements actually added" {
        r del ztmp
        r exzadd ztmp 10 x
        set retval [r exzadd ztmp 10 x 20 y 30 z]
        assert {$retval == 2}
    }

    test "EXZADD XX updates existing elements score" {
        r del ztmp
        r exzadd ztmp 10 x 20 y 30 z
        r exzadd ztmp xx 5 foo 11 x 21 y 40 zap
        assert {[r exzcard ztmp] == 3}
        assert {[r exzscore ztmp x] == 11}
        assert {[r exzscore ztmp y] == 21}
    }

    test "EXZADD XX and NX are not compatible" {
        r del ztmp
        catch {r exzadd ztmp xx nx 10 x} err
        set err
    } {ERR*}

    test "EXZADD NX with non existing key" {
        r del ztmp
        r exzadd ztmp nx 10 x 20 y 30 z
        assert {[r exzcard ztmp] == 3}
    }

    test "EXZADD NX only add new elements without updating old ones" {
        r del ztmp
        r exzadd ztmp 10 x 20 y 30 z
        assert {[r exzadd ztmp nx 11 x 21 y 100 a 200 b] == 2}
        assert {[r exzscore ztmp x] == 10}
        assert {[r exzscore ztmp y] == 20}
        assert {[r exzscore ztmp a] == 100}
        assert {[r exzscore ztmp b] == 200}
    }

    test "EXZADD INCR works like ZINCRBY" {
        r del ztmp
        r exzadd ztmp 10 x 20 y 30 z
        r exzadd ztmp INCR 15 x
        assert {[r exzscore ztmp x] == 25}
    }

    test "EXZADD INCR works with a single score-elemenet pair" {
        r del ztmp
        r exzadd ztmp 10 x 20 y 30 z
        catch {r exzadd ztmp INCR 15 x 10 y} err
        set err
    } {ERR*}

    test "EXZADD CH option changes return value to all changed elements" {
        r del ztmp
        r exzadd ztmp 10 x 20 y 30 z
        assert {[r exzadd ztmp 11 x 21 y 30 z] == 0}
        assert {[r exzadd ztmp ch 12 x 22 y 30 z] == 2}
    }

    test "EXZINCRBY calls leading to NaN result in error" {
        r exzincrby myzset +inf abc
        assert_error "*NaN*" {r exzincrby myzset -inf abc}
    }

    test {EXZADD - Variadic version base case} {
        r del myzset
        list [r exzadd myzset 10 a 20 b 30 c] [r exzrange myzset 0 -1 withscores]
    } {3 {a 10 b 20 c 30}}

    test {EXZADD - Return value is the number of actually added items} {
        list [r exzadd myzset 5 x 20 b 30 c] [r exzrange myzset 0 -1 withscores]
    } {1 {x 5 a 10 b 20 c 30}}

    test {EXZADD - Variadic version does not add nothing on single parsing err} {
        r del myzset
        catch {r exzadd myzset 10 a 20 b 30.badscore c} e
        assert_match {*ERR score is not a valid format*} $e
        r exists myzset
    } {0}

    test {EXZADD - Variadic version will raise error on missing arg} {
        r del myzset
        catch {r exzadd myzset 10 a 20 b 30 c 40} e
        assert_match {*ERR*syntax*} $e
    }

    test {EXZINCRBY does not work variadic even if shares ZADD implementation} {
        r del myzset
        catch {r exzincrby myzset 10 a 20 b 30 c} e
        assert_match {*ERR*wrong*number*arg*} $e
    }

    test "EXZCARD basics" {
        r del ztmp
        r exzadd ztmp 10 a 20 b 30 c
        assert_equal 3 [r exzcard ztmp]
        assert_equal 0 [r exzcard zdoesntexist]
    }

    test "EXZREM removes key after last element is removed" {
        r del ztmp
        r exzadd ztmp 10 x
        r exzadd ztmp 20 y

        assert_equal 1 [r exists ztmp]
        assert_equal 0 [r exzrem ztmp z]
        assert_equal 1 [r exzrem ztmp y]
        assert_equal 1 [r exzrem ztmp x]
        assert_equal 0 [r exists ztmp]
    }

    test "EXZREM variadic version" {
        r del ztmp
        r exzadd ztmp 10 a 20 b 30 c
        assert_equal 2 [r exzrem ztmp x y a b k]
        assert_equal 0 [r exzrem ztmp foo bar]
        assert_equal 1 [r exzrem ztmp c]
        r exists ztmp
    } {0}

    test "EXZREM variadic version -- remove elements after key deletion" {
        r del ztmp
        r exzadd ztmp 10 a 20 b 30 c
        r exzrem ztmp a b c d e f g
    } {3}

    test "EXZRANGE basics" {
        r del ztmp
        r exzadd ztmp 1 a
        r exzadd ztmp 2 b
        r exzadd ztmp 3 c
        r exzadd ztmp 4 d

        assert_equal {a b c d} [r exzrange ztmp 0 -1]
        assert_equal {a b c} [r exzrange ztmp 0 -2]
        assert_equal {b c d} [r exzrange ztmp 1 -1]
        assert_equal {b c} [r exzrange ztmp 1 -2]
        assert_equal {c d} [r exzrange ztmp -2 -1]
        assert_equal {c} [r exzrange ztmp -2 -2]

        # out of range start index
        assert_equal {a b c} [r exzrange ztmp -5 2]
        assert_equal {a b} [r exzrange ztmp -5 1]
        assert_equal {} [r exzrange ztmp 5 -1]
        assert_equal {} [r exzrange ztmp 5 -2]

        # out of range end index
        assert_equal {a b c d} [r exzrange ztmp 0 5]
        assert_equal {b c d} [r exzrange ztmp 1 5]
        assert_equal {} [r exzrange ztmp 0 -5]
        assert_equal {} [r exzrange ztmp 1 -5]

        # withscores
        assert_equal {a 1 b 2 c 3 d 4} [r exzrange ztmp 0 -1 withscores]
    }

    test "EXZREVRANGE basics" {
        r del ztmp
        r exzadd ztmp 1 a
        r exzadd ztmp 2 b
        r exzadd ztmp 3 c
        r exzadd ztmp 4 d

        assert_equal {d c b a} [r exzrevrange ztmp 0 -1]
        assert_equal {d c b} [r exzrevrange ztmp 0 -2]
        assert_equal {c b a} [r exzrevrange ztmp 1 -1]
        assert_equal {c b} [r exzrevrange ztmp 1 -2]
        assert_equal {b a} [r exzrevrange ztmp -2 -1]
        assert_equal {b} [r exzrevrange ztmp -2 -2]

        # out of range start index
        assert_equal {d c b} [r exzrevrange ztmp -5 2]
        assert_equal {d c} [r exzrevrange ztmp -5 1]
        assert_equal {} [r exzrevrange ztmp 5 -1]
        assert_equal {} [r exzrevrange ztmp 5 -2]

        # out of range end index
        assert_equal {d c b a} [r exzrevrange ztmp 0 5]
        assert_equal {c b a} [r exzrevrange ztmp 1 5]
        assert_equal {} [r exzrevrange ztmp 0 -5]
        assert_equal {} [r exzrevrange ztmp 1 -5]

        # withscores
        assert_equal {d 4 c 3 b 2 a 1} [r exzrevrange ztmp 0 -1 withscores]
    }

    test "EXZRANK/EXZREVRANK basics" {
        r del zranktmp
        r exzadd zranktmp 10 x
        r exzadd zranktmp 20 y
        r exzadd zranktmp 30 z
        assert_equal 0 [r exzrank zranktmp x]
        assert_equal 1 [r exzrank zranktmp y]
        assert_equal 2 [r exzrank zranktmp z]
        assert_equal "" [r exzrank zranktmp foo]
        assert_equal 2 [r exzrevrank zranktmp x]
        assert_equal 1 [r exzrevrank zranktmp y]
        assert_equal 0 [r exzrevrank zranktmp z]
        assert_equal "" [r exzrevrank zranktmp foo]
    }

    test "EXZRANK - after deletion" {
        r exzrem zranktmp y
        assert_equal 0 [r exzrank zranktmp x]
        assert_equal 1 [r exzrank zranktmp z]
    }

    test "EXZINCRBY - can create a new sorted set" {
        r del zset
        r exzincrby zset 1 foo
        assert_equal {foo} [r exzrange zset 0 -1]
        assert_equal 1 [r exzscore zset foo]
    }

    test "EXZINCRBY - increment and decrement" {
        r exzincrby zset 2 foo
        r exzincrby zset 1 bar
        assert_equal {bar foo} [r exzrange zset 0 -1]

        r exzincrby zset 10 bar
        r exzincrby zset -5 foo
        r exzincrby zset -5 bar
        assert_equal {foo bar} [r exzrange zset 0 -1]

        assert_equal -2 [r exzscore zset foo]
        assert_equal  6 [r exzscore zset bar]
    }

    test "EXZINCRBY return value" {
        r del ztmp
        set retval [r exzincrby ztmp 1.0 x]
        assert {$retval == 1.0}
    }


    test "EXZRANGEBYSCORE/EXZREVRANGEBYSCORE/EXZCOUNT basics" {
        create_default_tairzset

        # inclusive range
        assert_equal {a b c} [r exzrangebyscore tairzsetkey -inf 2]
        assert_equal {b c d} [r exzrangebyscore tairzsetkey 0 3]
        assert_equal {d e f} [r exzrangebyscore tairzsetkey 3 6]
        assert_equal {e f g} [r exzrangebyscore tairzsetkey 4 +inf]
        assert_equal {c b a} [r exzrevrangebyscore tairzsetkey 2 -inf]
        assert_equal {d c b} [r exzrevrangebyscore tairzsetkey 3 0]
        assert_equal {f e d} [r exzrevrangebyscore tairzsetkey 6 3]
        assert_equal {g f e} [r exzrevrangebyscore tairzsetkey +inf 4]
        assert_equal 3 [r exzcount tairzsetkey 0 3]

        # exclusive range
        assert_equal {b}   [r exzrangebyscore tairzsetkey (-inf (2]
        assert_equal {b c} [r exzrangebyscore tairzsetkey (0 (3]
        assert_equal {e f} [r exzrangebyscore tairzsetkey (3 (6]
        assert_equal {f}   [r exzrangebyscore tairzsetkey (4 (+inf]
        assert_equal {b}   [r exzrevrangebyscore tairzsetkey (2 (-inf]
        assert_equal {c b} [r exzrevrangebyscore tairzsetkey (3 (0]
        assert_equal {f e} [r exzrevrangebyscore tairzsetkey (6 (3]
        assert_equal {f}   [r exzrevrangebyscore tairzsetkey (+inf (4]
        assert_equal 2 [r exzcount tairzsetkey (0 (3]

        # test empty ranges
        r exzrem tairzsetkey a
        r exzrem tairzsetkey g

        # inclusive
        assert_equal {} [r exzrangebyscore tairzsetkey 4 2]
        assert_equal {} [r exzrangebyscore tairzsetkey 6 +inf]
        assert_equal {} [r exzrangebyscore tairzsetkey -inf -6]
        assert_equal {} [r exzrevrangebyscore tairzsetkey +inf 6]
        assert_equal {} [r exzrevrangebyscore tairzsetkey -6 -inf]

        # exclusive
        assert_equal {} [r exzrangebyscore tairzsetkey (4 (2]
        assert_equal {} [r exzrangebyscore tairzsetkey 2 (2]
        assert_equal {} [r exzrangebyscore tairzsetkey (2 2]
        assert_equal {} [r exzrangebyscore tairzsetkey (6 (+inf]
        assert_equal {} [r exzrangebyscore tairzsetkey (-inf (-6]
        assert_equal {} [r exzrevrangebyscore tairzsetkey (+inf (6]
        assert_equal {} [r exzrevrangebyscore tairzsetkey (-6 (-inf]

        # empty inner range
        assert_equal {} [r exzrangebyscore tairzsetkey 2.4 2.6]
        assert_equal {} [r exzrangebyscore tairzsetkey (2.4 2.6]
        assert_equal {} [r exzrangebyscore tairzsetkey 2.4 (2.6]
        assert_equal {} [r exzrangebyscore tairzsetkey (2.4 (2.6]
    }

    test "EXZRANGEBYSCORE with WITHSCORES" {
        create_default_tairzset
        assert_equal {b 1 c 2 d 3} [r exzrangebyscore tairzsetkey 0 3 withscores]
        assert_equal {d 3 c 2 b 1} [r exzrevrangebyscore tairzsetkey 3 0 withscores]
    }

    test "EXZRANGEBYSCORE with LIMIT" {
        create_default_tairzset
        assert_equal {b c}   [r exzrangebyscore tairzsetkey 0 10 LIMIT 0 2]
        assert_equal {d e f} [r exzrangebyscore tairzsetkey 0 10 LIMIT 2 3]
        assert_equal {d e f} [r exzrangebyscore tairzsetkey 0 10 LIMIT 2 10]
        assert_equal {}      [r exzrangebyscore tairzsetkey 0 10 LIMIT 20 10]
        assert_equal {f e}   [r exzrevrangebyscore tairzsetkey 10 0 LIMIT 0 2]
        assert_equal {d c b} [r exzrevrangebyscore tairzsetkey 10 0 LIMIT 2 3]
        assert_equal {d c b} [r exzrevrangebyscore tairzsetkey 10 0 LIMIT 2 10]
        assert_equal {}      [r exzrevrangebyscore tairzsetkey 10 0 LIMIT 20 10]
    }

    test "EXZRANGEBYSCORE with LIMIT and WITHSCORES" {
        create_default_tairzset
        assert_equal {e 4 f 5} [r exzrangebyscore tairzsetkey 2 5 LIMIT 2 3 WITHSCORES]
        assert_equal {d 3 c 2} [r exzrevrangebyscore tairzsetkey 5 2 LIMIT 2 3 WITHSCORES]
        assert_equal {} [r exzrangebyscore tairzsetkey 2 5 LIMIT 12 13 WITHSCORES]
    }

    test "EXZRANGEBYSCORE with non-value min or max" {
        assert_error "*not*float*" {r exzrangebyscore fooz str 1}
        assert_error "*not*float*" {r exzrangebyscore fooz 1 str}
        assert_error "*not*float*" {r exzrangebyscore fooz 1 NaN}
    }

    test "AOF rewrite" {
        create_big_tairzset tairzsetkey 1000
        assert_equal 1000 [r exzcard tairzsetkey]

        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof

        assert_equal 1000 [r exzcard tairzsetkey]
    }

    test "RDB save/load" {
        create_big_tairzset tairzsetkey 1000
        assert_equal 1000 [r exzcard tairzsetkey]

        r bgsave
        waitForBgsave r
        r debug reload

        assert_equal 1000 [r exzcard tairzsetkey]
    }

    set elements 128
    test "EXZRANGEBYSCORE fuzzy test, 100 ranges in $elements element sorted set" {
        set err {}
        r del zset
        for {set i 0} {$i < $elements} {incr i} {
            r exzadd zset [expr rand()] $i
        }


        for {set i 0} {$i < 100} {incr i} {
            set min [expr rand()]
            set max [expr rand()]
            if {$min > $max} {
                set aux $min
                set min $max
                set max $aux
            }
            set low [r exzrangebyscore zset -inf $min]
            set ok [r exzrangebyscore zset $min $max]
            set high [r exzrangebyscore zset $max +inf]
            set lowx [r exzrangebyscore zset -inf ($min]
            set okx [r exzrangebyscore zset ($min ($max]
            set highx [r exzrangebyscore zset ($max +inf]

            if {[r exzcount zset -inf $min] != [llength $low]} {
                append err "Error, len does not match zcount\n"
            }
            if {[r exzcount zset $min $max] != [llength $ok]} {
                append err "Error, len does not match zcount\n"
            }
            if {[r exzcount zset $max +inf] != [llength $high]} {
                append err "Error, len does not match zcount\n"
            }
            if {[r exzcount zset -inf ($min] != [llength $lowx]} {
                append err "Error, len does not match zcount\n"
            }
            if {[r exzcount zset ($min ($max] != [llength $okx]} {
                append err "Error, len does not match zcount\n"
            }
            if {[r exzcount zset ($max +inf] != [llength $highx]} {
                append err "Error, len does not match zcount\n"
            }

            foreach x $low {
                set score [r exzscore zset $x]
                if {$score > $min} {
                    append err "Error, score for $x is $score > $min\n"
                }
            }
            foreach x $lowx {
                set score [r exzscore zset $x]
                if {$score >= $min} {
                    append err "Error, score for $x is $score >= $min\n"
                }
            }
            foreach x $ok {
                set score [r exzscore zset $x]
                if {$score < $min || $score > $max} {
                    append err "Error, score for $x is $score outside $min-$max range\n"
                }
            }
            foreach x $okx {
                set score [r exzscore zset $x]
                if {$score <= $min || $score >= $max} {
                    append err "Error, score for $x is $score outside $min-$max open range\n"
                }
            }
            foreach x $high {
                set score [r exzscore zset $x]
                if {$score < $max} {
                    append err "Error, score for $x is $score < $max\n"
                }
            }
            foreach x $highx {
                set score [r exzscore zset $x]
                if {$score <= $max} {
                    append err "Error, score for $x is $score <= $max\n"
                }
            }
        }
        assert_equal {} $err
    }

     test "EXZREMRANGEBYSCORE basics" {
        proc remrangebyscore {min max} {
            create_tairzset zset {1 a 2 b 3 c 4 d 5 e}
            assert_equal 1 [r exists zset]
            r exzremrangebyscore zset $min $max
        }

        # inner range
        assert_equal 3 [remrangebyscore 2 4]
        assert_equal {a e} [r exzrange zset 0 -1]

        # start underflow
        assert_equal 1 [remrangebyscore -10 1]
        assert_equal {b c d e} [r exzrange zset 0 -1]

        # end overflow
        assert_equal 1 [remrangebyscore 5 10]
        assert_equal {a b c d} [r exzrange zset 0 -1]

        # switch min and max
        assert_equal 0 [remrangebyscore 4 2]
        assert_equal {a b c d e} [r exzrange zset 0 -1]

        # -inf to mid
        assert_equal 3 [remrangebyscore -inf 3]
        assert_equal {d e} [r exzrange zset 0 -1]

        # mid to +inf
        assert_equal 3 [remrangebyscore 3 +inf]
        assert_equal {a b} [r exzrange zset 0 -1]

        # -inf to +inf
        assert_equal 5 [remrangebyscore -inf +inf]
        assert_equal {} [r exzrange zset 0 -1]

        # exclusive min
        assert_equal 4 [remrangebyscore (1 5]
        assert_equal {a} [r exzrange zset 0 -1]
        assert_equal 3 [remrangebyscore (2 5]
        assert_equal {a b} [r exzrange zset 0 -1]

        # exclusive max
        assert_equal 4 [remrangebyscore 1 (5]
        assert_equal {e} [r exzrange zset 0 -1]
        assert_equal 3 [remrangebyscore 1 (4]
        assert_equal {d e} [r exzrange zset 0 -1]

        # exclusive min and max
        assert_equal 3 [remrangebyscore (1 (5]
        assert_equal {a e} [r exzrange zset 0 -1]

        # destroy when empty
        assert_equal 5 [remrangebyscore 1 5]
        assert_equal 0 [r exists zset]
    }

    test "EXZREMRANGEBYSCORE with non-value min or max" {
        assert_error "*not*float*" {r exzremrangebyscore fooz str 1}
        assert_error "*not*float*" {r exzremrangebyscore fooz 1 str}
        assert_error "*not*float*" {r exzremrangebyscore fooz 1 NaN}
    }

    test "EXZREMRANGEBYRANK basics" {
        proc remrangebyrank {min max} {
            create_tairzset zset {1 a 2 b 3 c 4 d 5 e}
            assert_equal 1 [r exists zset]
            r exzremrangebyrank zset $min $max
        }

        # inner range
        assert_equal 3 [remrangebyrank 1 3]
        assert_equal {a e} [r exzrange zset 0 -1]

        # start underflow
        assert_equal 1 [remrangebyrank -10 0]
        assert_equal {b c d e} [r exzrange zset 0 -1]

        # start overflow
        assert_equal 0 [remrangebyrank 10 -1]
        assert_equal {a b c d e} [r exzrange zset 0 -1]

        # end underflow
        assert_equal 0 [remrangebyrank 0 -10]
        assert_equal {a b c d e} [r exzrange zset 0 -1]

        # end overflow
        assert_equal 5 [remrangebyrank 0 10]
        assert_equal {} [r exzrange zset 0 -1]

        # destroy when empty
        assert_equal 5 [remrangebyrank 0 4]
        assert_equal 0 [r exists zset]
    }

    test "TAIRZSET sorting stresser" {
        set delta 0
        for {set test 0} {$test < 2} {incr test} {
            unset -nocomplain auxarray
            array set auxarray {}
            set auxlist {}
            r del myzset
            for {set i 0} {$i < $elements} {incr i} {
                if {$test == 0} {
                    set score [expr rand()]
                } else {
                    set score [expr int(rand()*10)]
                }
                set auxarray($i) $score
                r exzadd myzset $score $i
                # Random update
                if {[expr rand()] < .2} {
                    set j [expr int(rand()*1000)]
                    if {$test == 0} {
                        set score [expr rand()]
                    } else {
                        set score [expr int(rand()*10)]
                    }
                    set auxarray($j) $score
                    r exzadd myzset $score $j
                }
            }
            foreach {item score} [array get auxarray] {
                lappend auxlist [list $score $item]
            }
            set sorted [lsort -command zlistAlikeSort $auxlist]
            set auxlist {}
            foreach x $sorted {
                lappend auxlist [lindex $x 1]
            }

            set fromredis [r exzrange myzset 0 -1]
            set delta 0
            for {set i 0} {$i < [llength $fromredis]} {incr i} {
                if {[lindex $fromredis $i] != [lindex $auxlist $i]} {
                    incr delta
                }
            }
        }
        assert_equal 0 $delta
    }

    proc create_default_lex_tairzset {} {
        create_tairzset zset {0 alpha 0 bar 0 cool 0 down
                            0 elephant 0 foo 0 great 0 hill
                            0 omega}
    }

    test "EXZRANGEBYLEX/EXZREVRANGEBYLEX/EXZLEXCOUNT basics" {
        create_default_lex_tairzset

        # inclusive range
        assert_equal {alpha bar cool} [r exzrangebylex zset - \[cool]
        assert_equal {bar cool down} [r exzrangebylex zset \[bar \[down]
        assert_equal {great hill omega} [r exzrangebylex zset \[g +]
        assert_equal {cool bar alpha} [r exzrevrangebylex zset \[cool -]
        assert_equal {down cool bar} [r exzrevrangebylex zset \[down \[bar]
        assert_equal {omega hill great foo elephant down} [r exzrevrangebylex zset + \[d]
        assert_equal 3 [r exzlexcount zset \[ele \[h]

        # exclusive range
        assert_equal {alpha bar} [r exzrangebylex zset - (cool]
        assert_equal {cool} [r exzrangebylex zset (bar (down]
        assert_equal {hill omega} [r exzrangebylex zset (great +]
        assert_equal {bar alpha} [r exzrevrangebylex zset (cool -]
        assert_equal {cool} [r exzrevrangebylex zset (down (bar]
        assert_equal {omega hill} [r exzrevrangebylex zset + (great]
        assert_equal 2 [r exzlexcount zset (ele (great]

        # inclusive and exclusive
        assert_equal {} [r exzrangebylex zset (az (b]
        assert_equal {} [r exzrangebylex zset (z +]
        assert_equal {} [r exzrangebylex zset - \[aaaa]
        assert_equal {} [r exzrevrangebylex zset \[elez \[elex]
        assert_equal {} [r exzrevrangebylex zset (hill (omega]
    }
    
    test "EXZLEXCOUNT advanced" {
        create_default_lex_tairzset

        assert_equal 9 [r exzlexcount zset - +]
        assert_equal 0 [r exzlexcount zset + -]
        assert_equal 0 [r exzlexcount zset + \[c]
        assert_equal 0 [r exzlexcount zset \[c -]
        assert_equal 8 [r exzlexcount zset \[bar +]
        assert_equal 5 [r exzlexcount zset \[bar \[foo]
        assert_equal 4 [r exzlexcount zset \[bar (foo]
        assert_equal 4 [r exzlexcount zset (bar \[foo]
        assert_equal 3 [r exzlexcount zset (bar (foo]
        assert_equal 5 [r exzlexcount zset - (foo]
        assert_equal 1 [r exzlexcount zset (maxstring +]
    }

    test "EXZRANGEBYSLEX with LIMIT" {
        create_default_lex_tairzset
        assert_equal {alpha bar} [r exzrangebylex zset - \[cool LIMIT 0 2]
        assert_equal {bar cool} [r exzrangebylex zset - \[cool LIMIT 1 2]
        assert_equal {} [r exzrangebylex zset \[bar \[down LIMIT 0 0]
        assert_equal {} [r exzrangebylex zset \[bar \[down LIMIT 2 0]
        assert_equal {bar} [r exzrangebylex zset \[bar \[down LIMIT 0 1]
        assert_equal {cool} [r exzrangebylex zset \[bar \[down LIMIT 1 1]
        assert_equal {bar cool down} [r exzrangebylex zset \[bar \[down LIMIT 0 100]
        assert_equal {omega hill great foo elephant} [r exzrevrangebylex zset + \[d LIMIT 0 5]
        assert_equal {omega hill great foo} [r exzrevrangebylex zset + \[d LIMIT 0 4]
    }

    test "EXZRANGEBYLEX with invalid lex range specifiers" {
        assert_error "*not*string*" {r exzrangebylex fooz foo bar}
        assert_error "*not*string*" {r exzrangebylex fooz \[foo bar}
        assert_error "*not*string*" {r exzrangebylex fooz foo \[bar}
        assert_error "*not*string*" {r exzrangebylex fooz +x \[bar}
        assert_error "*not*string*" {r exzrangebylex fooz -x \[bar}
    }

    test "EXZMSCORE basic" {   
        r del exzmscoretest
        r exzadd exzmscoretest 10 x
        r exzadd exzmscoretest 20 y

        assert_equal {10 20} [r exzmscore exzmscoretest x y]

        r del exzmscoretest
        r exzadd exzmscoretest 10#20 x
        r exzadd exzmscoretest 20#30 y

        assert_equal {10#20 20#30} [r exzmscore exzmscoretest x y]
    } 

    test "EXZMSCORE retrieve from empty set" {
        r del exzmscoretest

        assert_equal {{} {}} [r exzmscore exzmscoretest x y]
    } 

    test "EXZMSCORE retrieve with missing member" {
        r del exzmscoretest
        r exzadd exzmscoretest 10 x

        assert_equal {10 {}} [r exzmscore exzmscoretest x y]

        r del exzmscoretest
        r exzadd exzmscoretest 10#1.1 x

        assert_equal {10#1.1000000000000001 {}} [r exzmscore exzmscoretest x y]
    } 

    test "EXZMSCORE retrieve single member" {
        r del exzmscoretest
        r exzadd exzmscoretest 10 x
        r exzadd exzmscoretest 20 y

        assert_equal {10} [r exzmscore exzmscoretest x]
        assert_equal {20} [r exzmscore exzmscoretest y]

        r del exzmscoretest
        r exzadd exzmscoretest 10#20#30#40 x
        r exzadd exzmscoretest 20#10#50#60 y

        assert_equal {10#20#30#40} [r exzmscore exzmscoretest x]
        assert_equal {20#10#50#60} [r exzmscore exzmscoretest y]
    } 

    test "EXZMSCORE retrieve requires one or more members" {
        r del exzmscoretest
        r zadd exzmscoretest 10 x
        r zadd exzmscoretest 20 y

        catch {r exzmscore exzmscoretest} e
        assert_match {*ERR*wrong*number*arg*} $e
    }

    test "EXZRANDMEMBER errors" {
        catch {r exzrandmember myzset 1 other args} e
        assert_match {*ERR*syntax*} $e

        catch {r exzrandmember myzset 1 withscores other} e
        assert_match {*ERR*syntax*} $e

        catch {r exzrandmember myzset 1 other}
        assert_match {*ERR*syntax*} $e

        catch {r exzrandmember myzset 1.1} e
        assert_match {*ERR*not*integer*} $e

        catch {r exzrandmember myzset not-int} e
        assert_match {*ERR*not*integer*} $e
    }

    test "EXZRANDMEMBER count of 0 is handled correctly" {
        assert_equal {} [r exzrandmember myzset 0]
    }

    test "EXZRANDMEMBER with <count> against non existing key" {
        assert_equal {} [r exzrandmember nonexisting_key 100]
    }

    # Make sure we can distinguish between an empty array and a null response
    # readraw is not supported in Redis 5.0
    set support_readraw true
    if {[catch {r readraw 1}]} {
        set support_readraw false
    }

    if {$support_readraw == true} {
        test "EXZRANDMEMBER count of 0 is handled correctly - emptyarray" {
            r del myzset
            r exzadd myzset 1#2 member
            assert_equal {*0} [r exzrandmember myzset 0]
        }

        test "EXZRANDMEMBER with <count> against non existing key - emptyarray" {
            assert_equal {*0} [r exzrandmember nonexisting_key 100]
        } 

        r readraw 0
    }


    proc get_keys {l} {
        set res {}
        foreach {score key} $l {
            lappend res $key
        }
        return $res
    }

    test "EXZRANDMEMBER without <count>" {
        set contents {1#2#3 a 2#3#4 b 3#4#5 c}
        create_tairzset myzset $contents
        array set arr {}
        for {set i 0} {$i < 100} {incr i} {
            set key [r exzrandmember myzset]
            set arr($key) 1
        }
        assert_equal [lsort [get_keys $contents]] [lsort [array names arr]]
    }

    # Check whether the zset members belong to the zset
    proc check_member {mydict res} {
        foreach ele $res {
            assert {[dict exists $mydict $ele]}
        }
    }

    # Check whether the zset members and score belong to the zset
    proc check_member_and_score {mydict res} {
       foreach {key val} $res {
            assert_equal $val [dict get $mydict $key]
        }
    }

    # Calculation value of Chi-Square Distribution. By this value
    # we can verify the random distribution sample confidence.
    # Based on the following wiki:
    # https://en.wikipedia.org/wiki/Chi-square_distribution
    #
    # param res    Random sample list
    # return       Value of Chi-Square Distribution
    #
    # x2_value: return of chi_square_value function
    # df: Degrees of freedom, Number of independent values minus 1
    #
    # By using x2_value and df to back check the cardinality table,
    # we can know the confidence of the random sample.
    #
    # This proc is added in Redis 6.2
    proc chi_square_value {res} {
        unset -nocomplain mydict
        foreach key $res {
            dict incr mydict $key 1
        }

        set x2_value 0
        set p [expr [llength $res] / [dict size $mydict]]
        foreach key [dict keys $mydict] {
            set value [dict get $mydict $key]

            # Aggregate the chi-square value of each element
            set v [expr {pow($value - $p, 2) / $p}]
            set x2_value [expr {$x2_value + $v}]
        }

        return $x2_value
    }

    test "EXZRANDMEMBER with <count>" {
        set contents {1#1.1 a 2#2.2 b 3#3.3 c 4#4.4 d 5#5.5 e 6#6.6 f 7#7.7 g 7#7.8 h 9#9.9 i 10#10.1 j}
        create_tairzset myzset $contents

        # create a dict for easy lookup
        set mydict [dict create {*}[r exzrange myzset 0 -1 withscores]]

        # We'll stress different parts of the code, see the implementation
        # of EXZRANDMEMBER for more information, but basically there are
        # four different code paths.

        # PATH 1: Use negative count.

        # 1) Check that it returns repeated elements with and without values.
        # 2) Check that all the elements actually belong to the original zset.
        set res [r exzrandmember myzset -20]
        assert_equal [llength $res] 20
        check_member $mydict $res

        set res [r exzrandmember myzset -1001]
        assert_equal [llength $res] 1001
        check_member $mydict $res

        # again with WITHSCORES
        set res [r exzrandmember myzset -20 withscores]
        assert_equal [llength $res] 40
        check_member_and_score $mydict $res

        set res [r exzrandmember myzset -1001 withscores]
        assert_equal [llength $res] 2002
        check_member_and_score $mydict $res

        # Test random uniform distribution
        # df = 9, 40 means 0.00001 probability
        set res [r exzrandmember myzset -1000]
        assert { [chi_square_value $res] < 40 }
        check_member $mydict $res

        # 3) Check that eventually all the elements are returned.
        #    Use both WITHSCORES and without
        unset -nocomplain auxset
        set iterations 1000
        while {$iterations != 0} {
            incr iterations -1
            if {[expr {$iterations % 2}] == 0} {
                set res [r exzrandmember myzset -3 withscores]
                foreach {key val} $res {
                    dict append auxset $key $val
                }
            } else {
                set res [r exzrandmember myzset -3]
                foreach key $res {
                    dict append auxset $key
                }
            }
            if {[lsort [dict keys $mydict]] eq
                [lsort [dict keys $auxset]]} {
                break;
            }
        }
        assert {$iterations != 0}

        # PATH 2: positive count (unique behavior) with requested size
        # equal or greater than set size.
        foreach size {10 20} {
            set res [r exzrandmember myzset $size]
            assert_equal [llength $res] 10
            assert_equal [lsort $res] [lsort [dict keys $mydict]]
            check_member $mydict $res

            # again with WITHSCORES
            set res [r exzrandmember myzset $size withscores]
            assert_equal [llength $res] 20
            assert_equal [lsort $res] [lsort $mydict]
            check_member_and_score $mydict $res
        }

        # PATH 3: Ask almost as elements as there are in the set.
        # In this case the implementation will duplicate the original
        # set and will remove random elements up to the requested size.
        #
        # PATH 4: Ask a number of elements definitely smaller than
        # the set size.
        #
        # We can test both the code paths just changing the size but
        # using the same code.
        foreach size {1 2 8} {
            # 1) Check that all the elements actually belong to the
            # original set.
            set res [r exzrandmember myzset $size]
            assert_equal [llength $res] $size
            check_member $mydict $res

            # again with WITHSCORES
            set res [r exzrandmember myzset $size withscores]
            assert_equal [llength $res] [expr {$size * 2}]
            check_member_and_score $mydict $res

            # 2) Check that eventually all the elements are returned.
            #    Use both WITHSCORES and without
            unset -nocomplain auxset
            unset -nocomplain allkey
            set iterations [expr {1000 / $size}]
            set all_ele_return false
            while {$iterations != 0} {
                incr iterations -1
                if {[expr {$iterations % 2}] == 0} {
                    set res [r exzrandmember myzset $size withscores]
                    foreach {key value} $res {
                        dict append auxset $key $value
                        lappend allkey $key
                    }
                } else {
                    set res [r exzrandmember myzset $size]
                    foreach key $res {
                        dict append auxset $key
                        lappend allkey $key
                    }
                }
                if {[lsort [dict keys $mydict]] eq
                    [lsort [dict keys $auxset]]} {
                    set all_ele_return true
                }
            }
            assert_equal $all_ele_return true
            # df = 9, 40 means 0.00001 probability
            assert { [chi_square_value $allkey] < 40 }
        }
    }

    test "EXZSCAN basic" {
        set cur 0
        set keys {}
        create_big_tairzset zset 1000
        while 1 {
            set res [r exzscan zset $cur]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
        }
        assert_equal 2000 [llength $keys]
    }

    test "EXZSCAN COUNT" {
        set cur 0
        set keys {}
        create_big_tairzset zset 1000
        while 1 {
            set res [r exzscan zset $cur count 5]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
        }
        assert_equal 2000 [llength $keys]
    }

    test "EXZSCAN MATCH" {
        set cur 0
        set keys {}
        create_big_tairzset zset 1000
        while 1 {
            set res [r exzscan zset $cur match "1??"]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
        }
        assert_equal 200 [llength $keys]
    }

    test "EXZSCAN verify member and score" {
        # Create the Sorted Set
        set count 1000

        create_big_tairzset zset $count

        set cur 0
        set keys {}
        while 1 {
            set res [r exzscan zset $cur]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
        }

        set keys2 {}
        foreach {k v} $keys {
            assert {"$k#$k#$k" eq $v}
            lappend keys2 $k
        }

        assert_equal $count [llength $keys2]
    }

    # Random integer between 0 and max (excluded).
    proc randomInt {max} {
        expr {int(rand()*$max)}
    }

    test "EXZSCAN guarantees check under write load" {
        create_big_tairzset zset 100

        # We start scanning here, so keys from 0 to 99 should all be
        # reported at the end of the iteration.
        set keys {}
        set cur 0
        while 1 {
            set res [r exzscan zset $cur]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
            # Write 10 random keys at every EXZSCAN iteration.
            for {set j 0} {$j < 10} {incr j} {
                r set addedkey:[randomInt 1000] foo
            }
        }

        set keys2 {}
        foreach k $keys {
            # length of "99#99#99" is 9.
            if {[string length $k] > 9} continue
            lappend keys2 $k
        }

        assert_equal 200 [llength $keys2]
    }

    test "EXZSCAN with PATTERN" {
        r del mykey
        r exzadd mykey 1#1.1 foo 2#2.2 fab 3#3.3 fiz 10#9.9 foobar
        set res [r exzscan mykey 0 MATCH foo* COUNT 10000]
        set keys [lindex $res 1]
        assert_equal 4 [llength $keys]
    }

    test "EXZSCAN scores: regression test for issue #2175 in Redis" {
        r del mykey
        for {set j 0} {$j < 500} {incr j} {
            r exzadd mykey 9.8813129168249309e-323 $j
        }
        set res [lindex [r exzscan mykey 0] 1]
        set first_score [lindex $res 1]
        assert {$first_score != 0}
    }

    test "EXZUNIONSTORE against non-existing key doesn't set destination" {
        r del zseta
        assert_equal 0 [r exzunionstore dst_key 1 zseta]
        assert_equal 0 [r exists dst_key]
    }

    test "EXZUNION/EXZINTER/EXZINTERCARD/EXDIFF against non-existing key" {
        r del zseta
        assert_equal {} [r exzunion 1 zseta]
        assert_equal {} [r exzinter 1 zseta]
        assert_equal 0 [r exzintercard 1 zseta]
        assert_equal 0 [r exzintercard 1 zseta limit 0]
        assert_equal {} [r exzdiff 1 zseta]
    }

    test "EXZUNIONSTORE with empty set" {
        r del zseta zsetb
        r exzadd zseta 1#2 a
        r exzadd zseta 2#3 b
        r exzunionstore zsetc 2 zseta zsetb
        r exzrange zsetc 0 -1 withscores
    } {a 1#2 b 2#3}

    test "EXZUNION/EXZINTER/EXZINTERCARD/EXZDIFF with empty set" {
        r del zseta zsetb
        r exzadd zseta 1#2 a
        r exzadd zseta 2#3 b
        assert_equal {a 1#2 b 2#3} [r exzunion 2 zseta zsetb withscores]
        assert_equal {} [r exzinter 2 zseta zsetb withscores]
        assert_equal 0 [r exzintercard 2 zseta zsetb{t}]
        assert_equal 0 [r exzintercard 2 zseta zsetb limit 0]
        assert_equal {a 1#2 b 2#3} [r exzdiff 2 zseta zsetb withscores]
    }

    test "EXZUNIONSTORE basics" {
        r del zseta zsetb zsetc
        r exzadd zseta 1#2 a
        r exzadd zseta 2#3 b
        r exzadd zseta 3#4 c
        r exzadd zsetb 1#2 b
        r exzadd zsetb 2#3 c
        r exzadd zsetb 3#4 d

        assert_equal 4 [r exzunionstore zsetc 2 zseta zsetb]
        assert_equal {a 1#2 d 3#4 b 3#5 c 5#7} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZUNION/EXZINTER/EXZINTERCARD/EXZDIFF with integer members" {
        r del zsetd zsetf
        r exzadd zsetd 1#1 1
        r exzadd zsetd 2#2 2
        r exzadd zsetd 3#3 3
        r exzadd zsetf 1#1 1
        r exzadd zsetf 3#3 3
        r exzadd zsetf 4#4 4

        assert_equal {1 2#2 2 2#2 4 4#4 3 6#6} [r exzunion 2 zsetd zsetf withscores]
        assert_equal {1 2#2 3 6#6} [r exzinter 2 zsetd zsetf withscores]
        assert_equal 2 [r exzintercard 2 zsetd zsetf]
        assert_equal 2 [r exzintercard 2 zsetd zsetf limit 0]
        assert_equal {2 2#2} [r exzdiff 2 zsetd zsetf withscores]
    }

    test "EXZUNIONSTORE with weights" {
        assert_equal 4 [r exzunionstore zsetc 2 zseta zsetb weights 2 3]
        assert_equal {a 2#4 b 7#12 d 9#12 c 12#17} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZUNION/EXZINTER with weights" {
        assert_equal {a 2#4 b 7#12 d 9#12 c 12#17} [r exzunion 2 zseta zsetb weights 2 3 withscores]
        assert_equal {b 7#12 c 12#17} [r exzinter 2 zseta zsetb weights 2 3 withscores]
    }

    test "EXZUNIONSTORE with AGGREGATE MIN" {
        assert_equal 4 [r exzunionstore zsetc 2 zseta zsetb aggregate min]
        assert_equal {a 1#2 b 1#2 c 2#3 d 3#4} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZUNION/EXZINTER with AGGREGATE MIN" {
        assert_equal {a 1#2 b 1#2 c 2#3 d 3#4} [r exzunion 2 zseta zsetb aggregate min withscores]
        assert_equal {b 1#2 c 2#3} [r exzinter 2 zseta zsetb aggregate min withscores]
    }

    test "EXZUNIONSTORE with AGGREGATE MAX" {
        assert_equal 4 [r exzunionstore zsetc 2 zseta zsetb aggregate max]
        assert_equal {a 1#2 b 2#3 c 3#4 d 3#4} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZUNION/EXZINTER with AGGREGATE MAX" {
        assert_equal {a 1#2 b 2#3 c 3#4 d 3#4} [r exzunion 2 zseta zsetb aggregate max withscores]
        assert_equal {b 2#3 c 3#4} [r exzinter 2 zseta zsetb aggregate max withscores]
    }

    test "EXZINTERSTORE basics" {
        assert_equal 2 [r exzinterstore zsetc 2 zseta zsetb]
        assert_equal {b 3#5 c 5#7} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZINTER basics" {
        assert_equal {b 3#5 c 5#7} [r exzinter 2 zseta zsetb withscores]
    }

    test "EXZINTERSTORE with weights" {
        assert_equal 2 [r exzinterstore zsetc 2 zseta zsetb weights 2 3]
        assert_equal {b 7#12 c 12#17} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZINTER with weights" {
        assert_equal {b 7#12 c 12#17} [r exzinter 2 zseta zsetb weights 2 3 withscores]
    }

    test "EXZINTERSTORE with AGGREGATE MIN" {
        assert_equal 2 [r exzinterstore zsetc 2 zseta zsetb aggregate min]
        assert_equal {b 1#2 c 2#3} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZINTERSTORE with AGGREGATE MAX" {
        assert_equal 2 [r exzinterstore zsetc 2 zseta zsetb aggregate max]
        assert_equal {b 2#3 c 3#4} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZINTERCARD with illegal arguments" {
        assert_error "ERR syntax error*" {r exzintercard 1 zseta zseta}
        assert_error "ERR syntax error*" {r exzintercard 1 zseta bar_arg}
        assert_error "ERR syntax error*" {r exzintercard 1 zseta LIMIT}

        assert_error "ERR LIMIT*" {r exzintercard 1 myset LIMIT -1}
        assert_error "ERR LIMIT*" {r exzintercard 1 myset LIMIT a}
    }

    test "ZINTERCARD basics" {
        assert_equal 2 [r exzintercard 2 zseta zsetb]
        assert_equal 2 [r exzintercard 2 zseta zsetb limit 0]
        assert_equal 1 [r exzintercard 2 zseta zsetb limit 1]
        assert_equal 2 [r exzintercard 2 zseta zsetb limit 10]
    }

    foreach cmd {EXZUNIONSTORE EXZINTERSTORE} {
        test "$cmd with +inf/-inf scores" {
            r del zsetinf1 zsetinf2

            r exzadd zsetinf1 +inf#1 key
            r exzadd zsetinf2 +inf#2 key
            r $cmd zsetinf3 2 zsetinf1 zsetinf2
            assert_equal inf#3 [r exzscore zsetinf3 key]

            r exzadd zsetinf1 -inf#1 key
            r exzadd zsetinf2 +inf#2 key
            r $cmd zsetinf3 2 zsetinf1 zsetinf2
            assert_equal 0#3 [r exzscore zsetinf3 key]

            r exzadd zsetinf1 +inf#1 key
            r exzadd zsetinf2 -inf#2 key
            r $cmd zsetinf3 2 zsetinf1 zsetinf2
            assert_equal 0#3 [r exzscore zsetinf3 key]

            r exzadd zsetinf1 -inf#1 key
            r exzadd zsetinf2 -inf#2 key
            r $cmd zsetinf3 2 zsetinf1 zsetinf2
            assert_equal -inf#3 [r exzscore zsetinf3 key]
        }

        test "$cmd with NaN weights" {
            r del zsetinf1 zsetinf2

            r exzadd zsetinf1 1.0 key
            r exzadd zsetinf2 1.0 key
            assert_error "*weight*not*float*" {
                r $cmd zsetinf3 2 zsetinf1 zsetinf2 weights nan nan
            }
        }
    }

    test {EXZUNIONSTORE regression, should not create NaN in scores} {
        r exzadd z -inf neginf
        r exzunionstore out 1 z weights 0 
        r exzrange out 0 -1 withscores
    } {neginf 0}

    test {EXZUNIONSTORE result is sorted} {
        # Create two sets with common and not common elements, perform
        # the UNION, check that elements are still sorted.
        r del one two dest
        set cmd1 [list r exzadd one]
        set cmd2 [list r exzadd two]
        for {set j 0} {$j < 1000} {incr j} {
            lappend cmd1 [expr rand()] [randomInt 1000]
            lappend cmd2 [expr rand()] [randomInt 1000]
        }
        {*}$cmd1
        {*}$cmd2
        assert {[r exzcard one] > 100}
        assert {[r exzcard two] > 100}
        r exzunionstore dest 2 one two
        set oldscore 0
        foreach {ele score} [r exzrange dest 0 -1 withscores] {
            assert {$score >= $oldscore}
            set oldscore $score
        }
    }

    test "EXZDIFFSTORE basics" {
        assert_equal 1 [r exzdiffstore zsetc 2 zseta zsetb]
        assert_equal {a 1#2} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZDIFF basics" {
        assert_equal {a 1#2} [r exzdiff 2 zseta zsetb withscores]
    }

    test "EXZDIFF subtracting set from itself" {
        assert_equal 0 [r exzdiffstore zsetc 2 zseta zseta]
        assert_equal {} [r exzrange zsetc 0 -1 withscores]
    }

    test "EXZDIFF algorithm 1" {
        r del zseta zsetb zsetc
        r exzadd zseta 1#2 a
        r exzadd zseta 2#3 b
        r exzadd zseta 3#4 c
        r exzadd zsetb 1#3 b
        r exzadd zsetb 2#4 c
        r exzadd zsetb 3#5 d
        assert_equal 1 [r exzdiffstore zsetc 2 zseta zsetb]
        assert_equal {a 1#2} [r exzrange zsetc 0 -1 withscores]
    }

    test "ZDIFF algorithm 2" {
        r del zseta zsetb zsetc zsetd zsete
        r exzadd zseta 1#2 a
        r exzadd zseta 2#3 b
        r exzadd zseta 3#4 c
        r exzadd zseta 5#6 e
        r exzadd zsetb 1#1 b
        r exzadd zsetc 1#1 c
        r exzadd zsetd 1#1 d
        assert_equal 2 [r exzdiffstore zsete 4 zseta zsetb zsetc zsetd]
        assert_equal {a 1#2 e 5#6} [r exzrange zsete 0 -1 withscores]
    }

    test "EXZDIFF fuzzing" {
        for {set j 0} {$j < 100} {incr j} {
            unset -nocomplain s
            array set s {}
            set args {}
            set num_sets [expr {[randomInt 10]+1}]
            for {set i 0} {$i < $num_sets} {incr i} {
                set num_elements [randomInt 100]
                r del zset_$i
                lappend args zset_$i
                while {$num_elements} {
                    set ele [randomValue]
                    r exzadd zset_$i [randomInt 100] $ele
                    if {$i == 0} {
                        set s($ele) x
                    } else {
                        unset -nocomplain s($ele)
                    }
                    incr num_elements -1
                }
            }
            set result [lsort [r exzdiff [llength $args] {*}$args]]
            assert_equal $result [lsort [array names s]]
        }
    }

    test "EXZUNIONSTORE/EXINTERSTORE/EXZDIFFSTORE error if using WITHSCORES " {
        assert_error "*ERR*syntax*" {r exzunionstore foo 2 zsetd zsetf withscores}
        assert_error "*ERR*syntax*" {r exzinterstore foo 2 zsetd zsetf withscores}
        assert_error "*ERR*syntax*" {r exzdiffstore foo 2 zsetd zsetf withscores}
    }

    test "exZunionInterDiffGenericCommand at least 1 input key" {
        assert_error {*ERR*at least 1 input key*} {r exzunion 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzunionstore dst_key 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzinter 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzinterstore dst_key 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzdiff 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzdiffstore dst_key 0 key}
        assert_error {*ERR*at least 1 input key*} {r exzintercard 0 key}
    }

    test "Basic EXZPOP with a single key" {
        r del zset
        assert_equal {} [r exzpopmin zset]
        create_tairzset zset {-1#1 a 2#1 b 2#2 c 3#3 d 4#4 e}
        assert_equal {a -1#1} [r exzpopmin zset]
        assert_equal {b 2#1} [r exzpopmin zset]
        assert_equal {e 4#4} [r exzpopmax zset]
        assert_equal {d 3#3} [r exzpopmax zset]
        assert_equal {c 2#2} [r exzpopmin zset]
        assert_equal 0 [r exists zset]
        r set foo bar
        assert_error "*WRONGTYPE*" {r exzpopmin foo}
    }

    test "EXZPOP with count" {
        r del z1 z2 z3 foo
        r set foo bar
        assert_equal {} [r exzpopmin z1 2]
        assert_error "*WRONGTYPE*" {r exzpopmin foo 2}
        create_tairzset z1 {0#0 a 1#1 b 2#2 c 3#3 d}
        assert_equal {a 0#0 b 1#1} [r exzpopmin z1 2]
        assert_equal {d 3#3 c 2#2} [r exzpopmax z1 2]
    }
}

start_server {tags {"repl test"} overrides {bind 0.0.0.0}} {
    r module load $testmodule
    set slave [srv 0 client]
    set slave_host [srv 0 host]
    set slave_port [srv 0 port]

    start_server {overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        # Start the replication process...
        $slave slaveof $master_host $master_port

        test {Slave enters handshake} {
            wait_for_condition 50 1000 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }

        test "EXZADD/EXZINCRBY/EXREM repl" {
            $master del repltest
            
            assert {[$master exzadd repltest 0 m0 1 m1 2 m2] == 3}
            assert_equal {m0 m1 m2} [$master exzrange repltest 0 -1]
            $master WAIT 1 5000
            assert_equal {m0 m1 m2} [$slave exzrange repltest 0 -1]

            assert_equal {2} [$master exzincrby repltest 2 m0]
            $master WAIT 1 5000
            assert_equal {m1 m0 m2} [$slave exzrange repltest 0 -1]

            assert_equal 1 [$master exzrem repltest m0]
            $master WAIT 1 5000
            assert_equal {m1 m2} [$slave exzrange repltest 0 -1]
        }

        test "DIGEST test" {
            assert_equal [$master debug digest] [$slave debug digest]
        }

        test "MEMORY usage test" {
            assert_equal [$master memory usage repltest] [$slave memory usage repltest]
        }
    }
}
