set testmodule [file normalize your_path/tairzset_module.so]

proc redis_deferring_client {args} {
    set level 0
    if {[llength $args] > 0 && [string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }

    # create client that defers reading reply
    set client [redis [srv $level "host"] [srv $level "port"] 1 $::tls]

    # select the right db and read the response (OK)
    $client select 9
    $client read
    return $client
}

start_server {tags {"tairzset"} overrides {bind 0.0.0.0}} {
    r module load $testmodule

    proc create_tairzset {key items} {
        r del $key
        foreach {score entry} $items {
            r exzadd $key $score $entry
        }
    }

    test "EXBZPOP with a single existing sorted set" {
        set rd [redis_deferring_client]
        create_tairzset zset {0 a 1 b 2 c}

        $rd exbzpopmin zset 5
        assert_equal {zset a 0} [$rd read]
        $rd exbzpopmin zset 5
        assert_equal {zset b 1} [$rd read]
        $rd exbzpopmax zset 5
        assert_equal {zset c 2} [$rd read]
        assert_equal 0 [r exists zset]
    }

    test "EXBZPOP with multiple existing sorted sets" {
        set rd [redis_deferring_client]
        create_tairzset z1 {0 a 1 b 2 c}
        create_tairzset z2 {3 d 4 e 5 f}

        $rd exbzpopmin z1 z2 5
        assert_equal {z1 a 0} [$rd read]
        $rd exbzpopmax z1 z2 5
        assert_equal {z1 c 2} [$rd read]
        assert_equal 1 [r exzcard z1]
        assert_equal 3 [r exzcard z2]

        $rd exbzpopmax z2 z1 5
        assert_equal {z2 f 5} [$rd read]
        $rd exbzpopmin z2 z1 5
        assert_equal {z2 d 3} [$rd read]
        assert_equal 1 [r exzcard z1]
        assert_equal 1 [r exzcard z2]
    }

    test "EXBZPOP second sorted set has members" {
        set rd [redis_deferring_client]
        r del z1
        create_tairzset z2 {3 d 4 e 5 f}
        $rd exbzpopmax z1 z2 5
        assert_equal {z2 f 5} [$rd read]
        $rd exbzpopmin z2 z1 5
        assert_equal {z2 d 3} [$rd read]
        assert_equal 0 [r exzcard z1]
        assert_equal 1 [r exzcard z2]
    }

    test "EXBZPOPMIN, ZADD + DEL should not awake blocked client" {
        set rd [redis_deferring_client]
        r del zset

        $rd exbzpopmin zset 0
        r multi
        r exzadd zset 0 foo
        r del zset
        r exec
        r del zset
        r exzadd zset 1 bar
        $rd read
    } {zset bar 1}

    test "EXBZPOPMIN, ZADD + DEL + SET should not awake blocked client" {
        set rd [redis_deferring_client]
        r del zset

        $rd exbzpopmin zset 0
        r multi
        r exzadd zset 0 foo
        r del zset
        r set zset foo
        r exec
        r del zset
        r exzadd zset 1 bar
        $rd read
    } {zset bar 1}

    test "EXBZPOPMIN with same key multiple times should work" {
        set rd [redis_deferring_client]
        r del z1 z2

        # Data arriving after the BZPOPMIN.
        $rd exbzpopmin z1 z2 z2 z1 0
        r exzadd z1 0 a
        assert_equal [$rd read] {z1 a 0}
        $rd exbzpopmin z1 z2 z2 z1 0
        r exzadd z2 1 b
        assert_equal [$rd read] {z2 b 1}

        # Data already there.
        r exzadd z1 0 a
        r exzadd z2 1 b
        $rd exbzpopmin z1 z2 z2 z1 0
        assert_equal [$rd read] {z1 a 0}
        $rd exbzpopmin z1 z2 z2 z1 0
        assert_equal [$rd read] {z2 b 1}
    }

    test "MULTI/EXEC is isolated from the point of view of EXBZPOPMIN" {
        set rd [redis_deferring_client]
        r del zset
        $rd exbzpopmin zset 0
        r multi
        r exzadd zset 0 a
        r exzadd zset 1 b
        r exzadd zset 2 c
        r exec
        $rd read
    } {zset a 0}

    test "EXBZPOPMIN with variadic EXZADD" {
        set rd [redis_deferring_client]
        r del zset
        if {$::valgrind} {after 100}
        $rd exbzpopmin zset 0
        if {$::valgrind} {after 100}
        assert_equal 2 [r exzadd zset -1 foo 1 bar]
        if {$::valgrind} {after 100}
        assert_equal {zset foo -1} [$rd read]
        assert_equal {bar} [r exzrange zset 0 -1]
    }

    test "EXBZPOPMIN with zero timeout should block indefinitely" {
        set rd [redis_deferring_client]
        r del zset
        $rd exbzpopmin zset 0
        after 1000
        r exzadd zset 0 foo
        assert_equal {zset foo 0} [$rd read]
    }
}