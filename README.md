# TairZset: Support multi-score sorting zset
## Introduction  [Chinese](README-CN.md)
     TairZset is a data structure developed based on the redis module. Compared with the native zset data structure of redis, TairZset not only has the same rich data interface and high performance as the native zset, but also provides (arbitrary) multi-score sorting capabilities.

### Features：

- Support (arbitrary) multi-score sorting, and the accuracy of any dimension is not lost
- Incrby semantics is still supported under multi-score sorting
- The syntax is similar to redis zset

### Application scenario：
- Sorting among gamers
- Anchor popularity ranking in live room

## Quick start
```
127.0.0.1:6379> exzadd tairzsetkey 1.1 x 2.2 y
(integer) 2
127.0.0.1:6379> exzrange tairzsetkey 0 -1 withscores
1) "x"
2) "1.1000000000000001"
3) "y"
4) "2.2000000000000002"
127.0.0.1:6379> exzincrby tairzsetkey 2 x 
"3.1000000000000001"
127.0.0.1:6379> exzrange tairzsetkey 0 -1 withscores
1) "y"
2) "2.2000000000000002"
3) "x"
4) "3.1000000000000001"
127.0.0.1:6379> exzadd tairzsetkey 3.3#3.3 z
(error) ERR score is not a valid format
127.0.0.1:6379> del tairzsetkey
(integer) 1
127.0.0.1:6379> exzadd tairzsetkey 1.1#3.3 x 2.2#2.2 y 3.3#1.1 z
(integer) 3
127.0.0.1:6379> exzrange tairzsetkey 0 -1 withscores
1) "x"
2) "1.1000000000000001#3.2999999999999998"
3) "y"
4) "2.2000000000000002#2.2000000000000002"
5) "z"
6) "3.2999999999999998#1.1000000000000001"
127.0.0.1:6379> exzincrby tairzsetkey 2 y 
(error) ERR score is not a valid format
127.0.0.1:6379> exzincrby tairzsetkey 2#0 y 
"4.2000000000000002#2.2000000000000002"
127.0.0.1:6379> exzrange tairzsetkey 0 -1 withscores
1) "x"
2) "1.1000000000000001#3.2999999999999998"
3) "z"
4) "3.2999999999999998#1.1000000000000001"
5) "y"
6) "4.2000000000000002#2.2000000000000002"
```
## Build

```
mkdir build  
cd build  
cmake ../ && make -j
```
then the tairzset_module.so library file will be generated in the lib directory

## Test
1. Modify the path in the tairzset.tcl file in the `tests` directory to `set testmodule [file your_path/tairzset_module.so]`
2. Add the path of the tairzset.tcl file in the `tests` directory to the all_tests of redis test_helper.tcl
3. run ./runtest --single tairzset

## Client
### Java : https://github.com/aliyun/alibabacloud-tairjedis-sdk

## API
[Ref](CMDDOC.md)

## Roadmap
- [x] support multi scores
- [ ] support EXZUNIONSTORE、EXZUNION、EXZSCAN、EXZDIFF、EXZDIFFSTORE、EXZINTER、EXZINTERSTORE、EXZRANDMEMBER、EXZMSCORE  


## Applicable redis version   
redis 5.x 、redis 6.x