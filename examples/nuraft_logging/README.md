./examples/nuraft_logging 1 localhost:10001

./examples/nuraft_logging 2 localhost:10002

./examples/nuraft_logging 3 localhost:10003

example commands:
add 2 localhost:10002
list
put a b 1
put a c 2
st
get a 1
get a 2
csn
del a
st

To build correctly as a standalone (not in kepler):
1. run git submodule update --init --recursive
2. cd into rocksdb
3. run `make static_lib`
4. cd into NuRaft
5. mkdir build
6. cmake ..
7. make -j8

If NuRaft is replaying logs and you eant to reset go to build/logs1, build/logs2, etc. and delete them