Elmdb, an Erlang NIF for LMDB
======================

[![Build Status](https://api.travis-ci.org/zambal/elmdb.svg?branch=master)](https://travis-ci.org/zambal/elmdb)

This is an Erlang NIF for OpenLDAP's Lightning Memory-Mapped Database (LMDB) database library. LMDB is an fast, compact key-value data store developed by Symas for the OpenLDAP Project. It uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases, and is only limited to the size of the virtual address space, (it is not limited to the size of physical RAM). LMDB was originally called MDB, but was renamed to avoid confusion with other software associated with the name MDB. See the [LMDB docs](http://lmdb.tech/doc/) for more information about LMDB itself.

As far as I know, two other Erlang NIF libraries exist for LMDB: [alepharchives/emdb](https://github.com/alepharchives/emdb) and [gburd/lmdb](https://github.com/gburd/lmdb). Reasons for developing another wrapper are that both libraries only offer a small subset of LMDB's functionality and their architecture prevents them from offering more of LMDB's functionality. The reason for this is that all read and writes in LMDB are wrapped in transactions and that all operations in a write transaction need to be performed on the same thread.

[alepharchives/emdb](https://github.com/alepharchives/emdb) performs all operations on the calling thread and since Erlang's scheduler might move a process to a different scheduler/thread at any time, it can not be guaranteed that all operations in a transaction will be performed on the same thread.

[gburd/lmdb](https://github.com/gburd/lmdb) uses an asynchronous thread pool, developed by Gregory Burd at Basho Technologies. Having a thread pool means that every operation might be scheduled to a different thread from the pool, which starts to be a problem for LMDB when executing transactions with multiple operations.

Elmdb solves this by creating one background thread per LMDB environment and serializing all write transactions to this thread. This means that Erlang applications can safely perform write transactions concurrently, with the only caveat that as long as a write transaction is active, it will block all other write transactions. However, LMDB itself enforces the same limit by serializing write transactions internally too, so the performance degredation of serializing all transaction operations to a single thread is fairly small and since the calls are asynchronous, the Erlang scheduler isn't blocked.

Quick Overview
--------------

Elmdb strives to provide a robust and eventually complete wrapper for LMDB. Elmdb currently supports:

 * Opening multiple databases within an environment (do not forget to set the max_dbs option in elmdb:env_open/2)
 * Safe use of read/write transactions, by serializing all operations within a transaction to a dedicated thread
 * Read-only transactions, which are always performed on the calling thread. This results in great performance, since multiple read-only transactions may be executed concurrently and do not block each other
 * Basic support for cursors (all operations for databases opened with MDB_DUPSORT still need to be implemented)
 * Convenience functions for all basic operations like `put`, `get`, `delete`, `drop`
 * Build against LMDB 0.9.18

Requirements
------------
Minimal requirement to be determined. Currently only build and tested on:

* Ubuntu 16.04 / GCC 5.3.1 / Erlang OTP-18, Erlang OTP-19
* FreeBSD 10.2 / GCC 4.8.5, Clang 3.4.1 / Erlang OTP-19

Oldest usable OTP version is probably OTP_R14B, but needs to be tested.

Should be able to run on Windows, MacOS and older/different Linux distributions too, but I haven't tried yet.


Build
-----

$ make

Usage
-----

```
$ make
$ ./start.sh
%% create a new environment
1> {ok, Env} = elmdb:env_open("/tmp/lmdb1", []).

%% create the default database of the environment
2> {ok, Dbi} = elmdb:db_open(Env, [create]).

%% insert the key <<"a">> with value <<"1">>
3> ok = elmdb:put(Dbi, <<"a">>, <<"1">>).

%% try to re-insert the same key <<"a">>
4> exists = elmdb:put_new(Dbi, <<"a">>, <<"2">>).

%% re-insert the same key <<"a">>
5> ok = elmdb:put(Dbi, <<"a">>, <<"2">>).

%% add new key-value pairs
6> ok = elmdb:put_new(Dbi, <<"b">>, <<"2">>).
7> ok = elmdb:put_new(Dbi, <<"d">>, <<"4">>).

%% search a non-existing key <<"c">>
8> not_found = elmdb:get(Dbi, <<"c">>).

%% retrieve the value for key <<"b">>
9> {ok, <<"2">>} = elmdb:get(Dbi, <<"b">>).

%% delete key <<"b">>
10> ok = elmdb:delete(Dbi, <<"b">>).

%% search a non-existing key <<"b">>
11> not_found = elmdb:get(Dbi, <<"b">>).

%% delete a non-existing key <<"z">>
12> not_found = elmdb:delete(Dbi, <<"z">>).

%% begin a read-only transaction
13> {ok, Txn} = elmdb:ro_txn_begin(Env).

%% open a cursor
14> {ok, Cur} = elmdb:ro_txn_cursor_open(Txn, Dbi).

%% get the first key/value pair
15> {ok, <<"a">>, <<"2">>} = elmdb:ro_txn_cursor_get(Cur, first).

%% get the next key/value pair
16> {ok, <<"d">>, <<"4">>} = elmdb:ro_txn_cursor_get(Cur, next).

%% try to get the next key/value pair
17> not_found = elmdb:ro_txn_cursor_get(Cur, next).

%% get the last key/value pair
18> {ok, <<"d">>, <<"4">>} = elmdb:ro_txn_cursor_get(Cur, last).

%% find the first key/value pair with a key equal or greater than the specified key.
19> {ok, <<"d">>, <<"4">>} = elmdb:ro_txn_cursor_get(Cur, {set_range, <<"c">>}).

%% close cursor
20> ok = elmdb:ro_txn_cursor_close(Cur).

%% commit transaction
21> ok = elmdb:ro_txn_commit(Txn).

%% swap two values atomically with a read/write transaction
22> {ok, Txn2} = elmdb:txn_begin(Env).
23> {ok, A} = elmdb:txn_get(Txn2, Dbi, <<"a">>).
24> {ok, D} = elmdb:txn_get(Txn2, Dbi, <<"d">>).
25> ok = elmdb:txn_put(Txn2, Dbi, <<"a">>, D).
26> ok = elmdb:txn_put(Txn2, Dbi, <<"d">>, A).
27> ok = elmdb:txn_commit(Txn2).

%% delete all key-value pairs in the database
28> ok = elmdb:drop(Env).

%% try to retrieve key <<"a">> value
29> not_found = elmdb:get(Env, <<"a">>).

%% close the environment
30> ok = elmdb:env_close(Env).
```


Design and implementation notes
-------------------------------

When an environment is opened, `elmdb` will create a background thread for handling read/write transactions. All elmdb functions starting with `txn_`, `async_` and `update_` make use of this thread. The functions push their operation to a FIFO queue and the background thread will pop operations from this queue when it can. The background thread sends a message back to the calling process with the results of the operations. This design is inspired by Gregory Burd's `async_nif.h`, used in [gburd/lmdb](https://github.com/gburd/lmdb).

All objects/handles from LMDB are allocated as NIF resources, meaning that they are reference counted and garbage collected by the Erlang run-time. If a resource has a reference to another resource, the ref counter is increased. When for example an environment resource goes out of scope, but a db from that environment is still in scope, the environment won't be closed and garbage collected as long as the db resource is still available. Here's an overview of the lifetime behaviour of resources:

* Environments - Will be closed and its background thread ended by the resource destructor before collection if it's still open. Will stay alive as long as its resource is in scope or any other resource opened within (Db, transaction, or cursor)
* Databases - Databases are never closed expicitly, or by its resource destructor, but only implicitly when their environment closes. Contains a reference to its environment.
* Transactions - Both read/write and read-only transactions will be aborted by the resource destructor before collection if still open. Contains a reference to the environment it was opened in.
* Cursors - read-only cursors will be closed by the resource destructor before collection. Read/write cursors are implicitly closed by LMDB when its transaction ends. Contains a reference to the transaction it was opened in.


TODO
----

* Documentation
* Complete cursor operations for MDB_DUPSORT databases
* More unit tests
* Bulk writing
* Implement wrapper for mnesia_ex plugin
* Provide more utility functions for stats and (re)configuration


Status
------

Work in progress, but the code should be pretty stable, as it is already about to be used as a test case in production for a non critical project.


Acknowledgements
----------------

This project started as a fork of [gburd/lmdb](https://github.com/gburd/lmdb), but ended up as an almost complete rewrite. However, there are still parts in `elmdb` that originated from that project, like async nif design, utility functions, Makefile, rebar configuration, structure of this readme, etc.


LICENSE
-------

Elmdb is Copyright (C) 2012 by Aleph Archives, (C) 2013 by Basho Technologies, (C) 2016 by Vincent Siliakus and released under the [OpenLDAP](http://www.OpenLDAP.org/license.html) License.
