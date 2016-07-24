-module(elmdb_tests).
-include_lib("eunit/include/eunit.hrl").

close_env_test() ->
    Name = "test/testdb",
    {ok, Env} = elmdb:env_open(Name, []),
    io:format("closing env~n"),
    ?assertMatch(ok, elmdb:env_close(Env)),
    io:format("try opening db~n"),
    ?assertMatch({error, env_closed}, elmdb:db_open(Env, [])),
    io:format("delete env~n"),
    delete_env(Name).

close_env_by_name_test() ->
    Name = "test/testdb",
    {ok, Env} = elmdb:env_open(Name, []),
    ?assertMatch(ok, elmdb:env_close_by_name(Name)),
    delete_env(Name),
    % Make sure Env exists until closing it.
    Env.

close_all_env_test() ->
    Name1 = "test/testdb1",
    Name2 = "test/testdb2",
    {ok, Env1} = elmdb:env_open(Name1, []),
    {ok, Env2} = elmdb:env_open(Name2, []),
    ?assertMatch(ok, elmdb:env_close_all()),
    ?assertMatch({error, env_closed}, elmdb:db_open(Env1, [])),
    ?assertMatch({error, env_closed}, elmdb:db_open(Env2, [])),
    delete_env(Name1),
    delete_env(Name2),
    % Make sure Envs exists until closing it.
    {Env1, Env2}.

open_env_badarg_test() ->
    ?assertError(badarg, elmdb:env_open("test/testdb", [123])).

open_db_badarg_test() ->
    Name = "test/testdb",
    {ok, Env} = elmdb:env_open(Name, []),
    ?assertError(badarg, elmdb:db_open(Env, 1234)),
    ?assertError(badarg, elmdb:db_open(1234, [])),
    ?assertMatch(ok, elmdb:env_close(Env)),
    delete_env(Name).

basic_test_() ->
    Tests = [
      fun sync_put_get/1,
      fun async_put_get/1,
      fun put_new/1,
      fun delete/1,
      fun drop/1,
      fun update/1,
      fun interfere_update/1,
      fun ro_txn_get/1,
      fun ro_txn_cursor/1,
      fun txn_put_get/1,
      fun txn_cursor/1
    ],
    {foreach, fun basic_setup/0, fun basic_teardown/1, Tests}.

sync_put_get({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"c">>, <<"3">>)),
            ?assertMatch({ok, <<"1">>}, elmdb:get(Dbi, <<"a">>)),
            ?assertMatch({ok, <<"2">>}, elmdb:get(Dbi, <<"b">>)),
            ?assertMatch({ok, <<"3">>}, elmdb:get(Dbi, <<"c">>)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"d">>))
    end.

async_put_get({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:async_put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:async_put(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:async_put(Dbi, <<"c">>, <<"3">>)),
            ?assertMatch({ok, <<"1">>}, elmdb:async_get(Dbi, <<"a">>)),
            ?assertMatch({ok, <<"2">>}, elmdb:async_get(Dbi, <<"b">>)),
            ?assertMatch({ok, <<"3">>}, elmdb:async_get(Dbi, <<"c">>)),
            ?assertMatch(not_found, elmdb:async_get(Dbi, <<"d">>))
    end.

put_new({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put_new(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:put_new(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(exists, elmdb:put_new(Dbi, <<"b">>, <<"3">>)),
            ?assertMatch({ok, <<"2">>}, elmdb:get(Dbi, <<"b">>))
    end.

delete({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch({ok, <<"1">>}, elmdb:get(Dbi, <<"a">>)),
            ?assertMatch(ok, elmdb:delete(Dbi, <<"a">>)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"a">>))
    end.

drop({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"c">>, <<"3">>)),
            ?assertMatch(ok, elmdb:drop(Dbi)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"a">>)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"b">>)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"c">>))
    end.

update({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            {ok, <<"1">>, Txn} = elmdb:update_get(Dbi, <<"a">>),
            ?assertMatch(ok, elmdb:update_put(Txn, Dbi, <<"a">>, <<"2">>)),
            ?assertMatch({ok, <<"2">>}, elmdb:get(Dbi, <<"a">>))
    end.

interfere_update({_, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            {ok, <<"1">>, Txn} = elmdb:update_get(Dbi, <<"a">>),
            ?assertMatch({error, timeout}, elmdb:async_put(Dbi, <<"a">>, <<"3">>, 1000)),
            ?assertMatch(ok, elmdb:update_put(Txn, Dbi, <<"a">>, <<"2">>)),
            ?assertMatch({ok, <<"3">>}, elmdb:async_get(Dbi, <<"a">>))
    end.

ro_txn_get({Env, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"c">>, <<"3">>)),
            {ok, Txn} = elmdb:ro_txn_begin(Env),
            ?assertMatch({ok, <<"1">>}, elmdb:ro_txn_get(Txn, Dbi, <<"a">>)),
            ?assertMatch({ok, <<"2">>}, elmdb:ro_txn_get(Txn, Dbi, <<"b">>)),
            ?assertMatch(ok, elmdb:ro_txn_commit(Txn)),
            ?assertMatch({error, txn_closed}, elmdb:ro_txn_get(Txn, Dbi, <<"c">>)),
            ?assertMatch({error, txn_closed}, elmdb:ro_txn_commit(Txn)),
            ?assertMatch(ok, elmdb:ro_txn_abort(Txn)),
            ?assertMatch(ok, elmdb:ro_txn_abort(Txn))
    end.

ro_txn_cursor({Env, Dbi, _}) ->
    fun() ->
            ?assertMatch(ok, elmdb:put(Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:put(Dbi, <<"d">>, <<"4">>)),
            {ok, Txn} = elmdb:ro_txn_begin(Env),
            {ok, Cur} = elmdb:ro_txn_cursor_open(Txn, Dbi),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:ro_txn_cursor_get(Cur, first)),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:ro_txn_cursor_get(Cur, last)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:ro_txn_cursor_get(Cur, prev)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:ro_txn_cursor_get(Cur, prev)),
            ?assertMatch(not_found, elmdb:ro_txn_cursor_get(Cur, prev)),
            ?assertMatch(not_found, elmdb:ro_txn_cursor_get(Cur, prev)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:ro_txn_cursor_get(Cur, first)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:ro_txn_cursor_get(Cur, get_current)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:ro_txn_cursor_get(Cur, next)),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:ro_txn_cursor_get(Cur, next)),
            ?assertMatch(not_found, elmdb:ro_txn_cursor_get(Cur, next)),
            ?assertMatch(not_found, elmdb:ro_txn_cursor_get(Cur, next)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:ro_txn_cursor_get(Cur, {set, <<"b">>})),
            ?assertMatch(not_found, elmdb:ro_txn_cursor_get(Cur, {set, <<"c">>})),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:ro_txn_cursor_get(Cur, {set_range, <<"c">>}))
    end.

txn_put_get({Env, Dbi, _}) ->
    fun() ->
            {ok, Txn1} = elmdb:txn_begin(Env),
            ?assertMatch(ok, elmdb:txn_put(Txn1, Dbi, <<"a">>, <<"1">>)),
            ?assertMatch({ok, <<"1">>}, elmdb:txn_get(Txn1, Dbi, <<"a">>)),
            ?assertMatch(ok, elmdb:txn_abort(Txn1)),
            ?assertMatch(not_found, elmdb:get(Dbi, <<"a">>)),
            {ok, Txn2} = elmdb:txn_begin(Env),
            ?assertError(badarg, elmdb:txn_put(Txn1, Dbi, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:txn_put(Txn2, Dbi, <<"a">>, <<"1">>)),
            ?assertMatch({ok, <<"1">>}, elmdb:txn_get(Txn2, Dbi, <<"a">>)),
            ?assertMatch(ok, elmdb:txn_commit(Txn2)),
            ?assertMatch({ok, <<"1">>}, elmdb:get(Dbi, <<"a">>)),
            ?assertError(badarg, elmdb:txn_get(Txn2, Dbi, <<"a">>)),
            ?assertError(badarg, elmdb:txn_commit(Txn2)),
            ?assertMatch(ok, elmdb:txn_abort(Txn2))
    end.

txn_cursor({Env, Dbi, _}) ->
    fun() ->
            {ok, Txn} = elmdb:txn_begin(Env),
            {ok, Cur} = elmdb:txn_cursor_open(Txn, Dbi),
            ?assertMatch(ok, elmdb:txn_cursor_put(Cur, <<"a">>, <<"1">>)),
            ?assertMatch(ok, elmdb:txn_cursor_put(Cur, <<"b">>, <<"2">>)),
            ?assertMatch(ok, elmdb:txn_cursor_put(Cur, <<"d">>, <<"4">>)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:txn_cursor_get(Cur, first)),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:txn_cursor_get(Cur, last)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:txn_cursor_get(Cur, prev)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:txn_cursor_get(Cur, prev)),
            ?assertMatch(not_found, elmdb:txn_cursor_get(Cur, prev)),
            ?assertMatch(not_found, elmdb:txn_cursor_get(Cur, prev)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:txn_cursor_get(Cur, first)),
            ?assertMatch({ok, <<"a">>, <<"1">>}, elmdb:txn_cursor_get(Cur, get_current)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:txn_cursor_get(Cur, next)),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:txn_cursor_get(Cur, next)),
            ?assertMatch(not_found, elmdb:txn_cursor_get(Cur, next)),
            ?assertMatch(not_found, elmdb:txn_cursor_get(Cur, next)),
            ?assertMatch({ok, <<"b">>, <<"2">>}, elmdb:txn_cursor_get(Cur, {set, <<"b">>})),
            ?assertMatch(not_found, elmdb:txn_cursor_get(Cur, {set, <<"c">>})),
            ?assertMatch({ok, <<"d">>, <<"4">>}, elmdb:txn_cursor_get(Cur, {set_range, <<"c">>}))
    end.

basic_setup() ->
    Name = "test/testdb",
    {ok, Env} = elmdb:env_open(Name, []),
    {ok, Dbi} = elmdb:db_open(Env, []),
    {Env, Dbi, Name}.

basic_teardown({Env, _Dbi, Name}) ->
    ?assertMatch(ok, elmdb:env_close(Env)),
    delete_env(Name).

delete_env(Name) ->
    ?assertMatch(ok, file:delete(Name ++ "/data.mdb")),
    ?assertMatch(ok, file:delete(Name ++ "/lock.mdb")),
    ?assertMatch(ok, file:del_dir(Name)).
