%%-------------------------------------------------------------------
%% This file is part of Elmdb - Erlang Lightning MDB API
%%
%% Copyright (c) 2012 by Aleph Archives. All rights reserved.
%% Copyright (c) 2013 by Basho Technologies, Inc. All rights reserved.
%% Copyright (c) 2016 by Vincent Siliakus. All rights reserved.
%%
%%-------------------------------------------------------------------
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted only as authorized by the OpenLDAP
%% Public License.
%%
%% A copy of this license is available in the file LICENSE in the
%% top-level directory of the distribution or, alternatively, at
%% <http://www.OpenLDAP.org/license.html>.
%%
%% Permission to use, copy, modify, and distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%-------------------------------------------------------------------

-module(elmdb).

%%====================================================================
%% EXPORTS
%%====================================================================
-export([
         env_open/2,
         env_open/3,
         env_close/1,
         env_close_by_name/1,
         env_close_all/0,
         db_open/2,
         db_open/3,
         db_open/4,

         put/3,
         put_new/3,
         get/2,
         delete/2,
         drop/1,

         async_put/3,
         async_put/4,
         async_get/2,
         async_get/3,

         update_put/4,
         update_put/5,
         update_get/2,
         update_get/3,

         ro_txn_begin/1,
         ro_txn_get/3,
         ro_txn_commit/1,
         ro_txn_abort/1,

         ro_txn_cursor_open/2,
         ro_txn_cursor_close/1,
         ro_txn_cursor_get/2,

         txn_begin/1,
         txn_begin/2,
         txn_put/4,
         txn_put/5,
         txn_get/3,
         txn_get/4,
         txn_commit/1,
         txn_commit/2,
         txn_abort/1,
         txn_abort/2,
         txn_cursor_open/2,
         txn_cursor_open/3,
         txn_cursor_get/2,
         txn_cursor_get/3,
         txn_cursor_put/3,
         txn_cursor_put/4
        ]).


%% internal export (ex. spawn, apply)
-on_load(init/0).


%%====================================================================
%% MACROS
%%====================================================================
-define(ELMDB_DRIVER_NAME, "elmdb").
-define(NOT_LOADED, not_loaded(?LINE)).
-define (TIMEOUT, 5000).

%%====================================================================
%% TYPES
%%====================================================================
-type env() :: binary().
-type dbi() :: binary().
-type txn() :: binary().
-type cursor() :: binary().

-type key() :: binary().
-type val() :: binary().

-type env_open_opt() :: {map_size, non_neg_integer()} |
                        {max_dbs, non_neg_integer()} |
                        fixed_map | no_subdir | read_only |
                        write_map | no_meta_sync | no_sync |
                        map_async | no_read_ahead | no_mem_init.

-type env_open_opts() :: [env_open_opt()].

-type db_open_opt() :: reverse_key | dup_sort | reverse_dup | create.
-type db_open_opts() :: [db_open_opt()].

-type cursor_op() :: first | first_dup | get_both | get_both_range |
                     get_current | last | last_dup |
                     next | next_dup | next_nodup |
                     prev | prev_dup | prev_nodup |
                     {set, key()} | {set_range, key()}.

%%====================================================================
%% PUBLIC API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Create a new, or open an existing MDB environment
%% @end
%%--------------------------------------------------------------------
-spec env_open(string(), env_open_opts()) -> {ok, env()} | {error, any}.
env_open(DirName, Opts) ->
    env_open(DirName, Opts, ?TIMEOUT).

-spec env_open(string(), env_open_opts(), non_neg_integer()) -> {ok, env()} | {error, any()}.
env_open(DirName, Opts, Timeout)
  when is_list(Opts) ->
    %% ensure directory exists
    DirName2 = filename:absname(DirName),
    ok = filelib:ensure_dir(filename:join([DirName2, "x"])),
    Ref = make_ref(),
    case nif_env_open(Ref, DirName2, Opts) of
        ok -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_env_open(_Ref, _DirName, _Opts) ->
    ?NOT_LOADED.

-spec env_close(env()) -> ok.
env_close(_Env) ->
    ?NOT_LOADED.

-spec env_close_by_name(string()) -> ok | {error, not_found}.
env_close_by_name(DirName) ->
    nif_env_close_by_name(filename:absname(DirName)).

nif_env_close_by_name(_DirName) ->
    ?NOT_LOADED.

-spec env_close_all() -> ok.
env_close_all() ->
    ?NOT_LOADED.

-spec db_open(env(), db_open_opts()) -> {ok, dbi()} | {error, any()}.
db_open(Env, Opts) ->
    db_open(Env, <<"">>, Opts, ?TIMEOUT).

-spec db_open(env(), key(), db_open_opts()) -> {ok, dbi()} | {error, any()}.
db_open(Env, Name, Opts) ->
    db_open(Env, Name, Opts, ?TIMEOUT).

-spec db_open(env(), key(), db_open_opts(), non_neg_integer()) -> {ok, dbi()} | {error, any()}.
db_open(Env, Name, Opts, Timeout) ->
    Ref = make_ref(),
    case nif_db_open(Ref, Env, Name, Opts) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_db_open(_Ref, _Env, _Name, _Opts) ->
    ?NOT_LOADED.

-spec put(dbi(), key(), val()) -> ok | {error, any()}.
put(_Dbi, _Key, _Val) ->
    ?NOT_LOADED.

-spec put_new(dbi(), key(), val()) -> ok | exists | {error, any()}.
put_new(_Dbi, _Key, _Val) ->
    ?NOT_LOADED.

-spec get(dbi(), key()) -> {ok, val()} | {error, any()}.
get(_Dbi, _Key) ->
    ?NOT_LOADED.

-spec delete(dbi(), key()) -> ok | {error, any()}.
delete(_Dbi, _Key) ->
    ?NOT_LOADED.

-spec drop(dbi()) -> ok | {error, any()}.
drop(_Dbi) ->
    ?NOT_LOADED.

-spec async_put(dbi(), key(), val()) -> ok | {error, any()}.
async_put(Dbi, Key, Val) ->
    async_put(Dbi, Key, Val, ?TIMEOUT).

-spec async_put(dbi(), key(), val(), non_neg_integer()) -> ok | {error, any()}.
async_put(Dbi, Key, Val, Timeout) ->
    Ref = make_ref(),
    case nif_async_put(Ref, Dbi, Key, Val) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_async_put(_Ref, _Dbi, _Key, _Val) ->
    ?NOT_LOADED.

-spec async_get(dbi(), key()) -> {ok, val()} | not_found | {error, any()}.
async_get(Dbi, Key) ->
    async_get(Dbi, Key, ?TIMEOUT).

-spec async_get(dbi(), key(), non_neg_integer()) -> {ok, val()} | not_found | {error, any()}.
async_get(Dbi, Key, Timeout) ->
    Ref = make_ref(),
    case nif_async_get(Ref, Dbi, Key) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_async_get(_Ref, _Dbi, _Key) ->
    ?NOT_LOADED.

-spec update_put(txn(), dbi(), key(), val()) -> ok | {error, any()}.
update_put(Txn, Dbi, Key, Val) ->
    update_put(Txn, Dbi, Key, Val, ?TIMEOUT).

-spec update_put(txn(), dbi(), key(), val(), non_neg_integer()) -> ok | {error, any()}.
update_put(Txn, Dbi, Key, Val, Timeout) ->
    Ref = make_ref(),
    case nif_update_put(Ref, Txn, Dbi, Key, Val) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_update_put(_Ref, _Txn, _Dbi, _Key, _Val) ->
    ?NOT_LOADED.

-spec update_get(dbi(), key()) -> {ok, val(), txn()} | {error, any()}.
update_get(Dbi, Key) ->
    update_get(Dbi, Key, ?TIMEOUT).

-spec update_get(dbi(), key(), non_neg_integer()) -> {ok, val(), txn()} | {error, any()}.
update_get(Dbi, Key, Timeout) ->
    Ref = make_ref(),
    case nif_update_get(Ref, Dbi, Key) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_update_get(_Ref, _Dbi, _Key) ->
    ?NOT_LOADED.

-spec ro_txn_begin(env()) -> {ok, txn()} | {error, any()}.
ro_txn_begin(_Env) ->
    ?NOT_LOADED.

-spec ro_txn_get(txn(), dbi(), key()) -> {ok, val()} | {error, any()}.
ro_txn_get(_Txn, _Dbi, _Key) ->
    ?NOT_LOADED.

-spec ro_txn_commit(txn()) -> ok | {error, any()}.
ro_txn_commit(_Txn) ->
    ?NOT_LOADED.

-spec ro_txn_abort(txn()) -> ok | {error, any()}.
ro_txn_abort(_Txn) ->
    ?NOT_LOADED.

-spec ro_txn_cursor_open(txn(), dbi()) -> {ok, cursor()} | {error, any()}.
ro_txn_cursor_open(_Txn, _Dbi) ->
    ?NOT_LOADED.

-spec ro_txn_cursor_close(cursor()) -> ok.
ro_txn_cursor_close(_Cur) ->
    ?NOT_LOADED.

-spec ro_txn_cursor_get(cursor(), cursor_op()) -> {ok, key(), val()} | not_found | {error, any()}.
ro_txn_cursor_get(_Cur, _Op) ->
    ?NOT_LOADED.

-spec txn_begin(env()) -> {ok, txn()} | {error, any()}.
txn_begin(Env) ->
    txn_begin(Env, ?TIMEOUT).

-spec txn_begin(env(), non_neg_integer()) -> {ok, txn()} | {error, any()}.
txn_begin(Env, Timeout) ->
    Ref = make_ref(),
    case nif_txn_begin(Ref, Env) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_begin(_Ref, _Env) ->
    ?NOT_LOADED.

-spec txn_put(txn(), dbi(), key(), val()) -> ok | {error, any()}.
txn_put(Txn, Dbi, Key, Val) ->
    txn_put(Txn, Dbi, Key, Val, ?TIMEOUT).

-spec txn_put(txn(), dbi(), key(), val(), non_neg_integer()) -> ok | {error, any()}.
txn_put(Txn, Dbi, Key, Val, Timeout) ->
    Ref = make_ref(),
    case nif_txn_put(Ref, Txn, Dbi, Key, Val) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_put(_Ref, _Txn, _Dbi, _Key, _Val) ->
    ?NOT_LOADED.

-spec txn_get(txn(), dbi(), key()) -> {ok, val()} | not_found | {error, any()}.
txn_get(Txn, Dbi, Key) ->
    txn_get(Txn, Dbi, Key, ?TIMEOUT).

-spec txn_get(txn(), dbi(), key(), non_neg_integer()) -> {ok, val()} | not_found | {error, any()}.
txn_get(Txn, Dbi, Key, Timeout) ->
    Ref = make_ref(),
    case nif_txn_get(Ref, Txn, Dbi, Key) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_get(_Ref, _Txn, _Dbi, _Key) ->
    ?NOT_LOADED.

-spec txn_commit(txn()) -> ok | {error, any()}.
txn_commit(Txn) ->
    txn_commit(Txn, ?TIMEOUT).

-spec txn_commit(txn(), non_neg_integer()) -> ok | {error, any()}.
txn_commit(Txn, Timeout) ->
    Ref = make_ref(),
    case nif_txn_commit(Ref, Txn) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_commit(_Ref, _Txn) ->
    ?NOT_LOADED.

-spec txn_abort(txn()) -> ok | {error, any()}.
txn_abort(Txn) ->
    txn_abort(Txn, ?TIMEOUT).

-spec txn_abort(txn(), non_neg_integer()) -> ok | {error, any()}.
txn_abort(Txn, Timeout) ->
    Ref = make_ref(),
    case nif_txn_abort(Ref, Txn) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_abort(_Ref, _Txn) ->
    ?NOT_LOADED.

-spec txn_cursor_open(txn(), dbi()) -> {ok, cursor()} | {error, any()}.
txn_cursor_open(Txn, Dbi) ->
    txn_cursor_open(Txn, Dbi, ?TIMEOUT).

-spec txn_cursor_open(txn(), dbi(), non_neg_integer()) -> {ok, cursor()} | {error, any()}.
txn_cursor_open(Txn, Dbi, Timeout) ->
    Ref = make_ref(),
    case nif_txn_cursor_open(Ref, Txn, Dbi) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_cursor_open(_Ref, _Txn, _Dbi) ->
    ?NOT_LOADED.

-spec txn_cursor_get(cursor(), cursor_op()) -> {ok, key(), val()} | not_found | {error, any()}.
txn_cursor_get(Cur, Op) ->
    txn_cursor_get(Cur, Op, ?TIMEOUT).

-spec txn_cursor_get(cursor(), cursor_op(), non_neg_integer()) -> {ok, key(), val()} | not_found | {error, any()}.
txn_cursor_get(Cur, Op, Timeout) ->
    Ref = make_ref(),
    case nif_txn_cursor_get(Ref, Cur, Op) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_cursor_get(_Ref, _Cur, _Op) ->
    ?NOT_LOADED.

-spec txn_cursor_put(cursor(), key(), val()) -> ok | {error, any}.
txn_cursor_put(Cur, Key, Val) ->
    txn_cursor_put(Cur, Key, Val, ?TIMEOUT).

-spec txn_cursor_put(cursor(), key(), val(), non_neg_integer()) -> ok | {error, any}.
txn_cursor_put(Cur, Key, Val, Timeout) ->
    Ref = make_ref(),
    case nif_txn_cursor_put(Ref, Cur, Key, Val) of
        ok    -> recv_async(Ref, Timeout);
        Error -> Error
    end.

nif_txn_cursor_put(_Ref, _Cur, _Key, _Val) ->
    ?NOT_LOADED.

%%====================================================================
%% PRIVATE API
%%====================================================================

recv_async(Ref, Timeout) ->
    receive
        {elmdb, Ref, {error, badarg}} ->
            error(badarg);
        {elmdb, Ref, Ret} ->
            Ret;
        % Flush old messages left over from previous timeouts
        {elmdb, OtherRef, _Ret} when is_reference(OtherRef) ->
            recv_async(Ref, Timeout)
    after
        Timeout ->
            {error, timeout}
    end.

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    case erlang:load_nif(filename:join(PrivDir, ?ELMDB_DRIVER_NAME), 0) of
        ok ->                  ok;
        {error,{reload, _}} -> ok;
        Error ->               Error
    end.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
