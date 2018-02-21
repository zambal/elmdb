/* -------------------------------------------------------------------------
 * This file is part of Elmdb - Erlang Lightning MDB API
 *
 * Copyright (c) 2012 by Aleph Archives. All rights reserved.
 * Copyright (c) 2013 by Basho Technologies, Inc. All rights reserved.
 * Copyright (c) 2016 by Vincent Siliakus. All rights reserved.
 *
 * -------------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * -------------------------------------------------------------------------*/

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/param.h>
#include <erl_nif.h>
#include <erl_driver.h>

#include "lmdb.h"
#include "queue.h"

typedef struct _OpEntry {
  uint64_t txn_ref;
  ErlNifEnv *msg_env;
  ErlNifPid caller;
  ERL_NIF_TERM ref;
  void *args;
  MDB_txn* (*handler)(MDB_txn *, struct _OpEntry*);
  STAILQ_ENTRY(_OpEntry) entries;
} OpEntry;

static ErlNifResourceType *elmdb_env_res;
typedef struct {
  uint64_t ref;
  MDB_env *env;
  uint64_t txn_ref_cnt;
  uint64_t active_txn_ref;
  ErlNifMutex *op_lock;
  ErlNifMutex *txn_lock;
  ErlNifCond *txn_cond;
  ErlNifTid tid;
  int shutdown;
  char path[MAXPATHLEN];

  STAILQ_HEAD(op_queue, _OpEntry) op_queue;
  STAILQ_HEAD(txn_queue, _OpEntry) txn_queue;
} ElmdbEnv;

static ErlNifResourceType *elmdb_dbi_res;
typedef struct {
  MDB_dbi dbi;
  char *name;
  ElmdbEnv *elmdb_env;
} ElmdbDbi;

static ErlNifResourceType *elmdb_txn_res;
typedef struct {
  uint64_t ref;
  ElmdbEnv *elmdb_env;
} ElmdbTxn;

static ErlNifResourceType *elmdb_ro_txn_res;
typedef struct {
  MDB_txn *txn;
  ElmdbEnv *elmdb_env;
  int active;
} ElmdbRoTxn;

static ErlNifResourceType *elmdb_cur_res;
typedef struct {
  MDB_cursor *cursor;
  ElmdbTxn *elmdb_txn;
  int active;
} ElmdbCur;

static ErlNifResourceType *elmdb_ro_cur_res;
typedef struct {
  MDB_cursor *cursor;
  ElmdbRoTxn *elmdb_ro_txn;
  int active;
} ElmdbRoCur;

typedef struct _EnvEntry {
  ElmdbEnv *elmdb_env;
  SLIST_ENTRY(_EnvEntry) entries;
} EnvEntry;

typedef struct {
  uint64_t env_ref;
  ErlNifMutex *env_lock;
  SLIST_HEAD(env_list, _EnvEntry) env_list;
}  ElmdbPriv;


typedef struct {
  char path[MAXPATHLEN];
  uint64_t mapsize;
  unsigned int maxdbs;
  unsigned int envflags;
  ErlNifPid caller;
  ERL_NIF_TERM ref;
  ErlNifEnv *msg_env;
  ElmdbPriv *priv;
  ErlNifTid tid;
}  handler_args;

typedef struct {
  MDB_val key;
  MDB_val val;
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
} kv_args;

typedef struct {
  MDB_val key;
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
} k_args;

typedef struct {
  ElmdbCur *elmdb_cur;
  int op;
  MDB_val key;
} cursor_get_args;

typedef struct {
  ElmdbCur *elmdb_cur;
  MDB_val key;
  MDB_val val;
} cursor_put_args;

typedef struct {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
} dbi_args;

typedef struct {
  ElmdbEnv *elmdb_env;
  char name[MAXPATHLEN];
  unsigned int flags;
} db_open_args;

typedef struct {
  ElmdbEnv *elmdb_env;
} txn_begin_args;

typedef struct {
  ElmdbTxn *elmdb_txn;
} txn_args;

/* Atoms (initialized in on_load) */
static ERL_NIF_TERM ATOM_ELMDB;
static ERL_NIF_TERM ATOM_FIXEDMAP;
static ERL_NIF_TERM ATOM_NOSUBDIR;
static ERL_NIF_TERM ATOM_RDONLY;
static ERL_NIF_TERM ATOM_WRITEMAP;
static ERL_NIF_TERM ATOM_NOMETASYNC;
static ERL_NIF_TERM ATOM_NOSYNC;
static ERL_NIF_TERM ATOM_MAPASYNC;
static ERL_NIF_TERM ATOM_NORDAHEAD;
static ERL_NIF_TERM ATOM_NOMEMINIT;
static ERL_NIF_TERM ATOM_MAPSIZE;
static ERL_NIF_TERM ATOM_MAXDBS;

static ERL_NIF_TERM ATOM_REVERSEKEY;
static ERL_NIF_TERM ATOM_DUPSORT;
static ERL_NIF_TERM ATOM_REVERSEDUP;
static ERL_NIF_TERM ATOM_CREATE;

static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ENV_CLOSED;
static ERL_NIF_TERM ATOM_TXN_CLOSED;
static ERL_NIF_TERM ATOM_CUR_CLOSED;
static ERL_NIF_TERM ATOM_ENV_NOT_FOUND;
static ERL_NIF_TERM ATOM_BADARG;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_EXISTS;
static ERL_NIF_TERM ATOM_KEYEXIST;
static ERL_NIF_TERM ATOM_PAGE_NOTFOUND;
static ERL_NIF_TERM ATOM_CORRUPTED;
static ERL_NIF_TERM ATOM_PANIC;
static ERL_NIF_TERM ATOM_VERSION_MISMATCH;
static ERL_NIF_TERM ATOM_MAP_FULL;
static ERL_NIF_TERM ATOM_DBS_FULL;
static ERL_NIF_TERM ATOM_READERS_FULL;
static ERL_NIF_TERM ATOM_TLS_FULL;
static ERL_NIF_TERM ATOM_TXN_FULL;
static ERL_NIF_TERM ATOM_CURSOR_FULL;
static ERL_NIF_TERM ATOM_PAGE_FULL;
static ERL_NIF_TERM ATOM_MAP_RESIZED;
static ERL_NIF_TERM ATOM_INCOMPATIBLE;
static ERL_NIF_TERM ATOM_BAD_RSLOT;

static ERL_NIF_TERM ATOM_TXN_STARTED;
static ERL_NIF_TERM ATOM_TXN_NOT_STARTED;

static ERL_NIF_TERM ATOM_FIRST;
static ERL_NIF_TERM ATOM_FIRST_DUP;
static ERL_NIF_TERM ATOM_GET_BOTH;
static ERL_NIF_TERM ATOM_GET_BOTH_RANGE;
static ERL_NIF_TERM ATOM_GET_CURRENT;
static ERL_NIF_TERM ATOM_LAST;
static ERL_NIF_TERM ATOM_LAST_DUP;
static ERL_NIF_TERM ATOM_NEXT;
static ERL_NIF_TERM ATOM_NEXT_DUP;
static ERL_NIF_TERM ATOM_NEXT_NODUP;
static ERL_NIF_TERM ATOM_PREV;
static ERL_NIF_TERM ATOM_PREV_DUP;
static ERL_NIF_TERM ATOM_PREV_NODUP;
static ERL_NIF_TERM ATOM_SET;
static ERL_NIF_TERM ATOM_SET_RANGE;

#define OP_HANDLER(handler) (MDB_txn* (*)(MDB_txn *, OpEntry*))handler

#define NEW_OP_(op_var, op_args, op_handler)                   \
  OpEntry *op_var;                                             \
  op_var = (OpEntry*) enif_alloc(sizeof(OpEntry));             \
  op_var->msg_env = enif_alloc_env();                          \
  enif_self(env, &op_var->caller);                             \
  op_var->ref = enif_make_copy(op_var->msg_env, argv[0]);      \
  op_var->args = op_args;                                      \
  op_var->handler = op_handler;                                \

#define NEW_OP(op_var, op_args, op_name)                \
  NEW_OP_(op_var, op_args, OP_HANDLER(op_name));        \

#define FREE_OP(op_var)                                 \
  enif_free_env(op_var->msg_env);                       \
  if(op_var->args != NULL) enif_free(op_var->args);     \
  enif_free(op_var);                                    \

#define PUSH(queue, x) STAILQ_INSERT_TAIL(&queue, x, entries)
#define POP(queue, var)                         \
  var = STAILQ_FIRST(&queue);                   \
  STAILQ_REMOVE_HEAD(&queue, entries);          \

#define ADD(list, var) SLIST_INSERT_HEAD(&list, var, entries)
#define FOREACH(var, list) SLIST_FOREACH(var, &list, entries)
#define REMOVE(list, var, stype) SLIST_REMOVE(&list, var, stype, entries)

#define SEND(var, msg) enif_send(NULL, &var->caller, var->msg_env, enif_make_tuple3(var->msg_env, ATOM_ELMDB, var->ref, msg))
#define SEND_OK(var, msg) SEND(var, enif_make_tuple2(var->msg_env, ATOM_OK, msg))
#define SEND_ERR(var, msg) SEND(var, __strerror_atom(var->msg_env, msg))
#define SEND_ERRNO(var, err) SEND(var, __strerror_int(var->msg_env, err))

#define CHECK(expr, label)                      \
  if(MDB_SUCCESS != (ret = (expr))) {           \
    err = __strerror_int(env, ret);             \
    goto label;                                 \
  }

#define OK(msg) enif_make_tuple2(env, ATOM_OK, msg)
#define ERR(msg) __strerror_atom(env, msg)
#define ERRNO(e) __strerror_int(env, (e))

#define FAIL_ERR(msg, label)                    \
  do {                                          \
    err = ERR(msg);                             \
    goto label;                                 \
  } while(0)                                    \

#define FAIL_ERRNO(e, label)                    \
  do {						\
    err = ERRNO(e);                             \
    goto label;					\
  } while(0)

#define BADARG enif_make_badarg(env)

#define CHECK_ENV(elmdb_env)                    \
  if(elmdb_env->shutdown > 0) {                 \
    enif_mutex_unlock(elmdb_env->txn_lock);     \
    return ERR(ATOM_ENV_CLOSED);                \
  }                                             \

#define LOCKED_CHECK_ENV(elmdb_env)             \
  do {                                          \
    enif_mutex_lock(elmdb_env->txn_lock);       \
    CHECK_ENV(elmdb_env);                       \
    enif_mutex_unlock(elmdb_env->txn_lock);     \
  } while(0)                                    \

#define __UNUSED(v) ((void)(v))

#define LOG(txt) printf(txt); fflush(stdout)


/**
 * Convenience function to generate {error, {errno, Reason}}
 *
 * env    NIF environment
 * err    number of last error
 */
static ERL_NIF_TERM
__strerror_int(ErlNifEnv* env, int err)
{
  ERL_NIF_TERM term = 0;

  if(err < MDB_LAST_ERRCODE && err > MDB_KEYEXIST) {
    switch (err) {
    case MDB_KEYEXIST: /** key/data pair already exists */
      term = ATOM_KEYEXIST;
      break;
    case MDB_NOTFOUND: /** key/data pair not found (EOF) */
      term = ATOM_NOT_FOUND;
      break;
    case MDB_PAGE_NOTFOUND: /** Requested page not found - this usually indicates corruption */
      term = ATOM_PAGE_NOTFOUND;
      break;
    case MDB_CORRUPTED: /** Located page was wrong type */
      term = ATOM_CORRUPTED;
      break;
    case MDB_PANIC	: /** Update of meta page failed, probably I/O error */
      term = ATOM_PANIC;
      break;
    case MDB_VERSION_MISMATCH: /** Environment version mismatch */
      term = ATOM_VERSION_MISMATCH;
      break;
    case MDB_INVALID: /** File is not a valid MDB file */
      term = ATOM_KEYEXIST;
      break;
    case MDB_MAP_FULL: /** Environment mapsize reached */
      term = ATOM_MAP_FULL;
      break;
    case MDB_DBS_FULL: /** Environment maxdbs reached */
      term = ATOM_DBS_FULL;
      break;
    case MDB_READERS_FULL: /** Environment maxreaders reached */
      term = ATOM_READERS_FULL;
      break;
    case MDB_TLS_FULL: /** Too many TLS keys in use - Windows only */
      term = ATOM_TLS_FULL;
      break;
    case MDB_TXN_FULL: /** Txn has too many dirty pages */
      term = ATOM_TXN_FULL;
      break;
    case MDB_CURSOR_FULL: /** Cursor stack too deep - internal error */
      term = ATOM_CURSOR_FULL;
      break;
    case MDB_PAGE_FULL: /** Page has not enough space - internal error */
      term = ATOM_PAGE_FULL;
      break;
    case MDB_MAP_RESIZED: /** Database contents grew beyond environment mapsize */
      term = ATOM_MAP_RESIZED;
      break;
    case MDB_INCOMPATIBLE: /** Database flags changed or would change */
      term = ATOM_INCOMPATIBLE;
      break;
    case MDB_BAD_RSLOT: /** Invalid reuse of reader locktable slot */
      term = ATOM_BAD_RSLOT;
      break;
    }
  } else {
    term = enif_make_atom(env, erl_errno_id(err));
  }

  /* We return the errno value as well as the message here because the error
     message provided by strerror() for differ across platforms and/or may be
     localized to any given language (i18n).  Use the errno atom rather than
     the message when matching in Erlang.  You've been warned. */
  return enif_make_tuple2(env, ATOM_ERROR,
                          enif_make_tuple2(env, term,
                                           enif_make_string(env, mdb_strerror(err), ERL_NIF_LATIN1)));
}

static ERL_NIF_TERM __strerror_atom(ErlNifEnv* env, ERL_NIF_TERM err_atom) {
  const char *err_str;

  if(err_atom == ATOM_ENV_CLOSED) {
    err_str = "The environment is closed";
  } else if(err_atom == ATOM_TXN_CLOSED) {
    err_str = "The transaction is either commited or aborted";
  } else if(err_atom == ATOM_CUR_CLOSED) {
    err_str = "The cursor is closed";
  } else if(err_atom == ATOM_ENV_NOT_FOUND) {
    err_str = "The environment is already closed or does not exist";
  } else {
    err_str = "Unknown error";
  }

  return enif_make_tuple2(env, ATOM_ERROR,
                          enif_make_tuple2(env, err_atom,
                                           enif_make_string(env, err_str, ERL_NIF_LATIN1)));
}

static int to_mdb_cursor_op(ErlNifEnv *env, ERL_NIF_TERM op, MDB_val *key) {
  const ERL_NIF_TERM *tup_array;
  int tup_arity = 0;
  ERL_NIF_TERM term_key;
  ErlNifBinary bin_key;

  if(enif_is_identical(op, ATOM_NEXT) != 0)
    return MDB_NEXT;
  if(enif_is_identical(op, ATOM_PREV) != 0)
    return MDB_PREV;
  if(enif_is_identical(op, ATOM_FIRST) != 0)
    return MDB_FIRST;
  if(enif_is_identical(op, ATOM_FIRST_DUP) != 0)
    return MDB_FIRST_DUP;
  if(enif_is_identical(op, ATOM_GET_BOTH) != 0)
    return MDB_GET_BOTH;
  if(enif_is_identical(op, ATOM_GET_BOTH_RANGE) != 0)
    return MDB_GET_BOTH_RANGE;
  if(enif_is_identical(op, ATOM_GET_CURRENT) != 0)
    return MDB_GET_CURRENT;
  if(enif_is_identical(op, ATOM_LAST) != 0)
    return MDB_LAST;
  if(enif_is_identical(op, ATOM_LAST_DUP) != 0)
    return MDB_LAST_DUP;
  if(enif_is_identical(op, ATOM_NEXT_DUP) != 0)
    return MDB_NEXT_DUP;
  if(enif_is_identical(op, ATOM_NEXT_NODUP) != 0)
    return MDB_NEXT_NODUP;
  if(enif_is_identical(op, ATOM_PREV_DUP) != 0)
    return MDB_PREV_DUP;
  if(enif_is_identical(op, ATOM_PREV_NODUP) != 0)
    return MDB_PREV_NODUP;

  if(enif_get_tuple(env, op, &tup_arity, &tup_array) &&
     tup_arity == 2 && enif_is_binary(env, tup_array[1])) {
    term_key = enif_make_copy(env, tup_array[1]);
    if(enif_inspect_binary(env, term_key, &bin_key) != 0) {
      key->mv_size = bin_key.size;
      key->mv_data = bin_key.data;
      if(enif_is_identical(tup_array[0], ATOM_SET) != 0)
        return MDB_SET;
      if(enif_is_identical(tup_array[0], ATOM_SET_RANGE) != 0)
        return MDB_SET_RANGE;
    }
  }

  return 0;
}

static ElmdbEnv* open_env(const char *path, uint64_t mapsize, int maxdbs, int envflags, int *ret) {
  ElmdbEnv *elmdb_env;

  if((elmdb_env = enif_alloc_resource(elmdb_env_res, sizeof(ElmdbEnv))) == NULL)
    goto err1;

  elmdb_env->tid            = enif_thread_self();
  elmdb_env->env            = NULL;
  elmdb_env->op_lock        = NULL;
  elmdb_env->txn_lock       = NULL;
  elmdb_env->txn_cond       = NULL;
  elmdb_env->active_txn_ref = 0;
  elmdb_env->txn_ref_cnt    = 0;
  elmdb_env->shutdown       = 0;
  strncpy(elmdb_env->path, path, MAXPATHLEN);
  STAILQ_INIT(&elmdb_env->op_queue);
  STAILQ_INIT(&elmdb_env->txn_queue);

  if((*ret = mdb_env_create(&(elmdb_env->env))) != MDB_SUCCESS)
    goto err1;
  if((*ret = mdb_env_set_mapsize(elmdb_env->env, mapsize)) != MDB_SUCCESS)
    goto err1;
  if((*ret = mdb_env_set_maxdbs(elmdb_env->env, maxdbs)) != MDB_SUCCESS)
    goto err1;
  if((*ret = mdb_env_open(elmdb_env->env, elmdb_env->path, envflags, 0664)) != MDB_SUCCESS)
    goto err2;
  if((elmdb_env->op_lock = enif_mutex_create(elmdb_env->path)) == NULL)
    goto err2;
  if((elmdb_env->txn_lock = enif_mutex_create(elmdb_env->path)) == NULL)
    goto err2;
  if((elmdb_env->txn_cond = enif_cond_create(elmdb_env->path)) == NULL)
    goto err2;

  return elmdb_env;

 err2:
  mdb_env_close(elmdb_env->env);
 err1:
  return NULL;
}

static void close_env(ElmdbEnv *elmdb_env) {
  enif_mutex_lock(elmdb_env->txn_lock);
  if(elmdb_env->shutdown < 2) {
    elmdb_env->shutdown = 2;
    enif_mutex_unlock(elmdb_env->txn_lock);

    enif_mutex_lock(elmdb_env->op_lock);
    OpEntry *op;
    while (!STAILQ_EMPTY(&elmdb_env->op_queue)) {
      op = STAILQ_FIRST(&elmdb_env->op_queue);
      STAILQ_REMOVE_HEAD(&elmdb_env->op_queue, entries);
      SEND_ERR(op, ATOM_ENV_CLOSED);
      FREE_OP(op);
    }
    enif_mutex_unlock(elmdb_env->op_lock);

    enif_mutex_lock(elmdb_env->txn_lock);
    OpEntry *txn;
    while (!STAILQ_EMPTY(&elmdb_env->txn_queue)) {
      txn = STAILQ_FIRST(&elmdb_env->txn_queue);
      STAILQ_REMOVE_HEAD(&elmdb_env->txn_queue, entries);
      SEND_ERR(txn, ATOM_ENV_CLOSED);
      FREE_OP(txn);
    }
    enif_mutex_unlock(elmdb_env->txn_lock);

    mdb_env_close(elmdb_env->env);
    elmdb_env->env = NULL;
  } else enif_mutex_unlock(elmdb_env->txn_lock);
}

static int register_env(ElmdbPriv *priv, ElmdbEnv *elmdb_env) {
  EnvEntry *env_entry = enif_alloc(sizeof(EnvEntry));
  if(env_entry == NULL)
    return 0;

  env_entry->elmdb_env = elmdb_env;
  enif_mutex_lock(priv->env_lock);
  elmdb_env->ref = ++priv->env_ref;
  ADD(priv->env_list, env_entry);
  enif_mutex_unlock(priv->env_lock);
  return 1;
}

static void unregister_env(ElmdbPriv *priv, ElmdbEnv *elmdb_env) {
  EnvEntry *n;
  enif_mutex_lock(priv->env_lock);
  FOREACH(n, priv->env_list) {
    if(strncmp(n->elmdb_env->path, elmdb_env->path, MAXPATHLEN) == 0) {
      REMOVE(priv->env_list, n, _EnvEntry);
      enif_mutex_unlock(priv->env_lock);
      enif_free(n);
      return;
    }
  }
  enif_mutex_unlock(priv->env_lock);
}

static void* elmdb_env_thread(void *p) {
  handler_args *args = (handler_args *) p;
  ElmdbPriv *priv = args->priv;
  ElmdbEnv *elmdb_env = NULL;
  MDB_txn *txn = NULL;
  int ret = 0;
  OpEntry *q_txn = NULL;
  OpEntry *q_op = NULL;

  if((elmdb_env = open_env(args->path, args->mapsize, args->maxdbs, args->envflags, &ret)) == NULL) {
    SEND_ERRNO(args, ret);
    enif_free_env(args->msg_env);
    return NULL;
  }

  if(register_env(priv, elmdb_env) == 0) {
    SEND_ERRNO(args, ENOMEM);
    enif_free_env(args->msg_env);
    close_env(elmdb_env);
    goto shutdown;
  }
  ERL_NIF_TERM term = enif_make_resource(args->msg_env, elmdb_env);
  enif_release_resource((void*)elmdb_env);
  enif_mutex_lock(elmdb_env->txn_lock);
  SEND_OK(args, term);
  enif_free_env(args->msg_env);
  while(elmdb_env->shutdown == 0) {
    enif_cond_wait(elmdb_env->txn_cond, elmdb_env->txn_lock);
    while(!STAILQ_EMPTY(&elmdb_env->txn_queue)) {
      POP(elmdb_env->txn_queue, q_txn);
      enif_mutex_unlock(elmdb_env->txn_lock);
      txn = q_txn->handler(txn, q_txn);
      enif_mutex_lock(elmdb_env->txn_lock);
      while(txn != NULL && elmdb_env->active_txn_ref > 0 && elmdb_env->shutdown == 0) {
        enif_mutex_unlock(elmdb_env->txn_lock);
        enif_mutex_lock(elmdb_env->op_lock);
        while(!STAILQ_EMPTY(&elmdb_env->op_queue)) {
          POP(elmdb_env->op_queue, q_op);
          enif_mutex_unlock(elmdb_env->op_lock);
          enif_mutex_lock(elmdb_env->txn_lock);
          if(txn != NULL && q_op->txn_ref == elmdb_env->active_txn_ref) {
            enif_mutex_unlock(elmdb_env->txn_lock);
            txn = q_op->handler(txn, q_op);
          }
          else {
            enif_mutex_unlock(elmdb_env->txn_lock);
            SEND_ERR(q_op, ATOM_TXN_CLOSED);
          }
          FREE_OP(q_op);
          enif_mutex_lock(elmdb_env->op_lock);
        }
        enif_mutex_unlock(elmdb_env->op_lock);
        enif_mutex_lock(elmdb_env->txn_lock);
      }
      elmdb_env->active_txn_ref = 0;
      if(txn != NULL) {
        mdb_txn_abort(txn);
        txn = NULL;
      }
      FREE_OP(q_txn);
    }
  }
  enif_mutex_unlock(elmdb_env->txn_lock);
 shutdown:
  unregister_env(priv, elmdb_env);
  close_env(elmdb_env);
  return NULL;
}

static void close_all(ElmdbPriv *priv) {
  EnvEntry *n;

  enif_mutex_lock(priv->env_lock);
  while (!SLIST_EMPTY(&priv->env_list)) {
    n = SLIST_FIRST(&priv->env_list);
    SLIST_REMOVE_HEAD(&priv->env_list, entries);
    enif_mutex_unlock(priv->env_lock);
    enif_mutex_lock(n->elmdb_env->txn_lock);
    n->elmdb_env->shutdown = 1;
    enif_cond_signal(n->elmdb_env->txn_cond);
    enif_mutex_unlock(n->elmdb_env->txn_lock);
    enif_thread_join(n->elmdb_env->tid, NULL);
    enif_free(n);
    enif_mutex_lock(priv->env_lock);
  }
  enif_mutex_unlock(priv->env_lock);
}

static ElmdbEnv* get_env(ElmdbPriv *priv, const char *path) {
  EnvEntry *n;
  enif_mutex_lock(priv->env_lock);
  FOREACH(n, priv->env_list) {
    if(strncmp(n->elmdb_env->path, path, MAXPATHLEN) == 0) {
      enif_mutex_unlock(priv->env_lock);
      return n->elmdb_env;
    }
  }
  enif_mutex_unlock(priv->env_lock);
  return NULL;
}

static int get_env_open_opts(ErlNifEnv *env, ERL_NIF_TERM opts, uint64_t *mapsize, unsigned int *maxdbs, unsigned int *flags) {
  uint64_t _mapsize = 1073741824;
  unsigned int _maxdbs = 0;
  unsigned int _flags = MDB_NOTLS;
  ERL_NIF_TERM head, tail;
  const ERL_NIF_TERM *tup_array;
  int tup_arity = 0;

  while(enif_get_list_cell(env, opts, &head, &tail)) {
    opts = tail;

    if(enif_is_atom(env, head) != 0) {
      if(enif_is_identical(head, ATOM_FIXEDMAP) != 0)
        _flags = _flags | MDB_FIXEDMAP;
      if(enif_is_identical(head, ATOM_NOSUBDIR) != 0)
        _flags = _flags | MDB_NOSUBDIR;
      if(enif_is_identical(head, ATOM_RDONLY) != 0)
        _flags = _flags | MDB_RDONLY;
      if(enif_is_identical(head, ATOM_WRITEMAP) != 0)
        _flags = _flags | MDB_WRITEMAP;
      if(enif_is_identical(head, ATOM_NOMETASYNC) != 0)
        _flags = _flags | MDB_NOMETASYNC;
      if(enif_is_identical(head, ATOM_NOSYNC) != 0)
        _flags = _flags | MDB_NOSYNC;
      if(enif_is_identical(head, ATOM_MAPASYNC) != 0)
        _flags = _flags | MDB_MAPASYNC;
      if(enif_is_identical(head, ATOM_NORDAHEAD) != 0)
        _flags = _flags | MDB_NORDAHEAD;
      if(enif_is_identical(head, ATOM_NOMEMINIT) != 0)
        _flags = _flags | MDB_NOMEMINIT;
    }
    else if(enif_get_tuple(env, head, &tup_arity, &tup_array) != 0) {
      if(tup_arity == 2) {

        if(enif_is_identical(tup_array[0], ATOM_MAPSIZE) != 0 &&
           enif_get_uint64(env, tup_array[1], &_mapsize) == 0)
          return 0;

        if(enif_is_identical(tup_array[0], ATOM_MAXDBS) != 0 &&
           enif_get_uint(env, tup_array[1], &_maxdbs) == 0)
          return 0;
      } else return 0;
    }
    else return 0;
  }
  *mapsize = _mapsize;
  *maxdbs = _maxdbs;
  *flags = _flags;
  return 1;
}

static int get_db_open_opts(ErlNifEnv *env, ERL_NIF_TERM opts, unsigned int *flags) {
  unsigned int _flags = 0;
  ERL_NIF_TERM head, tail;

  while(enif_get_list_cell(env, opts, &head, &tail)) {
    opts = tail;

    if(enif_is_atom(env, head) != 0) {
      if(enif_is_identical(head, ATOM_REVERSEKEY) != 0)
        _flags = _flags | MDB_REVERSEKEY;
      if(enif_is_identical(head, ATOM_DUPSORT) != 0)
        _flags = _flags | MDB_DUPSORT;
      if(enif_is_identical(head, ATOM_REVERSEDUP) != 0)
        _flags = _flags | MDB_REVERSEDUP;
      if(enif_is_identical(head, ATOM_CREATE) != 0)
        _flags = _flags | MDB_CREATE;
    }
    else {
      return 0;
    }

  }
  *flags = _flags;
  return 1;
}


/**
 * Opens a MDB database.
 *
 * argv[0]    msg ref
 * argv[1]    path to directory for the database files
 * argv[2]    property list with options
 */
static ERL_NIF_TERM elmdb_env_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  handler_args *args;
  int ret;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_is_list(env, argv[1]) &&
       enif_is_list(env, argv[2]))) {
    return BADARG;
  }

  args = (handler_args *) enif_alloc(sizeof(handler_args));
  memset(args, 0, sizeof(handler_args));

  if(enif_get_string(env, argv[1], args->path, MAXPATHLEN, ERL_NIF_LATIN1) == 0)
    return BADARG;

  if(get_env_open_opts(env, argv[2], &args->mapsize, &args->maxdbs, &args->envflags) == 0)
    return BADARG;

  args->priv = (ElmdbPriv*)enif_priv_data(env);
  args->msg_env = enif_alloc_env();
  args->ref = enif_make_copy(args->msg_env, argv[0]);
  enif_self(env, &args->caller);
  ErlNifTid tid;
  if((ret = enif_thread_create(args->path, &tid, elmdb_env_thread, args, NULL)) != 0)
    return ERRNO(ret);

  return ATOM_OK;
}


/**
 * Closes a MDB database.
 *
 * argv[0]    reference to the MDB handle resource
 */
static ERL_NIF_TERM elmdb_env_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbEnv *elmdb_env;

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_env_res, (void**)&elmdb_env))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_env->txn_lock);
  if(elmdb_env->shutdown > 0) {
    enif_mutex_unlock(elmdb_env->txn_lock);
    return ATOM_OK;
  }
  elmdb_env->shutdown = 1;
  enif_cond_signal(elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_env->txn_lock);
  enif_thread_join(elmdb_env->tid, NULL);
  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_env_close_by_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbEnv *elmdb_env;
  char path[MAXPATHLEN];
  ElmdbPriv *priv = (ElmdbPriv*)enif_priv_data(env);

  if(!(argc == 1 &&
       enif_is_list(env, argv[0]))) {
    return BADARG;
  }

  if(enif_get_string(env, argv[0], path, MAXPATHLEN, ERL_NIF_LATIN1) == 0)
    return BADARG;

  if((elmdb_env = get_env(priv, path)) == NULL)
    return ERR(ATOM_ENV_NOT_FOUND);

  enif_mutex_lock(elmdb_env->txn_lock);
  elmdb_env->shutdown = 1;
  enif_cond_signal(elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_env->txn_lock);
  enif_thread_join(elmdb_env->tid, NULL);
  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_env_close_all(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  __UNUSED(argc);
  __UNUSED(argv);

  ElmdbPriv *priv = (ElmdbPriv*)enif_priv_data(env);
  close_all(priv);
  return ATOM_OK;
}


static MDB_txn* elmdb_db_open_handler(MDB_txn *txn, OpEntry *op) {
  db_open_args *args = (db_open_args*)op->args;
  int ret;
  MDB_dbi dbi;
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM res;
  char *name = NULL;
  int len = strlen(args->name);
  if(len > 0) {
    if((name = enif_alloc(len + 1)) == NULL) {
      SEND_ERRNO(op, ENOMEM);
      goto err;
    }
    strncpy(name, args->name, MAXPATHLEN);
  }

  if((ret = mdb_txn_begin(args->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto err;
  }
  ret = mdb_dbi_open(txn, name, args->flags, &dbi);
  if(ret != MDB_SUCCESS) {
    mdb_txn_abort(txn);
    SEND_ERRNO(op, ret);
    goto err;
  }
  ret = mdb_txn_commit(txn);
  if(ret != MDB_SUCCESS) {
    mdb_dbi_close(args->elmdb_env->env, dbi);
    SEND_ERRNO(op, ret);
    goto err;
  }
  if((elmdb_dbi = enif_alloc_resource(elmdb_dbi_res, sizeof(ElmdbDbi))) == NULL) {
    SEND_ERRNO(op, ENOMEM);
    goto err;
  }

  elmdb_dbi->dbi = dbi;
  elmdb_dbi->name = name;
  elmdb_dbi->elmdb_env = args->elmdb_env;
  res = enif_make_resource(op->msg_env, elmdb_dbi);
  enif_release_resource(elmdb_dbi);
  SEND_OK(op, res);
  return NULL;

 err:
  if(name != NULL)
    enif_free(name);
  enif_release_resource(args->elmdb_env);
  return NULL;
}

static ERL_NIF_TERM elmdb_db_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbEnv *elmdb_env;
  ErlNifBinary db_name;
  unsigned int flags = 0;

  if(!(argc == 4 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_env_res, (void**)&elmdb_env) &&
       enif_inspect_binary(env, argv[2], &db_name) &&
       enif_is_list(env, argv[3]) && get_db_open_opts(env, argv[3], &flags))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_env);
  db_open_args *args = enif_alloc(sizeof(db_open_args));
  NEW_OP(op, args, elmdb_db_open_handler);
  memset(args->name, 0, MAXPATHLEN);
  strncpy(args->name, (char*)db_name.data, db_name.size);
  args->elmdb_env = elmdb_env;
  args->flags = flags;
  enif_keep_resource(elmdb_env);
  enif_mutex_lock(elmdb_env->txn_lock);
  PUSH(elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_env->txn_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_begin_handler(MDB_txn *txn, OpEntry *op) {
  txn_begin_args *args = (txn_begin_args*)op->args;
  int ret;
  ElmdbTxn *elmdb_txn;
  ERL_NIF_TERM res;
  if((ret = mdb_txn_begin(args->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    enif_release_resource(args->elmdb_env);
    return NULL;
  }
  else {
    if((elmdb_txn = enif_alloc_resource(elmdb_txn_res, sizeof(ElmdbTxn))) == NULL) {
      SEND_ERRNO(op, ENOMEM);
      enif_release_resource(args->elmdb_env);
      return NULL;
    }
    elmdb_txn->elmdb_env = args->elmdb_env;
    enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
    elmdb_txn->ref = ++args->elmdb_env->txn_ref_cnt;
    elmdb_txn->elmdb_env->active_txn_ref = elmdb_txn->ref;
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    res = enif_make_resource(op->msg_env, elmdb_txn);
    enif_release_resource(elmdb_txn);
    SEND_OK(op, res);
    return txn;
  }
}

static ERL_NIF_TERM elmdb_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbEnv *elmdb_env;
  if(!(argc == 2 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_env_res, (void**)&elmdb_env))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_env);
  txn_begin_args *args = enif_alloc(sizeof(txn_begin_args));
  NEW_OP(op, args, elmdb_txn_begin_handler);
  args->elmdb_env = elmdb_env;
  enif_keep_resource(elmdb_env);
  enif_mutex_lock(elmdb_env->txn_lock);
  PUSH(elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_env->txn_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_put_handler(MDB_txn *txn, OpEntry *op) {
  kv_args *args = (kv_args*)op->args;
  int ret;
  if((ret = mdb_put(txn, args->elmdb_dbi->dbi, &args->key, &args->val, 0)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
  }
  else {
    SEND(op, ATOM_OK);
  }
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM do_txn_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], MDB_txn* (*handler)(MDB_txn*, OpEntry*)) {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ERL_NIF_TERM term_val;
  ErlNifBinary bin_key;
  ErlNifBinary bin_val;

  if(!(argc == 5 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn) &&
       enif_get_resource(env, argv[2], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[3]) &&
       enif_is_binary(env, argv[4]))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;
  kv_args *args = enif_alloc(sizeof(kv_args));
  NEW_OP_(op, args, handler);
  op->txn_ref = elmdb_txn->ref;
  term_key = enif_make_copy(op->msg_env, argv[3]);
  term_val = enif_make_copy(op->msg_env, argv[4]);

  if(enif_inspect_binary(op->msg_env, term_key, &bin_key) == 0 ||
     enif_inspect_binary(op->msg_env, term_val, &bin_val) == 0) {
    FREE_OP(op);
    return BADARG;
  }
  args->elmdb_txn   = elmdb_txn;
  args->elmdb_dbi   = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  args->val.mv_size = bin_val.size;
  args->val.mv_data = bin_val.data;
  enif_keep_resource(elmdb_txn);
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_txn_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_txn_put(env, argc, argv, elmdb_txn_put_handler);
}

static MDB_txn* elmdb_txn_put_new_handler(MDB_txn *txn, OpEntry *op) {
  kv_args *args = (kv_args*)op->args;
  int ret;
  if((ret = mdb_put(txn, args->elmdb_dbi->dbi, &args->key, &args->val, MDB_NOOVERWRITE)) != MDB_SUCCESS) {
    if(MDB_KEYEXIST == ret) {
      SEND(op, ATOM_EXISTS);
    } else {
      SEND_ERRNO(op, ret);
    }
  }
  else {
    SEND(op, ATOM_OK);
  }
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_put_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_txn_put(env, argc, argv, elmdb_txn_put_new_handler);
}

static MDB_txn* elmdb_txn_get_handler(MDB_txn *txn, OpEntry *op) {
  k_args *args = (k_args*)op->args;
  MDB_val val;
  ERL_NIF_TERM term_val;
  unsigned char *bin;
  int ret;
  if((ret = mdb_get(txn, args->elmdb_dbi->dbi, &args->key, &val)) != MDB_SUCCESS) {
    if(ret == MDB_NOTFOUND) { SEND(op, ATOM_NOT_FOUND); }
    else { SEND_ERRNO(op, ret); }
  }
  else {
    bin = enif_make_new_binary(op->msg_env, val.mv_size, &term_val);
    if(bin == NULL) {
      SEND_ERRNO(op, ENOMEM);
    }
    memcpy(bin, val.mv_data, val.mv_size);
    SEND(op, enif_make_tuple(op->msg_env, 2, ATOM_OK, term_val));
  }
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ErlNifBinary bin_key;

  if(!(argc == 4 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn) &&
       enif_get_resource(env, argv[2], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[3]))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;

  k_args *args = enif_alloc(sizeof(k_args));
  NEW_OP(op, args, elmdb_txn_get_handler);
  op->txn_ref = elmdb_txn->ref;
  term_key = enif_make_copy(op->msg_env, argv[3]);
  if(enif_inspect_binary(env, term_key, &bin_key) == 0) {
    FREE_OP(op);
    return BADARG;
  }
  args->elmdb_txn = elmdb_txn;
  args->elmdb_dbi = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  enif_keep_resource(elmdb_txn);
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_delete_handler(MDB_txn *txn, OpEntry *op) {
  k_args *args = (k_args*)op->args;
  int ret;
  if((ret = mdb_del(txn, args->elmdb_dbi->dbi, &args->key, NULL)) != MDB_SUCCESS) {
    if(ret == MDB_NOTFOUND) { SEND(op, ATOM_NOT_FOUND); }
    else { SEND_ERRNO(op, ret); }
  }
  else {
    SEND(op, ATOM_OK);
  }
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ErlNifBinary bin_key;

  if(!(argc == 4 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn) &&
       enif_get_resource(env, argv[2], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[3]))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;

  k_args *args = enif_alloc(sizeof(k_args));
  NEW_OP(op, args, elmdb_txn_delete_handler);
  op->txn_ref = elmdb_txn->ref;
  term_key = enif_make_copy(op->msg_env, argv[3]);
  if(enif_inspect_binary(env, term_key, &bin_key) == 0) {
    FREE_OP(op);
    return BADARG;
  }
  args->elmdb_txn = elmdb_txn;
  args->elmdb_dbi = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  enif_keep_resource(elmdb_txn);
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_drop_handler(MDB_txn *txn, OpEntry *op) {
  dbi_args *args = (dbi_args*)op->args;
  int ret;
  if((ret = mdb_drop(txn, args->elmdb_dbi->dbi, 0)) == MDB_SUCCESS) {
    SEND(op, ATOM_OK);
  }
  else {
    SEND_ERRNO(op, ret);
  }
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn) &&
       enif_get_resource(env, argv[2], elmdb_dbi_res, (void**)&elmdb_dbi))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;

  dbi_args *args = enif_alloc(sizeof(dbi_args));
  NEW_OP(op, args, elmdb_txn_drop_handler);
  op->txn_ref = elmdb_txn->ref;
  args->elmdb_txn = elmdb_txn;
  args->elmdb_dbi = elmdb_dbi;
  enif_keep_resource(elmdb_txn);
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_commit_handler(MDB_txn *txn, OpEntry *op) {
  __UNUSED(op);
  txn_args *args = (txn_args*)op->args;
  int ret = mdb_txn_commit(txn);
  if(ret != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
  } else SEND(op, ATOM_OK);
  enif_release_resource(args->elmdb_txn);
  return NULL;
}

static ERL_NIF_TERM elmdb_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;

  if(!(argc == 2 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);

  txn_args *args = enif_alloc(sizeof(txn_args));
  NEW_OP(op, args, elmdb_txn_commit_handler);
  op->txn_ref = elmdb_txn->ref;
  args->elmdb_txn = elmdb_txn;
  enif_keep_resource(elmdb_txn);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_abort_handler(MDB_txn *txn, OpEntry *op) {
  __UNUSED(op);
  txn_args *args = (txn_args*)op->args;
  mdb_txn_abort(txn);
  SEND(op, ATOM_OK);
  enif_release_resource(args->elmdb_txn);
  return NULL;
}

static ERL_NIF_TERM elmdb_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;

  if(!(argc == 2 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);

  txn_args *args = enif_alloc(sizeof(txn_args));
  NEW_OP(op, args, elmdb_txn_abort_handler);
  op->txn_ref = elmdb_txn->ref;
  args->elmdb_txn = elmdb_txn;
  enif_keep_resource(elmdb_txn);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_cursor_open_handler(MDB_txn *txn, OpEntry *op) {
  dbi_args *args = (dbi_args*)op->args;
  ElmdbCur *elmdb_cur;
  ERL_NIF_TERM res;
  if((elmdb_cur = enif_alloc_resource(elmdb_cur_res, sizeof(ElmdbCur))) == NULL) {
    SEND_ERRNO(op, ENOMEM);
    enif_release_resource(args->elmdb_txn);
    return NULL;
  }

  elmdb_cur->elmdb_txn = args->elmdb_txn;
  elmdb_cur->active = 1;
  int ret = mdb_cursor_open(txn, args->elmdb_dbi->dbi, &elmdb_cur->cursor);
  if(ret == MDB_SUCCESS) {
    res = enif_make_resource(op->msg_env, elmdb_cur);
    enif_release_resource(elmdb_cur);
    SEND_OK(op, res);
  }
  else {
    SEND_ERRNO(op, ret);
    enif_release_resource(args->elmdb_txn);
  }
  enif_release_resource(args->elmdb_dbi);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbTxn *elmdb_txn;
  ElmdbDbi *elmdb_dbi;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_txn_res, (void**)&elmdb_txn) &&
       enif_get_resource(env, argv[2], elmdb_dbi_res, (void**)&elmdb_dbi))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_txn->elmdb_env);
  if(elmdb_txn->ref != elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;

  dbi_args *args = enif_alloc(sizeof(dbi_args));
  NEW_OP(op, args, elmdb_txn_cursor_open_handler);
  op->txn_ref = elmdb_txn->ref;
  args->elmdb_txn = elmdb_txn;
  args->elmdb_dbi = elmdb_dbi;
  enif_keep_resource(elmdb_txn);
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_cursor_get_handler(MDB_txn *txn, OpEntry *op) {
  cursor_get_args *args = (cursor_get_args*) op->args;
  MDB_val val;
  ERL_NIF_TERM term_key;
  ERL_NIF_TERM term_val;
  unsigned char *bin_key;
  unsigned char *bin_val;
  int ret = mdb_cursor_get(args->elmdb_cur->cursor, &args->key, &val, args->op);
  if(ret == MDB_SUCCESS) {
    bin_key = enif_make_new_binary(op->msg_env, args->key.mv_size, &term_key);
    if(!bin_key) {
      SEND_ERRNO(op, ENOMEM);
      goto done;
    }
    memcpy(bin_key, args->key.mv_data, args->key.mv_size);
    bin_val = enif_make_new_binary(op->msg_env, val.mv_size, &term_val);
    if(bin_val == NULL) {
      SEND_ERRNO(op, ENOMEM);
      goto done;
    }
    memcpy(bin_val, val.mv_data, val.mv_size);
    SEND(op, enif_make_tuple3(op->msg_env, ATOM_OK, term_key, term_val));
  }
  else if(ret == MDB_NOTFOUND) {
    SEND(op, ATOM_NOT_FOUND);
  }
  else SEND_ERRNO(op, ret);

 done:
  enif_release_resource(args->elmdb_cur);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_cursor_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbCur *elmdb_cur;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_cur_res, (void**)&elmdb_cur) &&
       (enif_is_atom(env, argv[2]) || enif_is_tuple(env, argv[2])))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_cur->elmdb_txn->elmdb_env);
  if(elmdb_cur->elmdb_txn->ref != elmdb_cur->elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_cur->active == 0)
    return ERR(ATOM_CUR_CLOSED);

  cursor_get_args *args = enif_alloc(sizeof(cursor_get_args));
  NEW_OP(op, args, elmdb_txn_cursor_get_handler);
  op->txn_ref = elmdb_cur->elmdb_txn->ref;
  args->elmdb_cur = elmdb_cur;
  args->op = to_mdb_cursor_op(env, argv[2], &args->key);
  enif_keep_resource(elmdb_cur);
  enif_mutex_lock(elmdb_cur->elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_cur->elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_txn_cursor_put_handler(MDB_txn *txn, OpEntry *op) {
  cursor_put_args *args = (cursor_put_args*)op->args;
  int ret;
  if((ret = mdb_cursor_put(args->elmdb_cur->cursor, &args->key, &args->val, 0)) == MDB_SUCCESS) {
    SEND(op, ATOM_OK);
  }
  else SEND_ERRNO(op, ret);
  enif_release_resource(args->elmdb_cur);
  return txn;
}

static ERL_NIF_TERM elmdb_txn_cursor_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbCur *elmdb_cur;
  ERL_NIF_TERM term_key;
  ERL_NIF_TERM term_val;
  ErlNifBinary bin_key;
  ErlNifBinary bin_val;

  if(!(argc == 4 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_cur_res, (void**)&elmdb_cur) &&
       enif_is_binary(env, argv[2]) &&
       enif_is_binary(env, argv[3]))) {
    return BADARG;
  }
  enif_mutex_lock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
  CHECK_ENV(elmdb_cur->elmdb_txn->elmdb_env);
  if(elmdb_cur->elmdb_txn->ref != elmdb_cur->elmdb_txn->elmdb_env->active_txn_ref) {
    enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
    return ERR(ATOM_TXN_CLOSED);
  }
  enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_cur->active == 0) {
    return ERR(ATOM_CUR_CLOSED);
  }
  cursor_put_args *args = enif_alloc(sizeof(cursor_put_args));
  NEW_OP(op, args, elmdb_txn_cursor_put_handler);
  op->txn_ref = elmdb_cur->elmdb_txn->ref;

  term_key = enif_make_copy(op->msg_env, argv[2]);
  term_val = enif_make_copy(op->msg_env, argv[3]);

  if(enif_inspect_binary(op->msg_env, term_key, &bin_key) == 0 ||
     enif_inspect_binary(op->msg_env, term_val, &bin_val) == 0) {
    FREE_OP(op);
    return BADARG;
  }

  args->elmdb_cur   = elmdb_cur;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  args->val.mv_size = bin_val.size;
  args->val.mv_data = bin_val.data;
  enif_keep_resource(elmdb_cur);
  enif_mutex_lock(elmdb_cur->elmdb_txn->elmdb_env->op_lock);
  PUSH(elmdb_cur->elmdb_txn->elmdb_env->op_queue, op);
  enif_mutex_unlock(elmdb_cur->elmdb_txn->elmdb_env->op_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_async_put_handler(MDB_txn *txn, OpEntry *op) {
  kv_args *args = (kv_args*)op->args;
  int ret;
 if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_put(txn, args->elmdb_dbi->dbi, &args->key, &args->val, 0)) != MDB_SUCCESS) {
    mdb_txn_abort(txn);
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_txn_commit(txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  SEND(op, ATOM_OK);

 done:
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM do_async_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], MDB_txn* (*handler)(MDB_txn*, OpEntry*)) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ERL_NIF_TERM term_val;
  ErlNifBinary bin_key;
  ErlNifBinary bin_val;
  if(!(argc == 4 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[2]) &&
       enif_is_binary(env, argv[3]))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  kv_args *args = enif_alloc(sizeof(kv_args));
  NEW_OP(op, args, handler);
  op->txn_ref = 0;
  term_key = enif_make_copy(op->msg_env, argv[2]);
  term_val = enif_make_copy(op->msg_env, argv[3]);

  if(enif_inspect_binary(op->msg_env, term_key, &bin_key) == 0 ||
     enif_inspect_binary(op->msg_env, term_val, &bin_val) == 0) {
    FREE_OP(op);
    return BADARG;
  }

  args->elmdb_dbi   = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  args->val.mv_size = bin_val.size;
  args->val.mv_data = bin_val.data;
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_dbi->elmdb_env->txn_lock);
  PUSH(elmdb_dbi->elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_dbi->elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_dbi->elmdb_env->txn_lock);
  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_async_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_async_put(env, argc, argv, elmdb_async_put_handler);
}

static MDB_txn* elmdb_async_put_new_handler(MDB_txn *txn, OpEntry *op) {
  kv_args *args = (kv_args*)op->args;
  int ret;
 if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_put(txn, args->elmdb_dbi->dbi, &args->key, &args->val, MDB_NOOVERWRITE)) != MDB_SUCCESS) {
    if(MDB_KEYEXIST == ret) {
      SEND(op, ATOM_EXISTS);
    } else {
      SEND_ERRNO(op, ret);
    }
    mdb_txn_abort(txn);
    goto done;
  }
  if((ret = mdb_txn_commit(txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  SEND(op, ATOM_OK);

 done:
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM elmdb_async_put_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_async_put(env, argc, argv, elmdb_async_put_new_handler);
}

static MDB_txn* elmdb_async_get_handler(MDB_txn *txn, OpEntry *op) {
  k_args *args = (k_args*)op->args;
  MDB_val val;
  ERL_NIF_TERM term_val;
  unsigned char *bin;
  int ret;
  if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_get(txn, args->elmdb_dbi->dbi, &args->key, &val)) != MDB_SUCCESS) {
    if(ret == MDB_NOTFOUND) { SEND(op, ATOM_NOT_FOUND); }
    else { SEND_ERRNO(op, ret); }
    goto done;
  }
  bin = enif_make_new_binary(op->msg_env, val.mv_size, &term_val);
  if(bin == NULL) {
    SEND_ERRNO(op, ENOMEM);
    goto done;
  }
  memcpy(bin, val.mv_data, val.mv_size);
  SEND(op, enif_make_tuple(op->msg_env, 2, ATOM_OK, term_val));

 done:
  enif_release_resource(args->elmdb_dbi);
  mdb_txn_abort(txn);
  return NULL;
}

static ERL_NIF_TERM do_async_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[], MDB_txn* (*handler)(MDB_txn*, OpEntry*)) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ErlNifBinary bin_key;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[2]))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  k_args *args = enif_alloc(sizeof(k_args));
  NEW_OP_(op, args, handler);
  op->txn_ref = 0;
  term_key = enif_make_copy(op->msg_env, argv[2]);

  if(enif_inspect_binary(op->msg_env, term_key, &bin_key) == 0) {
    FREE_OP(op);
    return BADARG;
  }

  args->elmdb_dbi   = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_dbi->elmdb_env->txn_lock);
  PUSH(elmdb_dbi->elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_dbi->elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_dbi->elmdb_env->txn_lock);
  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_async_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_async_get(env, argc, argv, elmdb_async_get_handler);
}

static MDB_txn* elmdb_async_delete_handler(MDB_txn *txn, OpEntry *op) {
  k_args *args = (k_args*)op->args;
  int ret;
  if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_del(txn, args->elmdb_dbi->dbi, &args->key, NULL)) != MDB_SUCCESS) {
    if(ret == MDB_NOTFOUND) { SEND(op, ATOM_NOT_FOUND); }
    else { SEND_ERRNO(op, ret); }
    mdb_txn_abort(txn);
    goto done;
  }
  if((ret = mdb_txn_commit(txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }

  SEND(op, ATOM_OK);

 done:
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM elmdb_async_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM term_key;
  ErlNifBinary bin_key;

  if(!(argc == 3 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_is_binary(env, argv[2]))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  k_args *args = enif_alloc(sizeof(k_args));
  NEW_OP(op, args, elmdb_async_delete_handler);
  op->txn_ref = 0;
  term_key = enif_make_copy(op->msg_env, argv[2]);
  if(enif_inspect_binary(env, term_key, &bin_key) == 0) {
    FREE_OP(op);
    return BADARG;
  }
  args->elmdb_dbi = elmdb_dbi;
  args->key.mv_size = bin_key.size;
  args->key.mv_data = bin_key.data;
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_dbi->elmdb_env->txn_lock);
  PUSH(elmdb_dbi->elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_dbi->elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_dbi->elmdb_env->txn_lock);
  return ATOM_OK;
}

static MDB_txn* elmdb_async_drop_handler(MDB_txn *txn, OpEntry *op) {
  dbi_args *args = (dbi_args*)op->args;
  int ret;
  if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }
  if((ret = mdb_drop(txn, args->elmdb_dbi->dbi, 0)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    mdb_txn_abort(txn);
    goto done;
  }
  if((ret = mdb_txn_commit(txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;
  }

  SEND(op, ATOM_OK);

 done:
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM elmdb_async_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;

  if(!(argc == 2 &&
       enif_is_ref(env, argv[0]) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  dbi_args *args = enif_alloc(sizeof(dbi_args));
  NEW_OP(op, args, elmdb_async_drop_handler);
  op->txn_ref = 0;
  args->elmdb_dbi = elmdb_dbi;
  enif_keep_resource(elmdb_dbi);
  enif_mutex_lock(elmdb_dbi->elmdb_env->txn_lock);
  PUSH(elmdb_dbi->elmdb_env->txn_queue, op);
  enif_cond_signal(elmdb_dbi->elmdb_env->txn_cond);
  enif_mutex_unlock(elmdb_dbi->elmdb_env->txn_lock);
  return ATOM_OK;
}


static MDB_txn* elmdb_update_get_handler(MDB_txn *txn, OpEntry *op) {
  k_args *args = (k_args*)op->args;
  MDB_val val;
  ElmdbTxn *elmdb_txn;
  ERL_NIF_TERM term_val, term_txn;
  unsigned char *bin;
  int ret;
  if((ret = mdb_txn_begin(args->elmdb_dbi->elmdb_env->env, NULL, 0, &txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    return NULL;
  }
  if((ret = mdb_get(txn, args->elmdb_dbi->dbi, &args->key, &val)) != MDB_SUCCESS) {
    if(ret == MDB_NOTFOUND) { SEND(op, ATOM_NOT_FOUND); }
    else { SEND_ERRNO(op, ret); }
    goto err;
  }
  if((elmdb_txn = enif_alloc_resource(elmdb_txn_res, sizeof(ElmdbTxn))) == NULL) {
    SEND_ERRNO(op, ENOMEM);
    goto err;
  }
  bin = enif_make_new_binary(op->msg_env, val.mv_size, &term_val);
  if(bin == NULL) {
    SEND_ERRNO(op, ENOMEM);
    goto err;
  }
  memcpy(bin, val.mv_data, val.mv_size);
  elmdb_txn->elmdb_env = args->elmdb_dbi->elmdb_env;
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  elmdb_txn->ref = ++elmdb_txn->elmdb_env->txn_ref_cnt;
  elmdb_txn->elmdb_env->active_txn_ref = elmdb_txn->ref;
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  term_txn = enif_make_resource(op->msg_env, elmdb_txn);
  enif_release_resource(elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  enif_keep_resource(elmdb_txn->elmdb_env);
  SEND(op, enif_make_tuple3(op->msg_env, ATOM_OK, term_val, term_txn));
  return txn;

 err:
  mdb_txn_abort(txn);
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM elmdb_update_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_async_get(env, argc, argv, elmdb_update_get_handler);
}

static MDB_txn* elmdb_update_put_handler(MDB_txn *txn, OpEntry *op) {
  kv_args *args = (kv_args*)op->args;
  int ret;
  if((ret = mdb_put(txn, args->elmdb_dbi->dbi, &args->key, &args->val, 0)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    mdb_txn_abort(txn);
    goto done;
  }
  if((ret = mdb_txn_commit(txn)) != MDB_SUCCESS) {
    SEND_ERRNO(op, ret);
    goto done;

  }
  SEND(op, ATOM_OK);

 done:
  enif_release_resource(args->elmdb_txn);
  enif_release_resource(args->elmdb_dbi);
  return NULL;
}

static ERL_NIF_TERM elmdb_update_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return do_txn_put(env, argc, argv, elmdb_update_put_handler);
}


/**
 * Put value indexed by key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
static ERL_NIF_TERM elmdb_put(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM err;
  ErlNifBinary key;
  ErlNifBinary val;
  MDB_val mkey;
  MDB_val mdata;
  MDB_txn * txn;
  int ret;

  if(!(argc == 3 &&
       enif_get_resource(env, argv[0], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_inspect_binary(env, argv[1], &key) &&
       enif_inspect_binary(env, argv[2], &val))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  mdata.mv_size = val.size;
  mdata.mv_data = val.data;

  CHECK(mdb_txn_begin(elmdb_dbi->elmdb_env->env, NULL, 0, &txn), err1);
  CHECK(mdb_put(txn, elmdb_dbi->dbi, &mkey, &mdata, 0), err2);
  CHECK(mdb_txn_commit(txn), err1);

  return ATOM_OK;

 err2:
  mdb_txn_abort(txn);
 err1:
  return err;
}


/**
 * Put a value indexed by a new key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 * argv[2]    value as an Erlang binary
 */
static ERL_NIF_TERM elmdb_put_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM err;
  ErlNifBinary key;
  ErlNifBinary val;
  MDB_val mkey;
  MDB_val mdata;
  MDB_txn * txn;
  int ret;

  if(!(argc == 3 &&
       enif_get_resource(env, argv[0], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_inspect_binary(env, argv[1], &key) &&
       enif_inspect_binary(env, argv[2], &val))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;
  mdata.mv_size = val.size;
  mdata.mv_data = val.data;

  CHECK(mdb_txn_begin(elmdb_dbi->elmdb_env->env, NULL, 0, & txn), err1);
  ret = mdb_put(txn, elmdb_dbi->dbi, &mkey, &mdata, MDB_NOOVERWRITE);
  if(MDB_KEYEXIST == ret) {
    mdb_txn_abort(txn);
    return ATOM_EXISTS;
  }
  if(ret != MDB_SUCCESS)
    FAIL_ERRNO(ret, err2);

  CHECK(mdb_txn_commit(txn), err1);

  return ATOM_OK;

 err2:
  mdb_txn_abort(txn);
 err1:
  return err;
}


/**
 * Retrieve the value associated with the key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 */
static ERL_NIF_TERM elmdb_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ErlNifBinary key;
  ERL_NIF_TERM val;
  unsigned char *bin;
  MDB_val mkey;
  MDB_val mdata;
  MDB_txn *txn;
  int ret;

  if(!(argc == 2 &&
       enif_get_resource(env, argv[0], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_inspect_binary(env, argv[1], &key))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;

  if((ret = mdb_txn_begin(elmdb_dbi->elmdb_env->env, NULL, MDB_RDONLY, &txn)) != MDB_SUCCESS)
    return ERRNO(ret);

  ret = mdb_get(txn, elmdb_dbi->dbi, &mkey, &mdata);
  mdb_txn_abort(txn);
  if(MDB_NOTFOUND == ret) {
    return ATOM_NOT_FOUND;
  }
  if(ret != MDB_SUCCESS)
    return ERRNO(ret);

  bin = enif_make_new_binary(env, mdata.mv_size, &val);
  if(bin == NULL)
    ERRNO(ENOMEM);
  memcpy(bin, mdata.mv_data, mdata.mv_size);

  return OK(val);
}

/**
 * Delete the value associated with the key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 */
static ERL_NIF_TERM elmdb_delete(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM err;
  ErlNifBinary key;
  MDB_val mkey;
  MDB_txn *txn;
  int ret;

  if(!(argc == 2 &&
       enif_get_resource(env, argv[0], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_inspect_iolist_as_binary(env, argv[1], &key))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;

  CHECK(mdb_txn_begin(elmdb_dbi->elmdb_env->env, NULL, 0, &txn), err1);

  ret = mdb_del(txn, elmdb_dbi->dbi, &mkey, NULL);
  if(MDB_NOTFOUND == ret) {
    mdb_txn_abort(txn);
    return ATOM_NOT_FOUND;
  }
  if(ret != MDB_SUCCESS)
    FAIL_ERRNO(ret, err2);

  CHECK(mdb_txn_commit(txn), err1);

  return ATOM_OK;

 err2:
  mdb_txn_abort(txn);
 err1:
  return err;
}

/**
 * Drop a MDB database.
 *
 * argv[0]    reference to the MDB handle resource
 */
static ERL_NIF_TERM elmdb_drop(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbDbi *elmdb_dbi;
  ERL_NIF_TERM err;
  MDB_txn *txn;
  int ret;

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_dbi_res, (void**)&elmdb_dbi))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_dbi->elmdb_env);
  CHECK(mdb_txn_begin(elmdb_dbi->elmdb_env->env, NULL, 0, &txn), err1);
  CHECK(mdb_drop(txn, elmdb_dbi->dbi, 0), err2);
  CHECK(mdb_txn_commit(txn), err1);

  return ATOM_OK;

 err2:
  mdb_txn_abort(txn);
 err1:
  return err;
}

static ERL_NIF_TERM elmdb_ro_txn_begin(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbEnv *elmdb_env;
  ElmdbRoTxn *ro_txn;
  ERL_NIF_TERM res;
  int ret;

  if((ro_txn = enif_alloc_resource(elmdb_ro_txn_res, sizeof(ElmdbRoTxn))) == NULL)
    return ERRNO(ENOMEM);

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_env_res, (void**)&elmdb_env))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(elmdb_env);
  if((ret = mdb_txn_begin(elmdb_env->env, NULL, MDB_RDONLY, &ro_txn->txn)) != MDB_SUCCESS)
    return ERRNO(ret);

  ro_txn->elmdb_env = elmdb_env;
  ro_txn->active = 1;
  res = enif_make_resource(env, ro_txn);
  enif_release_resource(ro_txn);
  enif_keep_resource(elmdb_env);
  return OK(res);
}


/**
 * Retrieve the value associated with the key.
 *
 * argv[0]    txn handle
 * argv[1]    dbi handle
 * argv[2]    key as an Erlang binary
 */
static ERL_NIF_TERM elmdb_ro_txn_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoTxn *ro_txn;
  ElmdbDbi *elmdb_dbi;
  ErlNifBinary key;
  ERL_NIF_TERM val;
  unsigned char *bin;
  MDB_val mkey;
  MDB_val mdata;
  int ret;

  if(!(argc == 3 &&
       enif_get_resource(env, argv[0], elmdb_ro_txn_res, (void**)&ro_txn) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi) &&
       enif_inspect_binary(env, argv[2], &key))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(ro_txn->elmdb_env);
  if(ro_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;
  if(ro_txn->active == 0)
    return ERR(ATOM_TXN_CLOSED);

  mkey.mv_size  = key.size;
  mkey.mv_data  = key.data;

  ret = mdb_get(ro_txn->txn, elmdb_dbi->dbi, &mkey, &mdata);
  if(MDB_NOTFOUND == ret)
    return ATOM_NOT_FOUND;
  if(ret != MDB_SUCCESS)
    return ERRNO(ret);

  bin = enif_make_new_binary(env, mdata.mv_size, &val);
  if(bin == NULL)
    return ERRNO(ENOMEM);
  memcpy(bin, mdata.mv_data, mdata.mv_size);

  return OK(val);
}

/**
 * Retrieve the value associated with the key.
 *
 * argv[0]    reference to the MDB handle resource
 * argv[1]    key as an Erlang binary
 */
static ERL_NIF_TERM elmdb_ro_txn_cursor_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoTxn *ro_txn;
  ElmdbDbi *elmdb_dbi;
  ElmdbRoCur *ro_cur;
  ERL_NIF_TERM res;
  int ret;

  if((ro_cur = enif_alloc_resource(elmdb_ro_cur_res, sizeof(ElmdbCur))) == NULL)
    return ERRNO(ENOMEM);

  if(!(argc == 2 &&
       enif_get_resource(env, argv[0], elmdb_ro_txn_res, (void**)&ro_txn) &&
       enif_get_resource(env, argv[1], elmdb_dbi_res, (void**)&elmdb_dbi))) {
    return BADARG;
  }
  if(ro_txn->elmdb_env->ref != elmdb_dbi->elmdb_env->ref)
    return BADARG;
  LOCKED_CHECK_ENV(ro_txn->elmdb_env);
  if(ro_txn->active == 0)
    return ERR(ATOM_TXN_CLOSED);

  if((ret = mdb_cursor_open(ro_txn->txn, elmdb_dbi->dbi, &ro_cur->cursor)) != MDB_SUCCESS)
    return ERRNO(ret);
  ro_cur->elmdb_ro_txn = ro_txn;
  ro_cur->active = 1;
  res = enif_make_resource(env, ro_cur);
  enif_release_resource(ro_cur);
  enif_keep_resource(ro_txn);
  return OK(res);
}

static ERL_NIF_TERM elmdb_ro_txn_cursor_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoCur *ro_cur;

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_ro_cur_res, (void**)&ro_cur))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(ro_cur->elmdb_ro_txn->elmdb_env);
  if(ro_cur->active == 0)
    return ATOM_OK;

  mdb_cursor_close(ro_cur->cursor);
  ro_cur->active = 0;

  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_ro_txn_cursor_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoCur *ro_cur;
  int ret;
  MDB_val key;
  MDB_val val;
  ERL_NIF_TERM term_key;
  ERL_NIF_TERM term_val;
  unsigned char *bin_key;
  unsigned char *bin_val;
  int cursor_op;

  if(!(argc == 2 &&
       enif_get_resource(env, argv[0], elmdb_ro_cur_res, (void**)&ro_cur) &&
       (enif_is_atom(env, argv[1]) || enif_is_tuple(env, argv[1])))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(ro_cur->elmdb_ro_txn->elmdb_env);
  if(ro_cur->active == 0)
    return ERR(ATOM_CUR_CLOSED);
  if(ro_cur->elmdb_ro_txn->active == 0)
    return ERR(ATOM_TXN_CLOSED);

  cursor_op = to_mdb_cursor_op(env, argv[1], &key);
  ret = mdb_cursor_get(ro_cur->cursor, &key, &val, cursor_op);
  if(ret == MDB_NOTFOUND) { return ATOM_NOT_FOUND; }
  if(ret != MDB_SUCCESS) { return ERRNO(ret); }

  bin_key = enif_make_new_binary(env, key.mv_size, &term_key);
  if(bin_key == NULL)
    return ERRNO(ENOMEM);
  memcpy(bin_key, key.mv_data, key.mv_size);

  bin_val = enif_make_new_binary(env, val.mv_size, &term_val);
  if(bin_val == NULL)
    return ERRNO(ENOMEM);
  memcpy(bin_val, val.mv_data, val.mv_size);

  return enif_make_tuple3(env, ATOM_OK, term_key, term_val);
}

static ERL_NIF_TERM elmdb_ro_txn_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoTxn *ro_txn;
  int ret;

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_ro_txn_res, (void**)&ro_txn))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(ro_txn->elmdb_env);
  if(ro_txn->active == 0)
    return ERR(ATOM_TXN_CLOSED);

  if((ret = mdb_txn_commit(ro_txn->txn)) != MDB_SUCCESS)
    return ERRNO(ret);

  ro_txn->active = 0;

  return ATOM_OK;
}

static ERL_NIF_TERM elmdb_ro_txn_abort(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  ElmdbRoTxn *ro_txn;

  if(!(argc == 1 &&
       enif_get_resource(env, argv[0], elmdb_ro_txn_res, (void**)&ro_txn))) {
    return BADARG;
  }
  LOCKED_CHECK_ENV(ro_txn->elmdb_env);
  if(ro_txn->active == 0)
    return ERR(ATOM_TXN_CLOSED);

  mdb_txn_abort(ro_txn->txn);
  ro_txn->active = 0;
  return ATOM_OK;
}

static void elmdb_env_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbEnv *elmdb_env = (ElmdbEnv*)resource;
  enif_mutex_lock(elmdb_env->txn_lock);
  if(elmdb_env->shutdown == 0) {
    elmdb_env->shutdown = 1;
    enif_cond_signal(elmdb_env->txn_cond);
    enif_mutex_unlock(elmdb_env->txn_lock);
    enif_thread_join(elmdb_env->tid, NULL);
    enif_mutex_destroy(elmdb_env->op_lock);
    enif_mutex_destroy(elmdb_env->txn_lock);
    enif_cond_destroy(elmdb_env->txn_cond);
  } else enif_mutex_unlock(elmdb_env->txn_lock);
}

static void elmdb_dbi_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbDbi *elmdb_dbi = (ElmdbDbi*)resource;
  if(elmdb_dbi->name != NULL)
    enif_free(elmdb_dbi->name);
  enif_release_resource(elmdb_dbi->elmdb_env);
}

static void elmdb_txn_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbTxn *elmdb_txn = (ElmdbTxn*)resource;
  enif_mutex_lock(elmdb_txn->elmdb_env->txn_lock);
  if(elmdb_txn->ref == elmdb_txn->elmdb_env->active_txn_ref)
    elmdb_txn->elmdb_env->active_txn_ref = 0;
  enif_mutex_unlock(elmdb_txn->elmdb_env->txn_lock);
  enif_release_resource(elmdb_txn->elmdb_env);
}

static void elmdb_ro_txn_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbRoTxn *elmdb_ro_txn = (ElmdbRoTxn*)resource;
  enif_mutex_lock(elmdb_ro_txn->elmdb_env->txn_lock);
  if(elmdb_ro_txn->elmdb_env->shutdown == 0 && elmdb_ro_txn->active == 1) {
    enif_mutex_unlock(elmdb_ro_txn->elmdb_env->txn_lock);
    mdb_txn_abort(elmdb_ro_txn->txn);
  } else enif_mutex_unlock(elmdb_ro_txn->elmdb_env->txn_lock);
  elmdb_ro_txn->active = 0;
  enif_release_resource(elmdb_ro_txn->elmdb_env);
}

static void elmdb_cur_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbCur *elmdb_cur = (ElmdbCur*)resource;
  elmdb_cur->active = 0;
}

static void elmdb_ro_cur_dtor(ErlNifEnv *env, void *resource) {
  __UNUSED(env);
  ElmdbRoCur *elmdb_ro_cur = (ElmdbRoCur*)resource;
  enif_mutex_lock(elmdb_ro_cur->elmdb_ro_txn->elmdb_env->txn_lock);
  if(elmdb_ro_cur->elmdb_ro_txn->elmdb_env->shutdown == 0 && elmdb_ro_cur->active == 1) {
    enif_mutex_unlock(elmdb_ro_cur->elmdb_ro_txn->elmdb_env->txn_lock);
    mdb_cursor_close(elmdb_ro_cur->cursor);
  } else enif_mutex_unlock(elmdb_ro_cur->elmdb_ro_txn->elmdb_env->txn_lock);
  elmdb_ro_cur->active = 0;
  enif_release_resource(elmdb_ro_cur->elmdb_ro_txn);
}

static int elmdb_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
  __UNUSED(load_info);

  ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;

  ElmdbPriv *priv = enif_alloc(sizeof(ElmdbPriv));
  if(priv == NULL)
    return ENOMEM;
  SLIST_INIT(&priv->env_list);
  if((priv->env_lock = enif_mutex_create("env_lock")) == NULL)
    return ENOMEM;
  priv->env_ref = 0;
  *priv_data = priv;

  ATOM_ELMDB = enif_make_atom(env, "elmdb");

  // mdb_env_open flags
  ATOM_FIXEDMAP = enif_make_atom(env, "fixed_map");
  ATOM_NOSUBDIR = enif_make_atom(env, "no_subdir");
  ATOM_RDONLY = enif_make_atom(env, "read_only");
  ATOM_WRITEMAP = enif_make_atom(env, "write_map");
  ATOM_NOMETASYNC = enif_make_atom(env, "no_meta_sync");
  ATOM_NOSYNC = enif_make_atom(env, "no_sync");
  ATOM_MAPASYNC = enif_make_atom(env, "map_async");
  ATOM_NORDAHEAD = enif_make_atom(env, "no_read_ahead");
  ATOM_NOMEMINIT = enif_make_atom(env, "no_mem_init");

  ATOM_MAPSIZE = enif_make_atom(env, "map_size");
  ATOM_MAXDBS = enif_make_atom(env, "max_dbs");

  // mdb_db_open flags
  ATOM_REVERSEKEY = enif_make_atom(env, "reverse_key");
  ATOM_DUPSORT = enif_make_atom(env, "dup_sort");
  ATOM_REVERSEDUP = enif_make_atom(env, "reverse_dup");
  ATOM_CREATE = enif_make_atom(env, "create");

  // return values
  ATOM_ERROR = enif_make_atom(env, "error");
  ATOM_ENV_CLOSED = enif_make_atom(env, "env_closed");
  ATOM_TXN_CLOSED = enif_make_atom(env, "txn_closed");
  ATOM_CUR_CLOSED = enif_make_atom(env, "cur_closed");
  ATOM_ENV_NOT_FOUND = enif_make_atom(env, "env_not_found");
  ATOM_BADARG = enif_make_atom(env, "badarg");
  ATOM_PANIC = enif_make_atom(env, "panic");
  ATOM_OK = enif_make_atom(env, "ok");
  ATOM_NOT_FOUND = enif_make_atom(env, "not_found");
  ATOM_EXISTS = enif_make_atom(env, "exists");

  ATOM_KEYEXIST = enif_make_atom(env, "key_exist");
  ATOM_CORRUPTED = enif_make_atom(env, "corrupted");
  ATOM_PANIC = enif_make_atom(env, "panic");
  ATOM_VERSION_MISMATCH = enif_make_atom(env, "version_mismatch");
  ATOM_MAP_FULL = enif_make_atom(env, "map_full");
  ATOM_DBS_FULL = enif_make_atom(env, "dbs_full");
  ATOM_READERS_FULL = enif_make_atom(env, "readers_full");
  ATOM_TLS_FULL = enif_make_atom(env, "tls_full");
  ATOM_TXN_FULL = enif_make_atom(env, "txn_full");
  ATOM_CURSOR_FULL = enif_make_atom(env, "cursor_full");
  ATOM_PAGE_FULL = enif_make_atom(env, "page_full");
  ATOM_MAP_RESIZED = enif_make_atom(env, "map_resized");
  ATOM_INCOMPATIBLE = enif_make_atom(env, "incompatible");
  ATOM_BAD_RSLOT = enif_make_atom(env, "bad_rslot");

  ATOM_TXN_STARTED = enif_make_atom(env, "txn_started");
  ATOM_TXN_NOT_STARTED = enif_make_atom(env, "txn_not_started");

  // Cursor ops
  ATOM_FIRST = enif_make_atom(env, "first");
  ATOM_FIRST_DUP = enif_make_atom(env, "first_dup");
  ATOM_GET_BOTH = enif_make_atom(env, "get_both");
  ATOM_GET_BOTH_RANGE = enif_make_atom(env, "get_both_range");
  ATOM_GET_CURRENT = enif_make_atom(env, "get_current");
  ATOM_LAST = enif_make_atom(env, "last");
  ATOM_LAST_DUP = enif_make_atom(env, "last_dup");
  ATOM_NEXT = enif_make_atom(env, "next");
  ATOM_NEXT_DUP = enif_make_atom(env, "next_dup");
  ATOM_NEXT_NODUP = enif_make_atom(env, "next_nodup");
  ATOM_PREV = enif_make_atom(env, "prev");
  ATOM_PREV_DUP = enif_make_atom(env, "prev_dup");
  ATOM_PREV_NODUP = enif_make_atom(env, "prev_nodup");
  ATOM_SET = enif_make_atom(env, "set");
  ATOM_SET_RANGE = enif_make_atom(env, "set_range");

  elmdb_env_res = enif_open_resource_type(env, NULL, "elmdb_env_res", elmdb_env_dtor, flags, NULL);
  elmdb_dbi_res = enif_open_resource_type(env, NULL, "elmdb_dbi_res", elmdb_dbi_dtor, flags, NULL);
  elmdb_txn_res = enif_open_resource_type(env, NULL, "elmdb_txn_res", elmdb_txn_dtor, flags, NULL);
  elmdb_cur_res = enif_open_resource_type(env, NULL, "elmdb_cur_res", elmdb_cur_dtor, flags, NULL);
  elmdb_ro_txn_res = enif_open_resource_type(env, NULL, "elmdb_ro_txn_res", elmdb_ro_txn_dtor, flags, NULL);
  elmdb_ro_cur_res = enif_open_resource_type(env, NULL, "elmdb_ro_cur_res", elmdb_ro_cur_dtor, flags, NULL);
  return (0);
}

static int elmdb_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv, ERL_NIF_TERM load_info)
{
  __UNUSED(env);
  __UNUSED(priv_data);
  __UNUSED(old_priv);
  __UNUSED(load_info);
  return (0); // TODO:
}


static void elmdb_unload(ErlNifEnv* env, void* priv_data)
{
  __UNUSED(env);
  ElmdbPriv *priv = (ElmdbPriv*)priv_data;
  close_all(priv);
  enif_free(priv);
  return;
}

static ErlNifFunc nif_funcs [] = {
  {"nif_env_open",          3, elmdb_env_open, 0},
  {"env_close",             1, elmdb_env_close, 0},
  {"nif_env_close_by_name", 1, elmdb_env_close_by_name, 0},
  {"env_close_all",         0, elmdb_env_close_all, 0},
  {"nif_db_open",           4, elmdb_db_open, 0},

  {"put",      3, elmdb_put, 0},
  {"put_new",  3, elmdb_put_new, 0},
  {"get",      2, elmdb_get, 0},
  {"delete",   2, elmdb_delete, 0},
  {"drop",     1, elmdb_drop, 0},

  {"nif_async_put",     4, elmdb_async_put, 0},
  {"nif_async_put_new", 4, elmdb_async_put_new, 0},
  {"nif_async_get",     3, elmdb_async_get, 0},
  {"nif_async_delete",  3, elmdb_async_delete, 0},
  {"nif_async_drop",    2, elmdb_async_drop, 0},
  {"nif_update_put",    5, elmdb_update_put, 0},
  {"nif_update_get",    3, elmdb_update_get, 0},

  {"ro_txn_begin",  1, elmdb_ro_txn_begin, 0},
  {"ro_txn_get",    3, elmdb_ro_txn_get, 0},
  {"ro_txn_commit", 1, elmdb_ro_txn_commit, 0},
  {"ro_txn_abort",  1, elmdb_ro_txn_abort, 0},

  {"ro_txn_cursor_open",  2, elmdb_ro_txn_cursor_open, 0},
  {"ro_txn_cursor_close", 1, elmdb_ro_txn_cursor_close, 0},
  {"ro_txn_cursor_get",   2, elmdb_ro_txn_cursor_get, 0},

  {"nif_txn_begin",   2,  elmdb_txn_begin, 0},
  {"nif_txn_put",     5,  elmdb_txn_put, 0},
  {"nif_txn_put_new", 5,  elmdb_txn_put_new, 0},
  {"nif_txn_get",     4,  elmdb_txn_get, 0},
  {"nif_txn_delete",  4,  elmdb_txn_delete, 0},
  {"nif_txn_drop",    3,  elmdb_txn_drop, 0},
  {"nif_txn_commit",  2, elmdb_txn_commit, 0},
  {"nif_txn_abort",   2,  elmdb_txn_abort, 0},

  {"nif_txn_cursor_open", 3, elmdb_txn_cursor_open, 0},
  {"nif_txn_cursor_get",  3, elmdb_txn_cursor_get, 0},
  {"nif_txn_cursor_put",  4, elmdb_txn_cursor_put, 0}
};

/* driver entry point */
ERL_NIF_INIT(elmdb,
             nif_funcs,
             & elmdb_load,
             NULL,
             & elmdb_upgrade,
             & elmdb_unload)
