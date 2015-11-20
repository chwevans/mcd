# Incompatibility Warning:
This library is an incompatible fork of [EchoTeam/mcd](http://github.com/EchoTeam/mcd/) with very minor changes, designed to play well in a heterogeneous environment where multiple languages are reading and writing to memcache. It only handles binaries as keys and values and sets them in memcache unchanged, all encoding of keys and values is up to you. This library's dependecies have also been updated to use a more modern version of lager. 

Now on to the regularly scheduled docs.

# Erlang memcached client library.

This library provides two ways of working with memcached servers. One way is to
use a single server, as described in *#1*. Another way is to use a number of
memcached servers in a consistent hash ring mode. This is described in *#2*.


## 1. RUNNING IN SINGLE-SERVER MODE

### 1.1  Starting a single memcached

The following will start an anonymous mcd client, addressable by the returned pid().

    {ok, Pid} = mcd:start_link(["localhost", 11211]).

The next one will connect to the memcached server on the local host, and will
also register the mcd instance under the 'myMcd' name. See *#3*.  The mcd API
can use the returned pid() and the registered name interchangeably.

    mcd:start_link(myMcd, []).
    mcd:set(myMcd, a, b).
    mcd:get(myMcd, a).

### 1.2  A ChildSpec for running the mcd named 'myMcd' under a supervisor

    {local_memcache,
        {mcd, start_link, ['myMcd', ["localhost", 11211]]},
        permanent, 10000, worker, [mcd] }

## 2. RUNNING IN CLUSTERED MODE

*mcd_cluster* starts a set of mcd processess and a dispatcher attached to them.
All mcd processess and the dispatcher are linked to form a proper exit hierarchy.

### 2.1  Starting memcached cluster from the command line (e.g., for testing)

    mcd_cluster:start_link(mainCluster, [
        {host1, ["localhost", 11211], 10},
        {host2, ["localhost", 11211], 20}
    ]).

### 2.2  A ChildSpec for running the cluster named 'mainCluster' under a supervisor

    {mainCluster,
        {mcd_cluster, start_link, [mainCluster, [
            {host1, ["localhost"], 10},
            {host2, ["localhost"], 20}
        ]]},
        permanent, 60000, worker, [mcd_cluster]}

Note: the default port for "localhost" is 11211.

### 2.3 Starting memcached cluster as an application

    application:set_env(mcd, mcd_hosts, [{localhost, ["localhost"]}]).
    application:set_env(mcd, mcd_buckets, [
        {cluster1, {localhost, 11211}},
        {cluster2, {localhost, 11211}}
    ]).
    application:start(mcd).

    {error, notfound} = mcd:get(cluster1, foo).
    {error, notfound} = mcd:get(cluster2, foo).

## 3. USING MEMCACHED

    {ok, b} = mcd:set(mainCluster, a, b).
    {ok,<<"b">>} = mcd:get(mainCluster, a).
    {error, notfound} = mcd:get(mainCluster, foo).
    {ok,"*"} = mcd:set(mainCluster, <<"bar">>, [42]).
    {ok,<<"*">>} = mcd:get(mainCluster, <<"bar">>).
    {ok,[{<<"a">>,<<"b">>},{<<"bar">>,<<"*">>}]} = mcd:get_multi(mainCluster, [a, <<"bar">>]).

## 4. RELEASE NOTES

See [CHANGELOG.md](CHANGELOG.md)
