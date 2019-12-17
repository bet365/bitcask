%%%-------------------------------------------------------------------
%%% @author dylanmitelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Dec 2019 10:52
%%%-------------------------------------------------------------------
-module(bitcask_manager).
-author("dylanmitelo").

%% API
-export([
	open/1,
	open/2,
	open/3,
	close/1,
	put/3,
	put/4,
	get/3,
	list_keys/2,
	merge/4,
	make_bitcask_key/3,
	check_and_upgrade_key/2
]).

-define(VERSION_0, 0).
-define(VERSION_1, 1).
-define(VERSION_2, 2).
-define(VERSION_3, 3).

-define(ENCODE_BITCASK_KEY, fun make_bitcask_key/3).
-define(DECODE_BITCASK_KEY, fun make_riak_key/1).
-define(FIND_SPLIT_FUN,
	fun(Key) ->
		case ?DECODE_BITCASK_KEY(Key) of
			{Split, {_Type, _Bucket}, _Key} ->
				binary_to_atom(Split, latin1);
			{Split, _Bucket, _Key} ->
				binary_to_atom(Split, latin1);
			{{_Type, Bucket}, _Key} ->
				binary_to_atom(Bucket, latin1);
			{Bucket, _Key} ->
				binary_to_atom(Bucket, latin1);
			_ ->
				default
		end
	end ).
-define(CHECK_AND_UPGRADE_KEY, fun check_and_upgrade_key/2).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
	open_instances 	:: [{atom(), reference()}],
	open_dirs		:: [{atom(), string()}]
}).


%%%===================================================================
%%% API
%%%===================================================================


open(Dir) ->
	open(Dir, []).
open(Dir, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
	BitcaskRef = bitcask:open(NewDir, Opts),
	State = #state{open_instances = [{Split, BitcaskRef}], open_dirs = [{Split, NewDir}]},
	Ref = make_ref(),
	erlang:put(Ref, State),
	Ref.
open(Ref, Dir, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,
	Split = proplists:get_value(split, Opts, default),

	case proplists:get_value(Split, OpenInstances) of
		undefined ->
			NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
			BitcaskRef = bitcask:open(NewDir, Opts),
			State1 = State#state{
				open_instances 	= [{Split, BitcaskRef} | OpenInstances],
				open_dirs 		= [{Split, NewDir} | OpenDirs]},
			erlang:put(Ref, State1);
		_ ->
			io:format("Bitcask instance already open for Dir: ~p and Split: ~p~n", [Dir, Split]),
			Ref
	end.

close(Ref) ->
	State = erlang:get(Ref),
	[bitcask:close(BitcaskRef) || {_, BitcaskRef} <- State#state.open_instances].

%%get(Ref, Key) ->
%%	get(Ref, Key, []).
get(Ref, {Bucket, Key}, Opts) ->
	Split = proplists:get_value(split, Opts),
	NewKey = make_bitcask_key(3, {Bucket, atom_to_binary(Split, latin1)}, Key),
	get(Ref, NewKey, Opts);
get(Ref, Key1, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
%%	Split = ?FIND_SPLIT_FUN(Key1),
	Split = proplists:get_value(split, Opts),

	case proplists:get_value(Split, OpenInstances) of
		undefined ->
			BitcaskRef = proplists:get_value(default, OpenInstances),
			bitcask:get(BitcaskRef, Key1);
		BitcaskRef ->
			bitcask:get(BitcaskRef, Key1)
	end.

put(Ref, Key, Value) ->
	put(Ref, Key, Value, []).
put(Ref, {Bucket, Key}, Value, Opts) ->
	Split = proplists:get_value(split, Opts),
	NewKey = make_bitcask_key(3, {Bucket, atom_to_binary(Split, latin1)}, Key),
	put(Ref, NewKey, Value, Opts);
put(Ref, Key1, Value, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
%%	Split = ?FIND_SPLIT_FUN(Key1),
	Split = proplists:get_value(split, Opts),

	case proplists:get_value(Split, OpenInstances) of
		undefined ->
			BitcaskRef = proplists:get_value(default, OpenInstances),
			bitcask:put(BitcaskRef, Key1, Value, Opts);
		BitcaskRef ->
			bitcask:put(BitcaskRef, Key1, Value, Opts)
	end.

list_keys(Ref, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	case Split of
		all ->
			lists:flatten([bitcask:list_keys(Ref0) || {_Split0, Ref0} <- OpenInstances]);
		_ ->
			BitcaskRef = proplists:get_value(Split, OpenInstances),
			bitcask:list_keys(BitcaskRef)
	end.


merge(Ref, _Dirname, Opts, FilesToMerge) ->
	State = erlang:get(Ref),
	_OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,

	case proplists:get_value(special_merge, Opts) of
		undefined ->
			[bitcask:merge(Dir, Opts, FilesToMerge) || {_Split, Dir} <- OpenDirs];
		true ->
			MergeFun = fun(Split, Key, Value) -> special_merge_fun(Split, Key, Value, Ref) end,
			NewOpts = [{special_merge_fun, MergeFun} | Opts],
			[bitcask:merge(Dir, [{current_split, Split} | NewOpts], FilesToMerge) || {Split, Dir} <- OpenDirs],
			ok
	end.

special_merge_fun(CurrentSplit, Key1, Value, Ref) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	NewSplit = ?FIND_SPLIT_FUN(Key1),
	case NewSplit of
		CurrentSplit ->
			ok;
		_ ->
			case proplists:get_value(NewSplit, OpenInstances) of
				undefined ->
					ok;
				BitcaskRef ->
					bitcask:put(BitcaskRef, Key1, Value)
			end
	end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_bitcask_key(0, Bucket, Key) ->
	term_to_binary({Bucket, Key});
make_bitcask_key(1, {Type, Bucket}, Key) ->
	TypeSz = size(Type),
	BucketSz = size(Bucket),
	<<?VERSION_1:7, 1:1, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bitcask_key(1, Bucket, Key) ->
	BucketSz = size(Bucket),
	<<?VERSION_1:7, 0:1, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bitcask_key(2, {Type, Bucket}, Key) ->
	TypeSz = size(Type),
	BucketSz = size(Bucket),
	<<?VERSION_2:7, 1:1, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bitcask_key(2, Bucket, Key) ->
	BucketSz = size(Bucket),
	<<?VERSION_2:7, 0:1, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bitcask_key(3, {Type, Bucket, Split}, Key) ->
	TypeSz = size(Type),
	BucketSz = size(Bucket),
	SplitSz = size(Split),
	<<?VERSION_3:7, 1:1, SplitSz:16/integer, Split/binary, TypeSz:16/integer, Type/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bitcask_key(3, {Bucket, Split}, Key) ->
	SplitSz = size(Split),
	BucketSz = size(Bucket),
	<<?VERSION_3:7, 0:1, SplitSz:16/integer, Split/binary, BucketSz:16/integer, Bucket/binary, Key/binary>>.

make_riak_key(<<?VERSION_3:7, HasType:1, SplitSz:16/integer, Split:SplitSz/bytes, Sz:16/integer,
	TypeOrBucket:Sz/bytes, Rest/binary>>) ->
	case HasType of
		0 ->
			%% no type, first field is bucket
			{Split, TypeOrBucket, Rest};
		1 ->
			%% has a tyoe, extract bucket as well
			<<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
			{Split, {TypeOrBucket, Bucket}, Key}
	end;
make_riak_key(<<?VERSION_2:7, HasType:1, Sz:16/integer,
	TypeOrBucket:Sz/bytes, Rest/binary>>) ->
	case HasType of
		0 ->
			%% no type, first field is bucket
			{TypeOrBucket, Rest};
		1 ->
			%% has a tyoe, extract bucket as well
			<<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
			{{TypeOrBucket, Bucket}, Key}
	end;
make_riak_key(<<?VERSION_1:7, HasType:1, Sz:16/integer,
	TypeOrBucket:Sz/bytes, Rest/binary>>) ->
	case HasType of
		0 ->
			%% no type, first field is bucket
			{TypeOrBucket, Rest};
		1 ->
			%% has a tyoe, extract bucket as well
			<<BucketSz:16/integer, Bucket:BucketSz/bytes, Key/binary>> = Rest,
			{{TypeOrBucket, Bucket}, Key}
	end;
make_riak_key(<<?VERSION_0:8,_Rest/binary>> = BK) ->
	binary_to_term(BK);
make_riak_key(BK) when is_binary(BK) ->
	BK.


check_and_upgrade_key(default, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
	KeyDirKey;
check_and_upgrade_key(Split, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
	case ?DECODE_BITCASK_KEY(KeyDirKey) of
		{{Bucket, Type}, Key} ->
			?ENCODE_BITCASK_KEY(3, {Type, Bucket, atom_to_binary(Split, latin1)}, Key);
		{Bucket, Key} ->
			NewKey = ?ENCODE_BITCASK_KEY(3, {Bucket, atom_to_binary(Split, latin1)}, Key),
			NewKey
	end;
check_and_upgrade_key(_Split, <<?VERSION_3:7, _Rest/bitstring>> = KeyDirKey) ->
	KeyDirKey;
check_and_upgrade_key(_Split, KeyDirKey) ->
	KeyDirKey.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

manager_merge_test() ->
	os:cmd("rm -rf /tmp/bc.man.merge"),
	Dir = "/tmp/bc.man.merge",
	Key1 = make_bitcask_key(3, {<<"b1">>, <<"default">>}, <<"k1">>),
	Key2 = make_bitcask_key(3, {<<"b2">>, <<"default">>}, <<"k2">>),
	Key3 = make_bitcask_key(3, {<<"b3">>, <<"default">>}, <<"k3">>),
	Key4 = make_bitcask_key(3, {<<"b4">>, <<"second">>}, <<"k4">>),
	Key5 = make_bitcask_key(3, {<<"b5">>, <<"second">>}, <<"k5">>),
	Key6 = make_bitcask_key(3, {<<"b6">>, <<"second">>}, <<"k6">>),
	_Keys = [Key1, Key2, Key3, Key4, Key5, Key6],
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 1}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 1}]),

	bitcask_manager:put(B, Key1, <<"Value1">>, [{split, default}]),
	bitcask_manager:put(B, Key2, <<"Value2">>, [{split, default}]),
	bitcask_manager:put(B, Key3, <<"Value3">>, [{split, default}]),
	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, default}]),
	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, default}]),
	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, default}]),

	bitcask_manager:put(B, Key4, <<"Value4">>, [{split, second}]),
	bitcask_manager:put(B, Key5, <<"Value5">>, [{split, second}]),
	bitcask_manager:put(B, Key6, <<"Value6">>, [{split, second}]),
	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),

%%	NewKeys = bitcask_manager:list_keys(B, [{split, all}]),
%%
%%	ct:pal("Keys: ~p~n", [Keys]),
%%	ct:pal("Fetched Keys: ~p~n", [NewKeys]),
%%
%%	?assertEqual(NewKeys, Keys),


	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,
	MergeFiles = [FileFun(X, default) || X <- lists:seq(1,3)],
	MergeFiles2 = [FileFun(X, second) || X <- lists:seq(1,3)],

	ct:pal("MergeFiles1: ~p~n", [MergeFiles]),
	ct:pal("MergeFiles2: ~p~n", [MergeFiles2]),

	ok.



-endif.