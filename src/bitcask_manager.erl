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
	close/2,
	activate_split/2,
	deactivate_split/2,
	put/3,
	put/4,
	get/3,
	delete/2,
	delete/3,
	sync/1,
	list_keys/2,
	fold_keys/3,
	fold_keys/4,
	fold_keys/7,
	fold/3,
	fold/4,
	fold/7,
	merge/2,
	merge/3,
	special_merge/4,
	reverse_merge/4,
	needs_merge/1,
	needs_merge/2,
	check_backend_exists/2,
	is_active/2,
	has_merged/2,
	is_empty_estimate/1,
	status/1
]).

-include("bitcask.hrl").

-define(VERSION_0, 0).
-define(VERSION_1, 1).
-define(VERSION_2, 2).
-define(VERSION_3, 3).

-ifdef(namespaced_types).
-type bitcask_set() :: sets:set().
-else.
-type bitcask_set() :: set().
-endif.

-record(mstate, {origin_dirname :: string(),
	destination_dirname :: string(),
	origin_splitname :: atom(),
	destination_splitname :: atom(),
	manager_ref :: reference(),
	merge_lock :: reference(),
	max_file_size :: integer(),
	input_files :: [#filestate{}],
	input_file_ids :: bitcask_set(),
	min_file_id :: non_neg_integer(),
	tombstone_write_files :: [#filestate{}],
	out_file :: 'fresh' | #filestate{},
	merge_coverage :: prefix | partial | full,
	origin_live_keydir :: reference(),
	destination_live_keydir :: reference(),
	del_keydir :: reference(),
	expiry_time :: integer(),
	expiry_grace_time :: integer(),
	decode_disk_key_fun :: function(),
	encode_disk_key_fun :: function(),
	find_split_fun :: function(),
	upgrade_key_fun :: function(),
	upgrade_key :: boolean(),
	partition :: integer(),
	read_write_p :: integer(),    % integer() avoids atom -> NIF
	opts :: list(),
	delete_files :: [#filestate{}],
	reverse_merge :: boolean()}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
	open_instances :: [{atom(), reference(), boolean(), boolean()}], %% {split, ref, has_merged, is_activated}
	open_dirs :: [{atom(), string()}]
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec open(string()) -> reference().
open(Dir) ->
	open(Dir, []).
-spec open(string(), list()) -> reference().
open(Dir, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
	BitcaskRef = bitcask:open(NewDir, Opts),
	State = #state{open_instances = [{Split, BitcaskRef, true, true}], open_dirs = [{Split, NewDir}]},
	Ref = make_ref(),
	erlang:put(Ref, State),
	Ref.
-spec open(reference(), string(), list()) -> reference().
open(Ref, Dir, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,
	Split = proplists:get_value(split, Opts, default),
	IsActive = proplists:get_value(is_active, Opts, false),
	HasMerged = proplists:get_value(has_merged, Opts, false),

	case lists:keyfind(Split, 1, OpenInstances) of
		false ->
			NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
			BitcaskRef = bitcask:open(NewDir, Opts),
			State1 = State#state{
				open_instances = [{Split, BitcaskRef, HasMerged, IsActive} | OpenInstances],
				open_dirs = [{Split, NewDir} | OpenDirs]},
			erlang:put(Ref, State1),
			Ref;
		_ ->
			io:format("Bitcask instance already open for Dir: ~p and Split: ~p~n", [Dir, Split]),
			Ref
	end.

-spec close(reference()) -> ok.
close(Ref) ->
	State = erlang:get(Ref),
	[bitcask:close(BitcaskRef) || {_, BitcaskRef, _, _} <- State#state.open_instances],
	ok.
-spec close(reference(), string()) -> reference().
close(Ref, Split) ->
	State = erlang:get(Ref),
	{Split, SplitRef, _, DeactivedState} = lists:keyfind(Split, 1, State#state.open_instances),
	{Split, SplitDir} = lists:keyfind(Split, 1, State#state.open_dirs),
	case DeactivedState of
		true ->
			error;
		_ -> %% Allow removal if in either false or eliminate?
			bitcask:close(SplitRef),
			NewOpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
			NewOpenDirs = lists:keydelete(Split, 1, State#state.open_dirs),
			NewState = State#state{open_instances = NewOpenInstances, open_dirs = NewOpenDirs},
			erlang:put(Ref, NewState),

			%% Clean up dir location
			os:cmd("rm -rf " ++ SplitDir)
	end,
	Ref.

-spec activate_split(reference(), atom()) -> reference().
activate_split(Ref, Split) ->
	State = erlang:get(Ref),
	{Split, SplitRef, HasMerged, IsActive} = lists:keyfind(Split, 1, State#state.open_instances),

	case IsActive of
		true ->
			Ref;
		false ->
			{default, DefRef, _DefHasMerged, _DefIsActive} = lists:keyfind(default, 1, State#state.open_instances),
			DefState = erlang:get(DefRef),
			case DefState#bc_state.write_file of
				undefined ->
					OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
					NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
					erlang:put(Ref, NewState),
					Ref;
				fresh ->
					OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
					NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
					erlang:put(Ref, NewState),
					Ref;
				_ ->
					NewDefState = bitcask:wrap_write_file(DefState),
					erlang:put(DefRef, NewDefState),
					OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
					NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
					erlang:put(Ref, NewState),
					Ref
			end;
		_ ->
			{error, wrong_state}
	end.

-spec deactivate_split(reference(), atom()) -> reference() | error_cannot_deactivate.
deactivate_split(Ref, Split) ->
	State = erlang:get(Ref),

	case lists:keyfind(Split, 1, State#state.open_instances) of
		undefined ->
			io:format("Cannot deactivate split: ~p as it does not exist in backends: ~p~n", [Split, State]),
			error_cannot_deactivate;
		{Split, SplitRef, HasMerged, IsActive} ->
			case IsActive of
				true ->
					SplitState = erlang:get(SplitRef),
					case SplitState#bc_state.write_file of
						undefined ->
							OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
							NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, false} | OpenInstances]},
							erlang:put(Ref, NewState),
							Ref;
						fresh ->
							OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
							NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, false} | OpenInstances]},
							erlang:put(Ref, NewState),
							Ref;
						_ ->
							NewSplitState = bitcask:wrap_write_file(SplitState),
							erlang:put(SplitRef, NewSplitState),
							OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
							NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, false} | OpenInstances]},
							erlang:put(Ref, NewState),
							Ref
					end
			end;
		_ ->
			Ref
	end.

-spec get(reference(), binary(), list()) -> not_found | {ok, binary()} | {error, term()}.
get(Ref, Key1, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	Split1 = proplists:get_value(split, Opts, default),
	{default, DefRef, _DefMerge, _DefActive} = lists:keyfind(default, 1, OpenInstances),
	DefState = erlang:get(DefRef),
	{Split, SplitRef, _, _Active} =
		case lists:keyfind(Split1, 1, OpenInstances) of
			false ->
				{default, DefRef, _DefMerge, _DefActive};
			SplitData ->
				SplitData
		end,
	get2(Key1, Split, SplitRef, DefState, DefRef).

-spec get2(binary(), atom(), reference(), bitcask:state(), reference()) -> not_found | {ok, binary()} | {error, term()}.
get2(Key1, Split, SplitRef, DefState, DefRef) ->
	SplitState = erlang:get(SplitRef),
	case Split of    %% Avoids us doing a lookup twice if the request is for default data
		default ->
			case bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key1) of
				not_found ->
					not_found;
				Entry ->
					bitcask:check_get(Key1, Entry, SplitRef, SplitState, 2)
			end;
		_ ->
			case bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key1) of
				not_found ->
					case bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1) of
						not_found ->
							UpgradeKeyFun = bitcask:get_upgrade_key_fun(bitcask:get_opt(check_and_upgrade_key_fun, DefState#bc_state.opts)),
							NewDefKey = UpgradeKeyFun(default, Key1),
							case bitcask_nifs:keydir_get(DefState#bc_state.keydir, NewDefKey) of
								not_found ->
									not_found;
								Entry ->
									bitcask:check_get(NewDefKey, Entry, DefRef, DefState, 2)
							end;
						Entry ->
							bitcask:check_get(Key1, Entry, DefRef, DefState, 2)
					end;
				Entry ->
					bitcask:check_get(Key1, Entry, SplitRef, SplitState, 2)
			end
	end.

-spec put(reference(), binary(), binary()) -> ok | {error, term()}.
put(Ref, Key, Value) ->
	put(Ref, Key, Value, []).
-spec put(reference(), binary(), binary(), list()) -> ok | {error, term()}.
put(Ref, Key1, Value, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	Split = proplists:get_value(split, Opts, default),

	{Split, BitcaskRef, HasMerged, IsActive} = lists:keyfind(Split, 1, OpenInstances),

	%% Firstly we want to check if the file is ready to be put to, if it is not then put to default.
	%% If it is ready then perform a get of the object from the default location and delete it from there
	%% if it exists.
	case IsActive of
		true when Split =:= default ->
			bitcask:put(BitcaskRef, Key1, Value, Opts);
		true ->
			case HasMerged of
				true ->
					bitcask:put(BitcaskRef, Key1, Value, Opts);
				false ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					DefState = erlang:get(DefRef),
					case bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1) of
						not_found ->
							bitcask:put(BitcaskRef, Key1, Value, Opts);
						_ ->
							bitcask:delete(DefRef, Key1, Opts),
							case bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1) of
								not_found ->
									bitcask:put(BitcaskRef, Key1, Value, Opts);
								_ ->
									error_could_not_put
							end
					end
			end;
		_ ->
			{default, BitcaskRef1, _, _} = lists:keyfind(default, 1, OpenInstances),
			bitcask:put(BitcaskRef1, Key1, Value, Opts)
	end.

-spec delete(reference(), binary()) -> ok.
delete(Ref, Key) ->
	delete(Ref, Key, []).
delete(Ref, Key, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	Split = proplists:get_value(split, Opts, default),

	{Split, SplitRef, _, Active} = lists:keyfind(Split, 1, OpenInstances),

	case Active of
		true when Split =:= default ->
			bitcask:delete(SplitRef, Key, Opts);
		true ->
			{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
			DefState = erlang:get(DefRef),
			case bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) of
				not_found ->
					bitcask:delete(SplitRef, Key, Opts);
				_ ->
					error_could_not_delete_data_still_exists_in_default_split %% Here we could just delete from both?
			end;
		false ->
			{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
			bitcask:delete(DefRef, Key, Opts)
	end.

-spec sync(reference()) -> ok.
sync(Ref) ->
	State = erlang:get(Ref),
	[bitcask:sync(BRef) || {_, BRef, _, _} <- State#state.open_instances],
	ok.

-spec list_keys(reference(), list()) -> [binary()] | {error, any()}.
list_keys(Ref, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	AllSplits = proplists:get_value(all_splits, Opts, false),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	case AllSplits of
		true ->
			lists:flatten([bitcask:list_keys(Ref0) || {_Split0, Ref0, _, _} <- OpenInstances]);
		false ->
			{Split, BitcaskRef, _, _} = lists:keyfind(Split, 1, OpenInstances),
			bitcask:list_keys(BitcaskRef)
	end.

-spec fold_keys(reference(), fun(), term()) -> term() | {error, any()}.
fold_keys(Ref, Fun, Acc) ->
	fold_keys(Ref, Fun, Acc, []).

-spec fold_keys(reference(), fun(), term(), list()) -> term() | {error, any()}.
fold_keys(Ref, Fun, Acc, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	{_Split, SplitRef, _HasMerged, _Active} =
		case lists:keyfind(Split, 1, State#state.open_instances) of
			false ->
				lists:keyfind(default, 1, State#state.open_instances);
			SplitData ->
				SplitData
		end,
	SplitState = erlang:get(SplitRef),

	MaxAge = bitcask:get_opt(max_fold_age, SplitState#bc_state.opts) * 1000, % convert from ms to us
	MaxPuts = bitcask:get_opt(max_fold_puts, SplitState#bc_state.opts),

	fold_keys(Ref, Fun, Acc, Opts, MaxAge, MaxPuts, false).

-spec fold_keys(reference(), fun(), term(), list(), non_neg_integer() | undefined,
	non_neg_integer() | undefined, boolean()) -> term | {error, any()}.
fold_keys(Ref, Fun, Acc, Opts, MaxAge, MaxPuts, SeeTombStoneP) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	{Split1, SplitRef, HasMerged, Active} =
		case lists:keyfind(Split, 1, State#state.open_instances) of
			false ->
				lists:keyfind(default, 1, State#state.open_instances);
			SplitData ->
				SplitData
		end,
	AllSplits = proplists:get_value(all_splits, Opts, false),
	case AllSplits of
		false ->
			case Active of
				true when Split1 =:= default ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP);
				true ->
					case HasMerged of
						false ->
							{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
							DefKeys = bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP),
							SplitKeys = bitcask:fold_keys(SplitRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP),
							lists:flatten([DefKeys, SplitKeys]);
						true ->
							bitcask:fold_keys(SplitRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP)
					end;
				eliminate ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP);
				_ ->
					case HasMerged of
						true ->
							{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
							DefKeys = bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP),
							SplitKeys = bitcask:fold_keys(SplitRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP),
							lists:flatten([DefKeys, SplitKeys]);
						false ->
							{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
							bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP)
					end
			end;
		true ->
			case [bitcask:fold_keys(SplitRef0, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP) || {_Split0, SplitRef0, _, SplitActive} <- OpenInstances, SplitActive =:= true] of
				Acc1 when is_tuple(hd(Acc1)) ->
					OutPut = lists:flatten([X || {X, _} <- Acc1]),
					{OutPut, element(2, hd(Acc1))};
				Acc1 when is_list(hd(Acc1)) ->
					lists:flatten(Acc1)
			end
	end.

-spec fold(reference() | tuple(),
	fun((binary(), binary(), any()) -> any()),
	any()) -> any() | {error, any()}.
fold(Ref, Fun, Acc) ->
	fold(Ref, Fun, Acc, []).

fold(Ref, Fun, Acc, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	{Split, SplitRef, _, _} = lists:keyfind(Split, 1, State#state.open_instances),
	SplitState = erlang:get(SplitRef),

	MaxAge = bitcask:get_opt(max_fold_age, SplitState#bc_state.opts) * 1000, % convert from ms to us
	MaxPuts = bitcask:get_opt(max_fold_puts, SplitState#bc_state.opts),
	SeeTombstonesP = bitcask:get_opt(fold_tombstones, SplitState#bc_state.opts) /= undefined,

	fold(Ref, Fun, Acc, Opts, MaxAge, MaxPuts, SeeTombstonesP).

-spec fold(reference() | tuple(), fun((binary(), binary(), any()) -> any()), any(),
	list(), non_neg_integer() | undefined, non_neg_integer() | undefined, boolean()) ->
	any() | {error, any()}.
fold(Ref, Fun, Acc, Opts, MaxAge, MaxPut, SeeTombstonesP) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	{Split, SplitRef, HasMerged, Active} = lists:keyfind(Split, 1, OpenInstances),
	AllSplits = proplists:get_value(all_splits, Opts, false),
	case AllSplits of
		false ->
			case Active of
				true when Split =:= default ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold(DefRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP);
				true ->
					case HasMerged of
						false ->
							{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
							DefKeys = bitcask:fold(DefRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP),
							SplitKeys = bitcask:fold(SplitRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP),
							lists:flatten([DefKeys, SplitKeys]);
						true ->
							bitcask:fold(SplitRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP)
					end;
				_ ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold(DefRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP)
			end;
		true ->
			lists:flatten([bitcask:fold(SplitRef0, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP) || {_, SplitRef0, _, _} <- OpenInstances])
	end.

-spec merge(reference(), list()) -> ok | {error, any()}.
merge(Ref, Opts) ->
	State = erlang:get(Ref),
	_OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,

	[begin
		 FilesToMerge = bitcask:readable_files(Dir),
		 bitcask:merge(Dir, Opts, FilesToMerge)
	 end || {_Split, Dir} <- OpenDirs].

-spec merge(reference(), list(), [string()]) -> ok | {error, any()}.
merge(_RootDir, Opts, FilesToMerge) ->
	[bitcask:merge(Dir, Opts, Files) || {Dir, Files} <- FilesToMerge].

-spec special_merge(reference(), atom(), atom(), list()) -> ok | {error, any()}.
special_merge(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
	{Split1, _SplitRef1, _HasMerged1, IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, SplitRef2, _HasMerged2, IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

	case eliminate of
		X when X =:= IsActive1 andalso X =:= IsActive2 ->
			error;
		_ ->
			ok = special_merge2(Ref, Split1, Split2, Opts),

			OpenInstances = lists:keyreplace(Split2, 1, State#state.open_instances, {Split2, SplitRef2, true, IsActive2}),
			NewState1 = State#state{open_instances = OpenInstances},
			erlang:put(Ref, NewState1),
			ok
	end.

-spec reverse_merge(reference(), atom(), atom(), list()) -> ok | {error, any()}.
reverse_merge(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
	{Split1, SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, _SplitRef2, _HasMerged2, _IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

	ok = special_merge2(Ref, Split1, Split2, [{reverse_merge, true} | Opts]),
	State1 = State#state{
		open_instances = lists:keyreplace(Split1, 1, State#state.open_instances, {Split1, SplitRef1, true, eliminate})},
	erlang:put(Ref, State1),
	ok.

-spec needs_merge(reference()) -> {true, [string()]} | false.
needs_merge(Ref) ->
	needs_merge(Ref, []).

-spec needs_merge(reference(), list()) -> {true, [string()]} | false.
needs_merge(Ref, Opts) ->
	State = erlang:get(Ref),
	Files = [{proplists:get_value(Split, State#state.open_dirs), bitcask:needs_merge(BRef, Opts)} || {Split, BRef, _, _} <- State#state.open_instances],
	case [{X, Z} || {X, {_Y, Z} = I} <- Files, I =/= false, X =/= undefined] of
		[] ->
			false;
		NewFiles ->
			{true, NewFiles}
	end.

-spec check_backend_exists(reference(), binary()) -> boolean().
check_backend_exists(Ref, Key) ->
	State = erlang:get(Ref),
	case lists:keyfind(Key, 1, State#state.open_instances) of
		false ->
			false;
		_ ->
			true
	end.

-spec is_active(reference(), atom()) -> atom().
is_active(Ref, Split) ->
	State = erlang:get(Ref),
	case lists:keyfind(Split, 1, State#state.open_instances) of
		{Split, _BRef, _, Active} ->
			Active;
		false ->
			false
	end.

-spec has_merged(reference(), atom()) -> atom().
has_merged(Ref, Split) ->
	State = erlang:get(Ref),
	case lists:keyfind(Split, 1, State#state.open_instances) of
		{Split, _SRef, HasMerged, _} ->
			HasMerged;
		false ->
			false
	end.

%% TODO Both these calls need to be reviewed as to what they actually do and best way for riak to know which split to call on.
-spec is_empty_estimate(reference()) -> boolean().
is_empty_estimate(Ref) ->
	State = erlang:get(Ref),
	lists:all(fun({_, BRef, _, _}) -> bitcask:is_empty_estimate(BRef) end, State#state.open_instances).

-spec status(reference()) -> {integer(), [{string(), integer(), integer(), integer()}]}.
status(Ref) ->
	State = erlang:get(Ref),
	{KC, SL} = lists:foldl(fun(BRef, Acc) ->
		{KeyCount, StatusList} = bitcask:status(BRef),
		case Acc of
			{} ->
				{KeyCount, StatusList};
			{KeyCountAcc, StatusAcc} ->
				{KeyCount + KeyCountAcc, [StatusList | StatusAcc]}
		end
				end, {}, State#state.open_instances),
	{KC, lists:flatten(SL)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

special_merge2(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
	{Split1, _SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, _SplitRef2, HasMerged2, IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

	MState = case IsActive2 of
				 true ->
					 case HasMerged2 of
						 true when Split2 =/= default ->	%%TODO this should only appear if special_merge is called for a second time. Should we merge files or not, a second activate would have to be processed so no data is lost too?
							 io:format("This Split has already been merged: ~p~n", [Split2]),
							 Dirname1 = proplists:get_value(Split1, State#state.open_dirs),
							 Dirname2 = proplists:get_value(Split2, State#state.open_dirs),
							 NewState = prep_mstate(Split1, Split2, Dirname1, Dirname2, [], Opts, Ref),
							 merge_files(NewState);	%% Will not merge since input files are []
						 true when Split2 =:= default ->
							 Dirname1 = proplists:get_value(Split1, State#state.open_dirs),
							 Dirname2 = proplists:get_value(Split2, State#state.open_dirs),
							 FilesToMerge = bitcask:readable_files(Dirname1),
							 NewState = prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts, Ref),
							 merge_files(NewState);
						 false ->
							 Dirname1 = proplists:get_value(Split1, State#state.open_dirs),
							 Dirname2 = proplists:get_value(Split2, State#state.open_dirs),
							 FilesToMerge = bitcask:readable_files(Dirname1),
							 NewState = prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts, Ref),
							 merge_files(NewState)
					 end;
				 ActiveState ->
					 io:format("Cannot merge split: ~p due to not being active for merging. Current state: ~p~n", [Split1, ActiveState]),
					 Dirname1 = proplists:get_value(Split1, State#state.open_dirs),
					 Dirname2 = proplists:get_value(Split2, State#state.open_dirs),
					 NewState = prep_mstate(Split1, Split2, Dirname1, Dirname2, [], Opts, Ref),
					 merge_files(NewState)
			 end,
	case MState#mstate.out_file of
		fresh ->
			ok;
		Outfile ->
			ok = bitcask_fileops:sync(Outfile),
			ok = bitcask_fileops:close(Outfile)
	end,

	_ = [begin
			 ok = bitcask_fileops:sync(TFile),
			 ok = bitcask_fileops:close(TFile)
		 end || TFile <- MState#mstate.tombstone_write_files],
	bitcask_fileops:close_all(MState#mstate.input_files),
	bitcask_nifs:keydir_release(MState#mstate.origin_live_keydir),
	bitcask_nifs:keydir_release(MState#mstate.destination_live_keydir),
	bitcask_lockops:release(MState#mstate.merge_lock).

prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts, Ref) ->

	DecodeDiskKeyFun = bitcask:get_decode_disk_key_fun(bitcask:get_opt(decode_disk_key_fun, Opts)),
	EncodeDiskKeyFun = bitcask:get_encode_disk_key_fun(bitcask:get_opt(encode_disk_key_fun, Opts)),
	FindSplitFun = bitcask:get_find_split_fun(bitcask:get_opt(find_split_fun, Opts)),
	UpgradeKeyFun = bitcask:get_upgrade_key_fun(bitcask:get_opt(check_and_upgrade_key_fun, Opts)),
	UpgradeKey = proplists:get_value(upgrade_key, Opts, false),
	Partition = proplists:get_value(partition, Opts, undefined),

	case bitcask_lockops:acquire(merge, Dirname2) of
		{ok, Lock} ->
			ok;
		{error, Reason} ->
			Lock = undefined,
			throw({error, {merge_locked, Reason, Dirname2}})
	end,

	case bitcask_nifs:maybe_keydir_new(Dirname1) of
		{ready, LiveKeyDir} ->
			%% Simplest case; a key dir is already available and
			%% loaded. Go ahead and open just the files we wish to
			%% merge
			InFiles0 = [begin
			%% Handle open errors gracefully.  QuickCheck
			%% plus PULSE showed that there are races where
			%% the open below can fail.
							case bitcask_fileops:open_file(F) of
								{ok, Fstate} -> Fstate;
								{error, _} -> skip
							end
						end
				|| F <- FilesToMerge],
			InFiles1 = [F || F <- InFiles0, F /= skip];
		{error, not_ready} ->
			%% Someone else is loading the keydir, or this cask isn't open.
			%% We'll bail here and try again later.

			ok = bitcask_lockops:release(Lock),
			% Make erlc happy w/ non-local exit
			LiveKeyDir = undefined, InFiles1 = [],
			throw({error, not_ready})
	end,

	{ready, DestinationKeyDir} = bitcask_nifs:maybe_keydir_new(Dirname2),

	ReadableFiles = lists:usort(bitcask:readable_files(Dirname1)),

	InFiles = lists:sort(fun(#filestate{tstamp = FTL}, #filestate{tstamp = FTR}) ->
		FTL =< FTR
						 end, InFiles1),
	InFileIds = sets:from_list([bitcask_fileops:file_tstamp(InFile)
		|| InFile <- InFiles]),

	MinFileId = if ReadableFiles == [] ->
		1;
					true ->
						lists:min([bitcask_fileops:file_tstamp(F) ||
							F <- ReadableFiles])
				end,
	{ok, DelKeyDir} = bitcask_nifs:keydir_new(),

	#mstate{origin_dirname = Dirname1,
		destination_dirname = Dirname2,
		origin_splitname = Split1,
		destination_splitname = Split2,
		manager_ref = Ref,
		merge_lock = Lock,
		max_file_size = proplists:get_value(max_file_size, Opts),
		input_files = InFiles,
		input_file_ids = InFileIds,
		min_file_id = MinFileId,
		tombstone_write_files = [],
		out_file = fresh,  % will be created when needed
		merge_coverage = full,
		origin_live_keydir = LiveKeyDir,
		destination_live_keydir = DestinationKeyDir,
		del_keydir = DelKeyDir,
		expiry_time = bitcask:expiry_time(Opts),
		expiry_grace_time = bitcask:expiry_grace_time(Opts),
		decode_disk_key_fun = DecodeDiskKeyFun,
		encode_disk_key_fun = EncodeDiskKeyFun,
		find_split_fun = FindSplitFun,
		upgrade_key_fun = UpgradeKeyFun,
		upgrade_key = UpgradeKey,
		partition = Partition,
		read_write_p = 0,
		opts = Opts,
		delete_files = []}.

merge_files(#mstate{input_files = []} = State) ->
	State;
merge_files(#mstate{origin_dirname = Dirname,
	input_files = [File | Rest],
	decode_disk_key_fun = DecodeDiskKeyFun,
	find_split_fun = FindSplitFun,
	partition = Partition} = State) ->
	FileId = bitcask_fileops:file_tstamp(File),
	F = fun(K0, V, Tstamp, Pos, State0) ->
		K = try
				DecodeDiskKeyFun(K0)
			catch
				Err ->
					{key_tx_error, Err}
			end,
		case K of
			{key_tx_error, TxErr} ->
				error_logger:error_msg("Invalid key on merge ~p: ~p",
					[K0, TxErr]),
				State0;
			#keyinfo{key = K1, tstamp_expire = TstampExpire} ->
				Split = FindSplitFun(K1, Partition),
				DestinationSplit = State0#mstate.destination_splitname,
				case DestinationSplit of
					default ->
						merge_single_entry(K1, K0, V, Tstamp, TstampExpire, FileId, Pos, State0);
					Split ->
						merge_single_entry(K1, K0, V, Tstamp, TstampExpire, FileId, Pos, State0);
					_ ->
						State0
				end
		end
		end,
	State2 = try bitcask_fileops:fold(File, F, State) of
				 #mstate{delete_files = _DelFiles} = State1 ->
					 State1
			 catch
				 throw:{fold_error, Error, _PartialAcc} ->
					 error_logger:error_msg(
						 "merge_files: skipping file ~s in ~s: ~p\n",
						 [File#filestate.filename, Dirname, Error]),
					 State
			 end,
	merge_files(State2#mstate{input_files = Rest}).

merge_single_entry(KeyDirKey, DiskKey, V, Tstamp, TstampExpire, FileId, {_, _, Offset, _} = Pos, State) ->
	case bitcask:out_of_date(State, KeyDirKey, Tstamp, bitcask:is_key_expired(TstampExpire), FileId, Pos, State#mstate.expiry_time,
		false, [State#mstate.origin_live_keydir, State#mstate.del_keydir]) of
		expired ->
			%% Drop tombstone if it's expired and do not transfer it to the new split location.
			bitcask_nifs:keydir_remove(State#mstate.origin_live_keydir, KeyDirKey,
				Tstamp, FileId, Offset),
			State;
		false ->
			case bitcask:is_tombstone(V) of
				true ->	%% Not out of date, current value is tombstone so we want to drop and not transfer
					ok = bitcask_nifs:keydir_put(State#mstate.del_keydir, KeyDirKey,
						FileId, 0, Offset, Tstamp, TstampExpire,
						bitcask_time:tstamp()),
					State;
				false -> %% Current value is not tombstone so needs to be transferred
					ok = bitcask_nifs:keydir_remove(State#mstate.del_keydir, KeyDirKey),
					transfer_split(KeyDirKey, DiskKey, V, Tstamp, TstampExpire, FileId, Pos, State)
			end;
		not_found ->
			%% Data not in the keydir can be dropped as is not active
			State;
		true ->
			%% Value in keydir is newer, but this could be a tombstone.
			State
	end.

transfer_split(KeyDirKey, DiskKey, V, Tstamp, TstampExpire, _FileId, {_, _, _Offset, _} = _Pos,
	#mstate{destination_splitname = DestinationSplit,
		upgrade_key_fun = UpgradeKeyFun,
		upgrade_key = UpgradeKey,
		decode_disk_key_fun = DecodeFun,
		encode_disk_key_fun = EncodeFun} = State) ->
	{KeyDirKey1, DiskKey1} = case UpgradeKey of
								 false ->
									 {KeyDirKey, DiskKey};
								 true ->
									 NewKeyDirKey = UpgradeKeyFun(DestinationSplit, KeyDirKey),
									 KeyRec = DecodeFun(DiskKey),
									 NewDiskKey 	 = UpgradeKeyFun(DestinationSplit, KeyRec#keyinfo.key),
									 EncodedDiskKey = EncodeFun(NewDiskKey, [{tstamp_expire, KeyRec#keyinfo.tstamp_expire}]),
									 {NewKeyDirKey, EncodedDiskKey}
							 end,
	State1 =
		case bitcask_fileops:check_write(State#mstate.out_file,
			DiskKey, size(V),
			State#mstate.max_file_size) of
			wrap ->
				%% Close the current output file
				ok = bitcask_fileops:sync(State#mstate.out_file),
				ok = bitcask_fileops:close(State#mstate.out_file),

				%% Start our next file and update state
				{ok, NewFile} = bitcask_fileops:create_file(
					State#mstate.destination_dirname,
					State#mstate.opts,
					State#mstate.destination_live_keydir),
				NewFileName = bitcask_fileops:filename(NewFile),
				ok = bitcask_lockops:write_activefile(
					State#mstate.merge_lock,
					NewFileName),
				State#mstate{out_file = NewFile};
			ok ->
				State;
			fresh ->
				%% create the output file and take the lock.
				Return = bitcask_fileops:create_file(
					State#mstate.destination_dirname,
					State#mstate.opts,
					State#mstate.destination_live_keydir),
				{ok, NewFile} = Return,
				NewFileName = bitcask_fileops:filename(NewFile),
				ok = bitcask_lockops:write_activefile(
					State#mstate.merge_lock,
					NewFileName),
				State#mstate{out_file = NewFile}
		end,

	{ok, Outfile, Offset, Size} =
		bitcask_fileops:write(State1#mstate.out_file, DiskKey1, V, Tstamp),

	OutFileId = bitcask_fileops:file_tstamp(Outfile),

	Outfile2 =
		case bitcask_nifs:keydir_put(State1#mstate.destination_live_keydir, KeyDirKey1,
			OutFileId,
			Size, Offset, Tstamp, TstampExpire,
			bitcask_time:tstamp(),
			0, 0) of
			ok ->
				Outfile;
			already_exists ->
				{ok, O} = bitcask_fileops:un_write(Outfile),
				O
		end,
	ManagerRef = State1#mstate.manager_ref,
	ManagerState = erlang:get(ManagerRef),
	{_, SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(State1#mstate.origin_splitname, 1, ManagerState#state.open_instances),
	Split1State = erlang:get(SplitRef1),

	ValSize = bitcask:tombstone_size_for_version(Split1State#bc_state.tombstone_version),
	NewSplitState = case bitcask_fileops:check_write(Split1State#bc_state.write_file, DiskKey, ValSize,	Split1State#bc_state.max_file_size) of
						wrap ->
							%% Close the current output file
							bitcask:wrap_write_file(Split1State);
						fresh ->
							%% create the output file and take the lock.
							case bitcask_lockops:acquire(write, Split1State#bc_state.dirname) of
								{ok, WriteLock} ->
									{ok, NewFile1} = bitcask_fileops:create_file(
										Split1State#bc_state.dirname,
										Split1State#bc_state.opts,
										Split1State#bc_state.keydir),
									ok = bitcask_lockops:write_activefile(
										WriteLock,
										bitcask_fileops:filename(NewFile1)),
									Split1State#bc_state{write_file = NewFile1, write_lock = WriteLock};
								Error ->
									throw({unrecoverable, Error, State})
							end;
						_ ->
							Split1State
					end,

	OriginWriteFile = NewSplitState#bc_state.write_file,

	%% TODO review case clauses are all good and behave as should
	case bitcask_nifs:keydir_get(State1#mstate.origin_live_keydir, KeyDirKey) of
		#bitcask_entry{tstamp = OldTstamp, file_id = OldFileId,
			offset = OldOffset} ->
			Tombstone = <<?TOMBSTONE2_STR, OldFileId:32>>,
			case bitcask_fileops:write(OriginWriteFile, DiskKey,
				Tombstone, Tstamp) of
				{ok, WriteFile2, _, TSize} ->
					ok = bitcask_nifs:update_fstats(
						Split1State#bc_state.keydir,
						bitcask_fileops:file_tstamp(WriteFile2), Tstamp,
						0, 0, 0, 0, TSize, _ShouldCreate = 1),
					case bitcask_nifs:keydir_remove(State1#mstate.origin_live_keydir,
						KeyDirKey, OldTstamp,
						OldFileId, OldOffset) of
						already_exists ->	%% TODO Will this ever happen on these types of merges? If so what do here?
							%% Merge updated the keydir after tombstone
							%% write.  beat us, so undo and retry in a
							%% new file.
							erlang:put(SplitRef1, NewSplitState#bc_state{write_file = WriteFile2}),
							State1#mstate{out_file = Outfile2};
						ok ->
							erlang:put(SplitRef1, NewSplitState#bc_state{write_file = WriteFile2}),
							State1#mstate{out_file = Outfile2}
					end;
				{error, _} = ErrorTomb ->
					throw({unrecoverable, ErrorTomb, Split1State})
			end;
		not_found ->
			erlang:put(SplitRef1, NewSplitState#bc_state{write_file = OriginWriteFile}),
			State1#mstate{out_file = Outfile2}
	end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

manager_special_merge_test() ->
	os:cmd("rm -rf /tmp/bc.man.merge"),
	Dir = "/tmp/bc.man.merge",
	Key1 = make_bitcask_key(3, {<<"b1">>, <<"default">>}, <<"k1">>),
	Key2 = make_bitcask_key(3, {<<"b2">>, <<"default">>}, <<"k2">>),
	Key3 = make_bitcask_key(3, {<<"b3">>, <<"default">>}, <<"k3">>),
	Key4 = make_bitcask_key(3, {<<"b4">>, <<"second">>},  <<"k4">>),
	Key5 = make_bitcask_key(3, {<<"b5">>, <<"second">>},  <<"k5">>),
	Key6 = make_bitcask_key(3, {<<"b6">>, <<"second">>},  <<"k6">>),
	Keys = [{default, Key1}, {default, Key2}, {default, Key3}, {second, Key4}, {second, Key5}, {second, Key6}],
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 60}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 60}]),

	[bitcask_manager:put(B, Key, Key, [{split, Split}]) || {Split, Key} <- Keys],

	%% Shows split data is in the default location and retrieable with both split option passed in
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || {_, Key} <- Keys],
	%% Here we test that data can be fetched from both splits. Since we
	%% have not merged to the new split the data is actually still in
	%% the default location.
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- [Key4, Key5, Key6]],

	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState = erlang:get(DefRef),
	SplitState = erlang:get(SplitRef),

	%% Shows its not actually in the split keydir and only in the default as is expected
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) || Key <- [Key4, Key5, Key6]],
	[not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key) || Key <- [Key4, Key5, Key6]],

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	Files = bitcask_fileops:data_file_tstamps(DefDir),
	Files2 = bitcask_fileops:data_file_tstamps(SplitDir),

	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,
	ExpectedMergeFiles = [{X, FileFun(X, default)} || X <- lists:seq(1, 6)],

	?assertEqual(ExpectedMergeFiles, lists:sort(Files)),
	?assertEqual([], Files2),

	bitcask_manager:activate_split(B, second),

	bitcask_manager:special_merge(B, default, second, [{max_file_size, 60}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	Files4 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles0 = [{X, FileFun(X, default)} || X <- lists:seq(1, 9)],
	ExpectedMergeFiles1 = [{X, FileFun(X, second)} || X <- lists:seq(1, 3)],
	?assertEqual(ExpectedMergeFiles0, lists:sort(Files3)),
	?assertEqual(ExpectedMergeFiles1, lists:sort(Files4)),

	%% Once merged it is only fetchable with correct split option
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- [Key1, Key2, Key3]],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}])  || Key <- [Key4, Key5, Key6]],
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- [Key4, Key5, Key6]],

	%% It is transferred to the split keydir rather than default one
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key) || Key <- [Key4, Key5, Key6]],
	[not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) || Key <- [Key4, Key5, Key6]],

	Key7 = make_bitcask_key(3, {<<"b7">>, <<"default">>}, <<"k7">>),
	Key77 = make_bitcask_key(3, {<<"b77">>, <<"default">>}, <<"k77">>),
	Key8 = make_bitcask_key(3, {<<"b8">>, <<"second">>}, <<"k8">>),

	bitcask_manager:put(B, Key7, <<"Value7">>, [{split, default}]),
	bitcask_manager:put(B, Key77, <<"Value77">>, [{split, default}]),
	bitcask_manager:put(B, Key8, <<"Value8">>, [{split, second}]),

	%% Split data is not retrievable after special merge with wrong split option
	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),
	{ok, <<"Value77">>} = bitcask_manager:get(B, Key77, [{split, default}]),
	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, second}]),
	not_found = bitcask_manager:get(B, Key8, [{split, default}]),
	{ok, <<"Value8">>} = bitcask_manager:get(B, Key8, [{split, second}]),

	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key7),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key7),
	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key8),
	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key8),

	Files5 = bitcask_fileops:data_file_tstamps(DefDir),
	Files6 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles5 = [{X, FileFun(X, default)} || X <- lists:seq(1, 11)],	%% We have 11 due to activating putting 3 split tombstones then putting 2 new keys.
	ExpectedMergeFiles6 = [{X, FileFun(X, second)}  || X <- lists:seq(1, 4)],
	?assertEqual(ExpectedMergeFiles5, lists:sort(Files5)),
	?assertEqual(ExpectedMergeFiles6, lists:sort(Files6)),

	{true, Z} = needs_merge(B),

	MergeResponses = bitcask_manager:merge(Dir, [], Z),
	timer:sleep(2000),
	?assertEqual(1, length(MergeResponses)),
	?assertEqual(DefDir, element(1, hd(Z))),	%% Only default should need merging

	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- [Key4, Key5, Key6]],
	{ok, <<"Value8">>} = bitcask_manager:get(B, Key8, [{split, second}]),

	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- [Key1, Key2, Key3]],
	{ok, <<"Value77">>} = bitcask_manager:get(B, Key77, [{split, default}]),
	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),

	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) || Key <- [Key1, Key2, Key3, Key7, Key77]],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key) || Key <- [Key4, Key5, Key6, Key8]],

	bitcask_manager:close(B).

fold_expired_keys_and_tombstones_test() ->
	os:cmd("rm -rf /tmp/bc.man.fold"),
	Dir = "/tmp/bc.man.fold",
	Keys  = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(0,5)],
	Keys2 = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(6,7)],
	Keys3 = [make_bitcask_key(3, {<<"new_bucket">>, <<"default">>}, integer_to_binary(N)) || N <- lists:seq(8,9)],
	Expiry = bitcask_time:tstamp() - 1000,
	PutOpts = [{tstamp_expire, Expiry}],
	DecodeKeyFun = decode_key_fun(Keys, Expiry),
	MergeOpts = [{decode_disk_key_fun, DecodeKeyFun}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}],

	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default} | MergeOpts]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second} | MergeOpts]),


	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys ++ Keys3],

	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys ++ Keys3],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys ++ Keys3],

	FoldKFun = fun(_, Key, Acc) -> [Key | Acc] end,
	FoldBFun = fun(Bucket, Acc) -> [Bucket | Acc] end,
	FoldKeysFun = fold_keys_fun(FoldKFun, <<"second">>),
	FoldBucketsFun = fold_buckets_fun(FoldBFun),
	FoldObjFun = fold_obj_fun(FoldKFun, undefined),

	ListKeys = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	{ListBuckets, _} = bitcask_manager:fold_keys(B, FoldBucketsFun, {[], sets:new()}),
	ListObjects = bitcask_manager:fold(B, FoldObjFun, [], []),
	ExpectedKeys = [integer_to_binary(N) || N <- lists:seq(0,5)],
	ExpectedKeys1 = [integer_to_binary(N) || N <- lists:seq(0,5) ++ lists:seq(8,9), N =/= 1],	%% Remove 1 as the decode fun above matches on it for further in the test
	ExpectedBuckets = [<<"new_bucket">>, <<"second">>],
	?assertEqual(ExpectedKeys, lists:sort(ListKeys)),
	?assertEqual(ExpectedBuckets, lists:sort(ListBuckets)),
	?assertEqual(ExpectedKeys1, lists:sort(ListObjects)),

	%% Replace value of existing key with one with expiry
	bitcask_manager:put(B, lists:nth(2, Keys), <<"deadval">>, [{split, second} | PutOpts]),	%% Will expire so not transfer
	bitcask_manager:delete(B, lists:nth(3, Keys), [{split, second}]), 						%% Puts tombstone so will not transfer
	bitcask_manager:put(B, lists:nth(4, Keys), <<"newval">>, [{split, second}]),			%% Updates val no tombstone but only this should transfer

	ListKeys1 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	ListObjects1 = bitcask_manager:fold(B, FoldObjFun, [], [{split, second}]),
	ExpectedKeys2 = [integer_to_binary(N) || N <- lists:seq(0,5), N =/= 1 andalso N =/= 2],
	ExpectedObjects1 = [integer_to_binary(N) || N <- lists:seq(0,5) ++ lists:seq(8,9), N =/= 1 andalso N =/= 2],
	?assertEqual(ExpectedKeys2, lists:sort(ListKeys1)),
	?assertEqual(ExpectedObjects1, lists:sort(ListObjects1)),

	bitcask_manager:activate_split(B, second),

	%% Add data to split so we have split data in both dir locations
	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys2],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys2],

	ListKeys2 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	{ListBuckets2, _} = bitcask_manager:fold_keys(B, FoldBucketsFun, {[], sets:new()}),
	ListObjects2 = bitcask_manager:fold(B, FoldObjFun, [], [{split, second}]),
	ExpectedKeys3 = [integer_to_binary(N) || N <- lists:seq(0,7), N =/= 1 andalso N =/= 2],
	ExpectedKeys4 = [integer_to_binary(N) || N <- lists:seq(0,9), N =/= 1 andalso N =/= 2],
	?assertEqual(ExpectedKeys3, lists:sort(ListKeys2)),
	?assertEqual(ExpectedBuckets, lists:sort(ListBuckets2)),	%% Splits not yet merged so both buckets should be in default still
	?assertEqual(ExpectedKeys4, lists:sort(ListObjects2)),

	bitcask_manager:special_merge(B, default, second, MergeOpts),

	Key7 = make_bitcask_key(3, {<<"b7">>, <<"default">>}, <<"k7">>),
	Key8 = make_bitcask_key(3, {<<"b8">>, <<"second">>}, <<"k8">>),
	bitcask_manager:put(B, Key7, <<"value-7">>, [{split, default}]),
	bitcask_manager:put(B, Key8, <<"value-8">>, [{split, second}]),
	{ok, <<"value-7">>} = bitcask_manager:get(B, Key7, [{split, default}]),
	{ok, <<"value-7">>} = bitcask_manager:get(B, Key7, [{split, second}]),
	not_found = bitcask_manager:get(B, Key8, [{split, default}]),
	{ok, <<"value-8">>} = bitcask_manager:get(B, Key8, [{split, second}]),


	%% Check data is same after special merge
	ListKeys3 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	{ListBuckets3, _} = bitcask_manager:fold_keys(B, FoldBucketsFun, {[], sets:new()}),		%% Will list only default split buckets
	{ListBuckets4, _} = bitcask_manager:fold_keys(B, FoldBucketsFun, {[], sets:new()}, [{all_splits, true}]),
	ListObjects3 = bitcask_manager:fold(B, FoldObjFun, [], [{split, second}]),
	?assertEqual(ExpectedKeys3, lists:sort(ListKeys3)),
	?assertEqual(ExpectedKeys3 ++ [<<"k8">>], lists:sort(ListObjects3)),
	?assertEqual([<<"b7">>, <<"new_bucket">>], lists:sort(ListBuckets3)),
	?assertEqual([<<"b7">>, <<"b8">>] ++ ExpectedBuckets, lists:sort(ListBuckets4)),

	bitcask_manager:close(B),
	ok.

special_merge_and_merge_test() ->
	os:cmd("rm -rf /tmp/bc.man.merge2"),
	Dir = "/tmp/bc.man.merge2",
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50}]),
	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState = erlang:get(DefRef),
	SplitState = erlang:get(SplitRef),
	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,

	Keys = [make_bitcask_key(3, {<<"b1">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(1,10)],
	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key) || Key <- Keys],

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	Files1 = bitcask_fileops:data_file_tstamps(DefDir),
	Files2 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles = [{X, FileFun(X, default)}  || X <- lists:seq(1, 10)],
	?assertEqual(ExpectedMergeFiles, lists:sort(Files1)),
	?assertEqual([], Files2),

	bitcask_manager:activate_split(B, second),

	%% Split is active so keys should not be in the new keydir and put normally
	[bitcask_manager:put(B, Key, <<"new_val">>, [{split, second}]) || Key <- Keys],

	DefState0 = erlang:get(DefRef),
	SplitState0 = erlang:get(SplitRef),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState0#bc_state.keydir, Key) || Key <- Keys],

	bitcask_manager:special_merge(B, default, second, [{max_file_size, 50}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	DefState00 = erlang:get(DefRef),
	SplitState00 = erlang:get(SplitRef),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(DefState00#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState00#bc_state.keydir, Key) || Key <- Keys],

	%% Create tombstone values of keys 2 and 3 so these files get merged
	[bitcask_manager:put(B, Key, <<"newer_val">>, [{split, second}]) || Key <- [lists:nth(2, Keys)]],

	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	Files4 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedDefFiles = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedSplitFiles = [{X, FileFun(X, second)}  || X <- lists:seq(1, 11)],
	?assertEqual(ExpectedDefFiles, 	 lists:sort(Files3)),
	?assertEqual(ExpectedSplitFiles, lists:sort(Files4)),

	{true, Z} = needs_merge(B),
	MergeResponses = bitcask_manager:merge(B, [], Z),
	timer:sleep(2000),

	DefState1 = erlang:get(DefRef),
	SplitState1 = erlang:get(SplitRef),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys, Key =/= lists:nth(2, Keys)],
	[not_found = bitcask_nifs:keydir_get(DefState1#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys],
	{ok, <<"newer_val">>} = bitcask_manager:get(B, lists:nth(2, Keys), [{split, second}]),

	Files5 = bitcask_fileops:data_file_tstamps(DefDir),
	Files6 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedDefFiles1 = [{X, FileFun(X, default)}  || X <- [20]],
	ExpectedSplitFiles2 = [{X, FileFun(X, second)}  || X <- [11, 12]],
	?assertEqual(ExpectedDefFiles1, 	lists:sort(Files5)),
	?assertEqual(ExpectedSplitFiles2, lists:sort(Files6)),

	?assertEqual(length(BState#state.open_instances), length(MergeResponses)),

	bitcask_manager:close(B).

deactivate_test() ->
	os:cmd("rm -rf /tmp/bc.man.deactivate"),
	Dir = "/tmp/bc.man.deactivate",
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50}]),
	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState = erlang:get(DefRef),
	SplitState = erlang:get(SplitRef),
	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,

	Keys = [make_bitcask_key(3, {<<"b1">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(1,10)],
	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key) || Key <- Keys],

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	Files1 = bitcask_fileops:data_file_tstamps(DefDir),
	Files2 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles0 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 10)],
	?assertEqual(ExpectedMergeFiles0, lists:sort(Files1)),
	?assertEqual([], Files2),

	bitcask_manager:activate_split(B, second),

	%% Split is active so keys should not be in the new keydir and put normally
	[bitcask_manager:put(B, Key, <<"new_val">>, [{split, second}]) || Key <- Keys],

	BState1 = erlang:get(B),
	DefState0 = erlang:get(DefRef),
	SplitState0 = erlang:get(SplitRef),
	{second, _, _, true} = lists:keyfind(second, 1, BState1#state.open_instances),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState0#bc_state.keydir, Key) || Key <- Keys],

	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	Files4 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles1 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedMergeFiles2 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 10)],
	?assertEqual(ExpectedMergeFiles1, lists:sort(Files3)),
	?assertEqual(ExpectedMergeFiles2, lists:sort(Files4)),

	bitcask_manager:deactivate_split(B, second),

	BState2 = erlang:get(B),
	{second, _, _, false} = lists:keyfind(second, 1, BState2#state.open_instances),

	[bitcask_manager:put(B, Key, <<"newer_val">>, [{split, second}]) || Key <- Keys],

	Files5 = bitcask_fileops:data_file_tstamps(DefDir),
	Files6 = bitcask_fileops:data_file_tstamps(SplitDir),

	ExpectedMergeFiles3 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 30)],	%% 10 more keys put to default
	ExpectedMergeFiles4 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 11)], %% Wrapped file so 11 total files
	?assertEqual(ExpectedMergeFiles3, lists:sort(Files5)),
	?assertEqual(ExpectedMergeFiles4, lists:sort(Files6)),

	bitcask_manager:close(B).

reverse_merge_test() ->
	os:cmd("rm -rf /tmp/bc.man.reverse_merge"),
	Dir = "/tmp/bc.man.reverse_merge",
	Keys = [make_bitcask_key(3, {<<"b1">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(1,10)],
	Expiry = bitcask_time:tstamp() - 1000,
	PutOpts = [{tstamp_expire, Expiry}],
	DecodeKeyFun = decode_key_fun(Keys, Expiry),
	MergeOpts = [{decode_disk_key_fun, DecodeKeyFun}],
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50} | MergeOpts]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50} | MergeOpts]),
	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,

	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],

	bitcask_manager:activate_split(B, second),

	[bitcask_manager:put(B, Key, <<"new_val">>, [{split, second}]) || Key <- Keys],

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	BState0 = erlang:get(B),
	DefState0 = erlang:get(DefRef),
	SplitState0 = erlang:get(SplitRef),
	{second, _, _, true} = lists:keyfind(second, 1, BState0#state.open_instances),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState0#bc_state.keydir, Key) || Key <- Keys],

	Files1 = bitcask_fileops:data_file_tstamps(DefDir),
	Files2 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles1 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedMergeFiles2 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 10)],
	?assertEqual(ExpectedMergeFiles1, lists:sort(Files1)),
	?assertEqual(ExpectedMergeFiles2, lists:sort(Files2)),

	%% Add tombstones to test that they aren't moved around
	bitcask_manager:put(B, lists:nth(2, Keys), <<"deadval">>, [{split, second} | PutOpts]),	%% Will expire so not transfer
	bitcask_manager:delete(B, lists:nth(3, Keys), [{split, second}]), 						%% Puts tombstone so will not transfer
	bitcask_manager:put(B, lists:nth(4, Keys), <<"newer_val">>, [{split, second}]),

	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	Files4 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles3 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedMergeFiles4 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 13)],
	?assertEqual(ExpectedMergeFiles3, lists:sort(Files3)),
	?assertEqual(ExpectedMergeFiles4, lists:sort(Files4)),

	bitcask_manager:deactivate_split(B, second),

	BState1 = erlang:get(B),
	DefState1 = erlang:get(DefRef),
	SplitState1 = erlang:get(SplitRef),
	{second, _, _, false} = lists:keyfind(second, 1, BState1#state.open_instances),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys, Key =/= lists:nth(2, Keys) andalso Key =/= lists:nth(3, Keys) andalso Key =/= lists:nth(4, Keys)],
	not_found = bitcask_manager:get(B, lists:nth(2, Keys), [{split, second}]),
	not_found = bitcask_manager:get(B, lists:nth(3, Keys), [{split, second}]),
	{ok, <<"newer_val">>} = bitcask_manager:get(B, lists:nth(4, Keys), [{split, second}]),

	[not_found = bitcask_nifs:keydir_get(DefState1#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys, Key =/= lists:nth(2, Keys) andalso Key =/= lists:nth(3, Keys) andalso Key =/= lists:nth(4, Keys)],
	not_found = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, lists:nth(2, Keys)),
	not_found = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, lists:nth(3, Keys)),

	Files5 = bitcask_fileops:data_file_tstamps(DefDir),
	Files6 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles5 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedMergeFiles6 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 14)],
	?assertEqual(ExpectedMergeFiles5, lists:sort(Files5)),
	?assertEqual(ExpectedMergeFiles6, lists:sort(Files6)),

	bitcask_manager:reverse_merge(B, second, default, [{max_file_size, 50}, {upgrade_key, true}, {find_split_fun, fun find_split/2}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	BState2 = erlang:get(B),
	DefState2 = erlang:get(DefRef),
	{second, SplitRef1, true, eliminate} = lists:keyfind(second, 1, BState2#state.open_instances),
	SplitState2 = erlang:get(SplitRef1),
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys, Key =/= lists:nth(2, Keys) andalso Key =/= lists:nth(3, Keys) andalso Key =/= lists:nth(4, Keys)],
	[{ok, <<"new_val">>} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys, Key =/= lists:nth(2, Keys) andalso Key =/= lists:nth(3, Keys) andalso Key =/= lists:nth(4, Keys)],
	{ok, <<"newer_val">>} = bitcask_manager:get(B, lists:nth(4, Keys), [{split, default}]),
	{ok, <<"newer_val">>} = bitcask_manager:get(B, lists:nth(4, Keys), [{split, second}]),

	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState2#bc_state.keydir, Key) || Key <- Keys, Key =/= lists:nth(2, Keys) andalso Key =/= lists:nth(3, Keys) andalso Key =/= lists:nth(4, Keys)],
	[not_found = bitcask_nifs:keydir_get(SplitState2#bc_state.keydir, Key) || Key <- Keys],

	bitcask_manager:close(B, second),

	Files7 = bitcask_fileops:data_file_tstamps(DefDir),
	Files8 = file:list_dir(SplitDir),
	ExpectedMergeFiles7 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 28)],	%% 28 due to 2 of the keys becoming tombstones in the split and merged back into the default
	?assertEqual(ExpectedMergeFiles7, lists:sort(Files7)),
	?assertEqual({error, enoent}, Files8),

	bitcask_manager:close(B).

reverse_merge2_test() ->
	os:cmd("rm -rf /tmp/bc.man.reverse_merge2"),
	Dir = "/tmp/bc.man.reverse_merge2",
	Keys = [make_bitcask_key(3, {<<"b1">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(1,10)],
	Expiry = bitcask_time:tstamp() - 1000,
	DecodeKeyFun = decode_key_fun(Keys, Expiry),
	MergeOpts = [{decode_disk_key_fun, DecodeKeyFun}],
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50} | MergeOpts]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50} | MergeOpts]),
	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,

	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],

	bitcask_manager:activate_split(B, second),

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	BState0 = erlang:get(B),
	DefState0 = erlang:get(DefRef),
	SplitState0 = erlang:get(SplitRef),
	{second, _, _, true} = lists:keyfind(second, 1, BState0#state.open_instances),
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(SplitState0#bc_state.keydir, Key) || Key <- Keys],

	Files1 = bitcask_fileops:data_file_tstamps(DefDir),
	Files2 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles1 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 11)],
	?assertEqual(ExpectedMergeFiles1, lists:sort(Files1)),
	?assertEqual([], Files2),

	bitcask_manager:special_merge(B, default, second, [{max_file_size, 50}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	%% Confirm Data has been moved to new location and is retrievable
	DefState1 = erlang:get(DefRef),
	SplitState1 = erlang:get(SplitRef),
	[not_found = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(DefState1#bc_state.keydir, Key) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys],

	%% Confirm tombstones get writen to file in default location and data is also transferred
	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	Files4 = bitcask_fileops:data_file_tstamps(SplitDir),
	ExpectedMergeFiles2 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 20)],
	ExpectedMergeFiles3 = [{X, FileFun(X, second)}   || X <- lists:seq(1, 10)],
	?assertEqual(ExpectedMergeFiles2, lists:sort(Files3)),
	?assertEqual(ExpectedMergeFiles3, lists:sort(Files4)),

	bitcask_manager:reverse_merge(B, second, default, [{max_file_size, 50}, {upgrade_key, true}, {find_split_fun, fun find_split/2}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	DefState2 = erlang:get(DefRef),
	SplitState2 = erlang:get(SplitRef),
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState2#bc_state.keydir, Key) || Key <- Keys],
	[not_found = bitcask_nifs:keydir_get(SplitState2#bc_state.keydir, Key) || Key <- Keys],

	bitcask_manager:close(B, second),

	Files5 = bitcask_fileops:data_file_tstamps(DefDir),
	{error, enoent} = file:list_dir(SplitDir),
	ExpectedMergeFiles4 = [{X, FileFun(X, default)}  || X <- lists:seq(1, 30)],
	?assertEqual(ExpectedMergeFiles4, lists:sort(Files5)),

	{true, Z} = needs_merge(B),
	_MergeResponses = bitcask_manager:merge(B, [], Z),
	timer:sleep(2000),

	_Files6 = bitcask_fileops:data_file_tstamps(DefDir),
	bitcask_manager:close(B).

upgrade_keys_test() ->
	os:cmd("rm -rf /tmp/bc.man.upgrade_keys"),
	Dir = "/tmp/bc.man.upgrade_keys",
	Keys = [make_bitcask_key(2, <<"second">>, integer_to_binary(N)) || N <- lists:seq(0,5)],	%% Old Key Type no split
	Keys1 = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(0,5)],	%% New Version of above keys
	Keys2 = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(6,9)],	%% New Keys with split

	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50}]),
	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState0 = erlang:get(DefRef),
	SplitState0 = erlang:get(SplitRef),
	FoldKFun = fun(_, Key, Acc) -> [Key | Acc] end,
	FoldKeysFun = fold_keys_fun(FoldKFun, <<"second">>),

	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys ++ Keys2],

	%% Check versions of keys in keydir are correct
	BKeys0 = [#bitcask_entry{} = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys],
	BKeys1 = [#bitcask_entry{} = bitcask_nifs:keydir_get(DefState0#bc_state.keydir, Key) || Key <- Keys2],
	[not_found = bitcask_nifs:keydir_get(SplitState0#bc_state.keydir, Key) || Key <- Keys ++ Keys2],

	[?assertEqual(2, Version) || #bitcask_entry{key = <<Version:7, _Rest/binary>>} <- BKeys0],
	[?assertEqual(3, Version) || #bitcask_entry{key = <<Version:7, _Rest/binary>>} <- BKeys1],

	ListKeys = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),

	ExpectedKeys = [integer_to_binary(N) || N <- lists:seq(0,9)],
	?assertEqual(ExpectedKeys, lists:sort(ListKeys)),

	bitcask_manager:activate_split(B, second),

	bitcask_manager:special_merge(B, default, second, [{max_file_size, 50}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	ListKeys1 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),

	DefState1 = erlang:get(DefRef),
	SplitState1 = erlang:get(SplitRef),
	[not_found = bitcask_nifs:keydir_get(DefState1#bc_state.keydir, Key) || Key <- Keys ++ Keys1 ++ Keys2],
	[not_found = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys], %% key now been updated so old ones not found in keydir
	BKeys2 = [#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys1],
	BKeys3 = [#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, Key) || Key <- Keys2],
	[?assertEqual(3, Version) || #bitcask_entry{key = <<Version:7, _Rest/binary>>} <- BKeys2],
	[?assertEqual(3, Version) || #bitcask_entry{key = <<Version:7, _Rest/binary>>} <- BKeys3],
	?assertEqual(ExpectedKeys, lists:sort(ListKeys1)),

	bitcask_manager:deactivate_split(B, second),
ct:pal("About to reverse merge"),
	bitcask_manager:reverse_merge(B, second, default, [{max_file_size, 50}, {find_split_fun, fun find_split/2}, {upgrade_key, true}, {check_and_upgrade_key_fun, fun check_and_upgrade_key/2}]),

	ListKeys2 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	?assertEqual(ExpectedKeys, lists:sort(ListKeys2)),

	DefState2 = erlang:get(DefRef),
	SplitState2 = erlang:get(SplitRef),
	[not_found = bitcask_nifs:keydir_get(DefState2#bc_state.keydir, Key) || Key <- Keys], %% Keys have been upgraded so originals v2 would be not_found
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState2#bc_state.keydir, Key) || Key <- Keys1],
	[#bitcask_entry{} = bitcask_nifs:keydir_get(DefState2#bc_state.keydir, Key) || Key <- Keys2],
	[not_found = bitcask_nifs:keydir_get(SplitState2#bc_state.keydir, Key) || Key <- Keys ++ Keys1 ++ Keys2], %% key now been updated so old ones not found in keydir
%%	BKeys2 = [#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState2#bc_state.keydir, Key) || Key <- Keys1],
%%	BKeys3 = [#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState2#bc_state.keydir, Key) || Key <- Keys2],

	bitcask_manager:close(B).

decode_key_fun(Keys, Expiry) ->
	fun(Key) ->
		case lists:nth(2, Keys) of
			Key ->
				#keyinfo{key = Key, tstamp_expire = Expiry};
			_ ->
				#keyinfo{key = Key}
		end
	end.

fold_keys_fun(FoldKeysFun, Bucket) ->
	fun(#bitcask_entry{key=BK}, Acc) ->
		case make_riak_key(BK) of
			{_S, B, Key} ->
				case B =:= Bucket of
					true ->
						FoldKeysFun(B, Key, Acc);
					false ->
						Acc
				end;
			{B, Key} ->
				case B =:= Bucket of
					true ->
						FoldKeysFun(B, Key, Acc);
					false ->
						Acc
				end
		end
	end.

fold_buckets_fun(FoldBucketsFun) ->
	fun(#bitcask_entry{key=BK}, {Acc, BucketSet}) ->
		case make_riak_key(BK) of
			{Bucket, _} ->
				case sets:is_element(Bucket, BucketSet) of
					true ->
						{Acc, BucketSet};
					false ->
						{FoldBucketsFun(Bucket, Acc),
							sets:add_element(Bucket, BucketSet)}
				end;
			{_S, Bucket, _} ->
				case sets:is_element(Bucket, BucketSet) of
					true ->
						{Acc, BucketSet};
					false ->
						{FoldBucketsFun(Bucket, Acc),
							sets:add_element(Bucket, BucketSet)}
				end
		end
	end.

fold_obj_fun(FoldObjectsFun, undefined) ->
	fun(BK, _Value, Acc) ->
		case make_riak_key(BK) of
			{_S, Bucket, Key} ->
				FoldObjectsFun(Bucket, Key, Acc);
			{Bucket, Key} ->
				FoldObjectsFun(Bucket, Key, Acc)
		end
	end;
fold_obj_fun(FoldKeysFun, Bucket) ->
	fun(BK, _Value, Acc) ->
		case make_riak_key(BK) of
			{_S, B, Key} ->
				case B =:= Bucket of
					true ->
						FoldKeysFun(B, Key, Acc);
					false ->
						Acc
				end;
			{B, Key} ->
				case B =:= Bucket of
					true ->
						FoldKeysFun(B, Key, Acc);
					false ->
						Acc
				end
		end
	end.

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
make_riak_key(<<?VERSION_0:8, _Rest/binary>> = BK) ->
	binary_to_term(BK);
make_riak_key(BK) when is_binary(BK) ->
	BK.

find_split(Key, _) ->
	case make_riak_key(Key) of
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
	end.

check_and_upgrade_key(default, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
	KeyDirKey;
check_and_upgrade_key(Split, <<Version:7, _Rest/bitstring>> = KeyDirKey) when Version =/= ?VERSION_3 ->
	case make_riak_key(KeyDirKey) of
		{{Bucket, Type}, Key} ->
			make_bitcask_key(3, {Type, Bucket, atom_to_binary(Split, latin1)}, Key);
		{Bucket, Key} ->
			NewKey = make_bitcask_key(3, {Bucket, atom_to_binary(Split, latin1)}, Key),
			NewKey
	end;
check_and_upgrade_key(_Split, <<?VERSION_3:7, _Rest/bitstring>> = KeyDirKey) ->
	KeyDirKey;
check_and_upgrade_key(_Split, KeyDirKey) ->
	KeyDirKey.

	-endif.