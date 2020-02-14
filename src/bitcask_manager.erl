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
	activate_split/2,
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
	needs_merge/1,
	needs_merge/2,
	make_bitcask_key/3,
	check_and_upgrade_key/2,
	check_backend_exists/2,
	is_active/2,
	has_merged/2,
	is_empty_estimate/1,
	status/1,
	make_riak_key/1
]).

-include("bitcask.hrl").

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
	end).
-define(CHECK_AND_UPGRADE_KEY, fun check_and_upgrade_key/2).

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
	read_write_p :: integer(),    % integer() avoids atom -> NIF
	opts :: list(),
	delete_files :: [#filestate{}]}).

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


open(Dir) ->
	open(Dir, []).
open(Dir, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
	BitcaskRef = bitcask:open(NewDir, Opts),
	State = #state{open_instances = [{Split, BitcaskRef, true, true}], open_dirs = [{Split, NewDir}]},
	Ref = make_ref(),
	erlang:put(Ref, State),
	Ref.
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

close(Ref) ->
	State = erlang:get(Ref),
	[bitcask:close(BitcaskRef) || {_, BitcaskRef, _, _} <- State#state.open_instances],
	ok.

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
%%					case bitcask_lockops:acquire(write, DefState#bc_state.dirname) of
%%						{ok, WriteLock} ->
%%							DefState1 = DefState#bc_state{write_lock = WriteLock},
%%							NewDefState = bitcask:wrap_write_file(DefState1),
%%							ct:pal("DefState after wrap: ~p~n", [NewDefState]),
%%							erlang:put(DefRef, NewDefState),
%%							OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
%%							NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
%%							erlang:put(Ref, NewState),
%%							Ref;
%%						Error ->
%%							throw({unrecorverable, Error})
%%					end
%%				_ ->
%%					NewDefState = bitcask:wrap_write_file(DefState),
%%					ct:pal("DefState before wrap22222222222: ~p~n", [NewDefState]),
%%					erlang:put(DefRef, NewDefState),
%%					OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
%%					NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
%%					erlang:put(Ref, NewState),
%%					Ref
			end
	end.

%%get(Ref, Key) ->
%%	get(Ref, Key, []).
get(Ref, {Bucket, Key}, Opts) ->
	Split = proplists:get_value(split, Opts),
	NewKey = make_bitcask_key(3, {Bucket, atom_to_binary(Split, latin1)}, Key),
	get(Ref, NewKey, Opts);
get(Ref, Key1, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	Split = proplists:get_value(split, Opts, default),

	{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
	DefState = erlang:get(DefRef),
	{Split, SplitRef, _, _Active} = lists:keyfind(Split, 1, OpenInstances),
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
							not_found;
						Entry ->
							bitcask:check_get(Key1, Entry, DefRef, DefState, 2)
					end;
				Entry ->
					bitcask:check_get(Key1, Entry, SplitRef, SplitState, 2)
			end
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
	Split = proplists:get_value(split, Opts, default),

	{Split, BitcaskRef, _HasMerged, IsActive} = lists:keyfind(Split, 1, OpenInstances),

	%% Firstly we want to check if the file is ready to be put to, if it is not then put to default.
	%% If it is ready then perform a get of the object from the default location and delete it from there
	%% if it exists.
	case IsActive of
		false ->
			{default, BitcaskRef1, _, _} = lists:keyfind(default, 1, OpenInstances),
			bitcask:put(BitcaskRef1, Key1, Value, Opts);
		true when Split =:= default ->
			bitcask:put(BitcaskRef, Key1, Value, Opts);
		true ->
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
	end.

delete(Ref, Key) ->
	delete(Ref, Key, []).
delete(Ref, Key, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	Split = proplists:get_value(split, Opts, default),

	{Split, SplitRef, _, Active} = lists:keyfind(Split, 1, OpenInstances),

	case Active of
		false ->
			{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
			bitcask:delete(DefRef, Key, Opts);
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
			end
	end.

sync(Ref) ->
	State = erlang:get(Ref),
	[bitcask:sync(BRef) || {_, BRef, _, _} <- State#state.open_instances],
	ok.


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

fold_keys(Ref, Fun, Acc) ->
	fold_keys(Ref, Fun, Acc, []).

fold_keys(Ref, Fun, Acc, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	{Split, SplitRef, _, _} = lists:keyfind(Split, 1, State#state.open_instances),
	SplitState = erlang:get(SplitRef),

	MaxAge = bitcask:get_opt(max_fold_age, SplitState#bc_state.opts) * 1000, % convert from ms to us
	MaxPuts = bitcask:get_opt(max_fold_puts, SplitState#bc_state.opts),

	fold_keys(Ref, Fun, Acc, Opts, MaxAge, MaxPuts, false).

fold_keys(Ref, Fun, Acc, Opts, MaxAge, MaxPuts, SeeTombStoneP) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	{Split, SplitRef, HasMerged, Active} = lists:keyfind(Split, 1, OpenInstances),
	AllSplits = proplists:get_value(all_splits, Opts, false),
	case AllSplits of
		false ->
			case Active of
				false ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold_keys(DefRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP);
				true when Split =:= default ->
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
					end
			end;
		true ->
			lists:flatten([bitcask:fold_keys(SplitRef0, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP) || {_Split0, SplitRef0, _, SplitActive} <- OpenInstances, SplitActive =:= true])
	end.

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

fold(Ref, Fun, Acc, Opts, MaxAge, MaxPut, SeeTombstonesP) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	AllSplits = proplists:get_value(all_splits, Opts, false),

	case AllSplits of
		false ->
			case lists:keyfind(Split, 1, OpenInstances) of
				{Split, SplitRef, _, _} ->
					bitcask:fold(SplitRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP);
				false ->
					{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
					bitcask:fold(DefRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP)
			end;
		true ->
			lists:flatten([bitcask:fold(SplitRef, Fun, Acc, MaxAge, MaxPut, SeeTombstonesP) || {_, SplitRef, _, _} <- OpenInstances])
	end.

%% TODO This is unfinished, need to check for `active` splits to merge.
merge(Ref, Opts) ->
	State = erlang:get(Ref),
	_OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,

	[begin
		 FilesToMerge = bitcask:readable_files(Dir),
		 bitcask:merge(Dir, Opts, FilesToMerge)
	 end || {_Split, Dir} <- OpenDirs].

%%merge(Ref, Opts, FilesToMerge) when is_reference(Ref) ->
%%	merge(Ref, Opts, FilesToMerge);
merge(_RootDir, Opts, FilesToMerge) ->
	[bitcask:merge(Dir, Opts, Files) || {Dir, Files} <- FilesToMerge].
%%	State = erlang:get(Ref),
%%	ct:pal("Bitcask manager merge State of the ref passed in: ~p~n", [State]),
%%	[bitcask:merge(Dirname, Opts, proplists:get_value(Split, FilesToMerge, {[], []})) || {Split, Dirname} <- State#state.open_dirs].

special_merge(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
	{Split1, _SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, SplitRef2, HasMerged2, IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

	MState = case IsActive2 of
				 false ->
					 io:format("Cannot merge split: ~p due to not being active for merging", [Split1]);
				 true ->
					 case HasMerged2 of
						 true ->
							 io:format("This Split has already been merged: ~p~n", [Split2]);
						 false ->
							 Dirname1 = proplists:get_value(Split1, State#state.open_dirs),
							 Dirname2 = proplists:get_value(Split2, State#state.open_dirs),
							 FilesToMerge = bitcask:readable_files(Dirname1),

							 NewState = prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts, Ref),
							 merge_files(NewState)
					 end
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
	ok = bitcask_lockops:release(MState#mstate.merge_lock),

	OpenInstances = lists:keyreplace(Split2, 1, State#state.open_instances, {Split2, SplitRef2, true, IsActive2}),
	NewState1 = State#state{open_instances = OpenInstances},
	erlang:put(Ref, NewState1),
	ok.

needs_merge(Ref) ->
	needs_merge(Ref, []).
needs_merge(Ref, Opts) ->
	State = erlang:get(Ref),
	Files = [{proplists:get_value(Split, State#state.open_dirs), bitcask:needs_merge(BRef, Opts)} || {Split, BRef, _, _} <- State#state.open_instances],
%%	ct:pal("Files: ~p~n", [Files]),
%%	Files1 = [{X, Y} || {X, Y} <- Files, Y =/= false],
%%	ct:pal("Files1: ~p~n", [Files1]),
%%	[{splitname, {true, {[], []}}}]
%%	[{splitname, {[], []}}]
	case [{X, Z} || {X, {_Y, Z} = I} <- Files, I =/= false, X =/= undefined] of
		[] ->
			false;
		NewFiles ->
			{true, NewFiles}
	end.

%%needs_merge(Ref, Opts) ->
%%	State = erlang:get(Ref),
%%	Files = [{Split, bitcask:needs_merge(BRef, Opts)} || {Split, BRef, _, _} <- State#state.open_instances],
%%	Files1 = [Y || {_X, Y} <- Files, Y =/= false],
%%	Results = lists:foldl(
%%		fun({_Split, BRef, _, _}, {LiveFiles0, DeadFiles0} = Acc) ->
%%			case bitcask:needs_merge(BRef, Opts) of
%%				false ->
%%					Acc;
%%				{true, {LiveFiles, DeadFiles}} ->
%%					{lists:append(LiveFiles, LiveFiles0), lists:append(DeadFiles, DeadFiles0)}
%%			end
%%		end, {[], []}, State#state.open_instances),
%%
%%	case Results of
%%		{[], []} ->
%%			false;
%%		NewFiles ->
%%			{true, NewFiles}
%%	end.

merge_files(#mstate{input_files = []} = State) ->
	State;
merge_files(#mstate{origin_dirname = Dirname,
	input_files = [File | Rest],
	decode_disk_key_fun = DecodeDiskKeyFun} = State) ->
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
				Split = ?FIND_SPLIT_FUN(K1),
				DestinationSplit = State0#mstate.destination_splitname,

				case Split of
					DestinationSplit ->
						transfer_split(K1, K0, V, Tstamp, TstampExpire, FileId, Pos, State0);
					_ ->
						State0
				end
		end
		end,
	State2 = try bitcask_fileops:fold(File, F, State) of
				 #mstate{delete_files = DelFiles} = State1 ->
					 State1#mstate{delete_files = [File | DelFiles]}        %% TODO We don't want to delete files in this merge or else the main merge will miss data
			 catch
				 throw:{fold_error, Error, _PartialAcc} ->
					 error_logger:error_msg(
						 "merge_files: skipping file ~s in ~s: ~p\n",
						 [File#filestate.filename, Dirname, Error]),
					 State
			 end,
	merge_files(State2#mstate{input_files = Rest}).

transfer_split(KeyDirKey, DiskKey, V, Tstamp, TstampExpire, _FileId, {_, _, _Offset, _} = _Pos, State) ->
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

	ct:pal("Transferring Split for Key: ~p,  ~p and Value: ~p is tombstone: ~p~n", [KeyDirKey, make_riak_key(KeyDirKey), V, bitcask:is_tombstone(V)]),
	{ok, Outfile, Offset, Size} =
		bitcask_fileops:write(State1#mstate.out_file, DiskKey, V, Tstamp),

	OutFileId = bitcask_fileops:file_tstamp(Outfile),

	Outfile2 =
		case bitcask_nifs:keydir_put(State1#mstate.destination_live_keydir, KeyDirKey,
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
	OriginWriteFile = Split1State#bc_state.write_file,	%% FIXED - TODO Check write file is not `fresh` which it will be if bitcask has just started and this merge triggers.

	%% TODO Need to add case for `not_found` for keydir get. This happens if a key has been deleted.
	%% TODO Deleted Keys reappear in list keys after special merge, investigate why.
	%% TODO this needs a tombstone check before the keydir one, if the value is a tombstone we need to write to the new location but update the fstats and keydir to say its a tombstone
	case bitcask_nifs:keydir_get(State1#mstate.origin_live_keydir, KeyDirKey) of
		#bitcask_entry{tstamp = OldTstamp, file_id = OldFileId,
			offset = OldOffset} ->
			Tombstone = <<?TOMBSTONE2_STR, OldFileId:32>>,
%%			Tstamp = bitcask_time:tstamp(),
			case bitcask_fileops:write(OriginWriteFile, DiskKey,        %% Find out about using diskkey here or a tombstone key isntead?
				Tombstone, Tstamp) of
				{ok, WriteFile2, _, TSize} ->
					ok = bitcask_nifs:update_fstats(
						Split1State#bc_state.keydir,
						bitcask_fileops:file_tstamp(WriteFile2), Tstamp,
						0, 0, 0, 0, TSize, _ShouldCreate = 1),
					case bitcask_nifs:keydir_remove(State1#mstate.origin_live_keydir,
						KeyDirKey, OldTstamp,
						OldFileId, OldOffset) of
						already_exists ->
							%% Merge updated the keydir after tombstone
							%% write.  beat us, so undo and retry in a
							%% new file.
%%							{ok, _WriteFile3} =
%%								bitcask_fileops:un_write(WriteFile2);
%%							State3 = wrap_write_file(
%%								Split1State#bc_state {
%%									write_file = WriteFile3 });
%%							{ok, Split1State},
							State1#mstate{out_file = Outfile2};
						ok ->
%%							{ok, Split1State#bc_state { write_file = WriteFile2 }},
							State1#mstate{out_file = Outfile2}
					end;
				{error, _} = ErrorTomb ->
					throw({unrecoverable, ErrorTomb, Split1State})
			end;
		not_found ->
			State1#mstate{out_file = Outfile2}
	end.

prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts, Ref) ->

	DecodeDiskKeyFun = bitcask:get_decode_disk_key_fun(bitcask:get_opt(decode_disk_key_fun, Opts)),

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
		read_write_p = 0,
		opts = Opts,
		delete_files = []}.


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
make_riak_key(<<?VERSION_0:8, _Rest/binary>> = BK) ->
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

check_backend_exists(Ref, Key) ->
	State = erlang:get(Ref),
	case lists:keyfind(Key, 1, State#state.open_instances) of
		false ->
			false;
		_ ->
			true
	end.

is_active(Ref, Split) ->
	State = erlang:get(Ref),
	case lists:keyfind(Split, 1, State#state.open_instances) of
		{Split, _BRef, _, Active} ->
			Active;
		false ->
			false
	end.

has_merged(Ref, Split) ->
	State = erlang:get(Ref),
	case lists:keyfind(Split, 1, State#state.open_instances) of
		{Split, _SRef, HasMerged, _} ->
			HasMerged;
		false ->
			false
	end.

%% TODO Both these calls need to be reviewed as to what they actually do and best way for riak to know which split to call on.
is_empty_estimate(Ref) ->
	State = erlang:get(Ref),
	{default, BRef, _, _} = lists:keyfind(default, 1, State#state.open_instances),
	bitcask:is_empty_estimate(BRef).

status(Ref) ->
	State = erlang:get(Ref),
	{default, BRef, _, _} = lists:keyfind(default, 1, State#state.open_instances),
	[bitcask:status(BRef)].
%%	[bitcask:status(erlang:get(BRef)) || {_, BRef, _, _} <- OpenInstances].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%%manager_merge_test() ->
%%	os:cmd("rm -rf /tmp/bc.man.merge"),
%%	Dir = "/tmp/bc.man.merge",
%%	Key1 = make_bitcask_key(3, {<<"b1">>, <<"default">>}, <<"k1">>),
%%	Key2 = make_bitcask_key(3, {<<"b2">>, <<"default">>}, <<"k2">>),
%%	Key3 = make_bitcask_key(3, {<<"b3">>, <<"default">>}, <<"k3">>),
%%	Key4 = make_bitcask_key(3, {<<"b4">>, <<"second">>}, <<"k4">>),
%%	Key5 = make_bitcask_key(3, {<<"b5">>, <<"second">>}, <<"k5">>),
%%	Key6 = make_bitcask_key(3, {<<"b6">>, <<"second">>}, <<"k6">>),
%%%%	_Keys = [Key1, Key2, Key3, Key4, Key5, Key6],
%%	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 1}]),
%%	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 1}]),
%%
%%	bitcask_manager:put(B, Key1, <<"Value1">>, [{split, default}]),
%%	bitcask_manager:put(B, Key2, <<"Value2">>, [{split, default}]),
%%	bitcask_manager:put(B, Key3, <<"Value3">>, [{split, default}]),
%%	bitcask_manager:put(B, Key4, <<"Value4">>, [{split, second}]),
%%	bitcask_manager:put(B, Key5, <<"Value5">>, [{split, second}]),
%%	bitcask_manager:put(B, Key6, <<"Value6">>, [{split, second}]),
%%
%%	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, default}]),
%%	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, default}]),
%%	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, default}]),
%%	%% Here we test that data can be fetched from both splits. Since we
%%	%% have not merged to the new split the data is actually still in
%%	%% the default location.
%%	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, default}]),
%%	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, default}]),
%%	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, default}]),
%%	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
%%	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
%%	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),
%%
%%	BState = erlang:get(B),
%%	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
%%	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
%%	DefState = erlang:get(DefRef),
%%	SplitState = erlang:get(SplitRef),
%%
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key4),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key5),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key6),
%%	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key4),
%%	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key5),
%%	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key6),
%%
%%	DefDir = lists:concat([Dir, "/", default]),
%%	ct:pal("DefDir: ~p~n", [DefDir]),
%%	Files = bitcask_fileops:data_file_tstamps(DefDir),
%%	ct:pal("Files in the default dir location: ~p~n", [Files]),
%%
%%	DefDir2 = lists:concat([Dir, "/", second]),
%%	ct:pal("DefDir: ~p~n", [DefDir2]),
%%	Files2 = bitcask_fileops:data_file_tstamps(DefDir2),
%%	ct:pal("Files in the second dir location: ~p~n", [Files2]),
%%
%%	FileFun =
%%		fun(N, Split) ->
%%			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
%%		end,
%%	MergeFiles = [{X, FileFun(X, default)} || X <- lists:seq(1, 6)],
%%
%%	?assertEqual(MergeFiles, Files),
%%	?assertEqual([], Files2),
%%
%%%%	delete(B, Key6, [{split, default}]),
%%%%	not_found = bitcask_manager:get(B, Key6, [{split, second}]),
%%
%%	ct:pal("MergeFiles: ~p~n", [MergeFiles]),
%%
%%%%	bitcask_manager:close(B),
%%
%%%%	C = bitcask_manager:open(Dir, [{read_write, true}, {split, default}]),
%%%%	bitcask_manager:open(C, Dir, [{read_write, true}, {split, second}]),
%%
%%%%	ct:pal("Reopened the instances: ~p~n", [erlang:get(C)]),
%%
%%	bitcask_manager:activate_split(B, second),
%%
%%	ct:pal("Activared backend, check if it is: ~p~n", [erlang:get(B)]),
%%
%%
%%	bitcask_manager:special_merge(B, default, second, [{max_file_size, 1}]),
%%
%%	Files3 = bitcask_fileops:data_file_tstamps(DefDir2),
%%	ct:pal("Files in the second dir location again: ~p~n", [Files3]),
%%	MergeFiles1 = [{X, FileFun(X, second)} || X <- lists:seq(1, 3)],
%%	?assertEqual(Files3, MergeFiles1),
%%
%%	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
%%	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
%%	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),
%%	not_found = bitcask_manager:get(B, Key4, [{split, default}]),    %% TODO Check these are actually tombstones
%%	not_found = bitcask_manager:get(B, Key5, [{split, default}]),
%%	not_found = bitcask_manager:get(B, Key6, [{split, default}]),
%%
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key4),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key5),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key6),	%% TODO this should not be here considering it got deleted?
%%	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key4),
%%	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key5),
%%	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key6),
%%
%%	Key7 = make_bitcask_key(3, {<<"b7">>, <<"default">>}, <<"k7">>),
%%	Key8 = make_bitcask_key(3, {<<"b8">>, <<"second">>}, <<"k8">>),
%%
%%	bitcask_manager:put(B, Key7, <<"Value7">>, [{split, default}]),
%%	bitcask_manager:put(B, Key8, <<"Value8">>, [{split, second}]),
%%
%%	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),
%%%%	not_found = bitcask_manager:get(B, Key7, [{split, second}]),
%%	not_found = bitcask_manager:get(B, Key8, [{split, default}]),
%%	{ok, <<"Value8">>} = bitcask_manager:get(B, Key8, [{split, second}]),
%%
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key7),
%%	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key7),
%%	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key8),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key8),
%%
%%	Files4 = bitcask_fileops:data_file_tstamps(DefDir),
%%	Files5 = bitcask_fileops:data_file_tstamps(DefDir2),
%%	MergeFiles4 = [{X, FileFun(X, default)} || X <- lists:seq(1, 8)],
%%	MergeFiles5 = [{X, FileFun(X, second)} || X <- lists:seq(1, 4)],
%%	ct:pal("Files in the default dir location final: ~p~n", [Files4]),
%%	ct:pal("Files in the second dir location final: ~p~n", [Files5]),
%%	?assertEqual(Files4, MergeFiles4),
%%	?assertEqual(Files5, MergeFiles5),
%%
%%	ct:pal("Checking needs merge of defref: ~p~n", [bitcask:readable_files(DefDir)]),
%%	ct:pal("Checking needs merge of splitref: ~p~n", [bitcask:readable_files(DefDir2)]),
%%
%%	{true, Z} = needs_merge(B),
%%	ct:pal("==========Needs merge: ~p~n", [Z]),
%%
%%	ok = bitcask_manager:merge(Dir, [], Z),
%%
%%	bitcask_manager:put(B, Key8, <<"Value8">>, [{split, second}]),
%%
%%	ct:pal("Checking needs merge of defref2: ~p~n", [bitcask:readable_files(DefDir)]),
%%	ct:pal("Checking needs merge of splitref2: ~p~n", [bitcask:readable_files(DefDir2)]),
%%
%%	Files6 = bitcask_fileops:data_file_tstamps(DefDir),
%%	Files7 = bitcask_fileops:data_file_tstamps(DefDir2),
%%	ct:pal("Files in the default dir location final: ~p~n", [Files6]),
%%	ct:pal("Files in the second dir location final: ~p~n", [Files7]),
%%
%%	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
%%	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
%%	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),
%%	{ok, <<"Value8">>} = bitcask_manager:get(B, Key8, [{split, second}]),
%%
%%	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, default}]),
%%	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, default}]),
%%	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, default}]),
%%	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),
%%
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key2),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key3),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key7),
%%
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key4),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key5),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key6),
%%	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key8),
%%
%%	{ok, something} = bitcask_manager:get(B, Key4, [{split, default}]), %% This is here to cause failure to check logs
%%
%%	ok.

activate_test() ->
	os:cmd("rm -rf /tmp/bc.man.merge"),
	Dir = "/tmp/bc.man.merge",

	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 1}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 1}]),

	ct:pal("################# Test #######################"),

	bitcask_manager:activate_split(B, second),

	bitcask_manager:close(B),

	ok.

%% TODO Need to replicate riak bug where deleted key reappears after special merge, appears in fold_keys.
fold_keys_test() ->
	os:cmd("rm -rf /tmp/bc.man.fold"),
	Dir = "/tmp/bc.man.fold",
ct:pal("FoldKeys test ###############################"),
	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}]),

	Keys = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(0,4)],
	Keys2 = [make_bitcask_key(3, {<<"second">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(5,6)],
	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],

	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],

	FoldFun = fun(_, Key, Acc) -> [Key | Acc] end,
	FoldKeysFun = fold_keys_fun(FoldFun, <<"second">>),

	ListKeys = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	ct:pal("Listkeys output: ~p~n", [lists:sort(ListKeys)]),

	ExpectedKeys = [integer_to_binary(N) || N <- lists:seq(0,4)],
	?assertEqual(ExpectedKeys, lists:sort(ListKeys)),

	bitcask_manager:delete(B, make_bitcask_key(3, {<<"second">>, <<"second">>}, <<"1">>), [{split, second}]),
	BState = erlang:get(B),
	{second, SRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	SplitState = erlang:get(SRef),
	not_found = bitcask_manager:get(B, lists:nth(2, Keys), [{split, second}]),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, lists:nth(2, Keys)),

%%	ListKeys1 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
%%	ct:pal("Listkeys1 output: ~p~n", [lists:sort(ListKeys1)]),

	bitcask_manager:activate_split(B, second),

	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys2],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys2],

	ListKeys2 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	ct:pal("Listkeys2 after activate output: ~p~n", [lists:sort(ListKeys2)]),
	ExpectedKeys2 = [integer_to_binary(N) || N <- lists:seq(0,6), N =/= 1],
	?assertEqual(ExpectedKeys2, lists:sort(ListKeys2)),

	bitcask_manager:special_merge(B, default, second, []),
	BState1 = erlang:get(B),
	{second, SRef1, _, _} = lists:keyfind(second, 1, BState1#state.open_instances),
	SplitState1 = erlang:get(SRef1),
	not_found = bitcask_manager:get(B, lists:nth(2, Keys), [{split, second}]),
	not_found = bitcask_nifs:keydir_get(SplitState1#bc_state.keydir, lists:nth(2, Keys)),	%% Fails - TODO This should not exist as its the tombstone from the original split moved to the new spot
	%% TODO Looks like value is in the keydir when moved to the new split, its a tombstone that on a get gets checked and since is tombstone is not returned.
	%% TODO However in a fold this check is not done and just keydir vals are returned. Since this doesnt happedn in the original split before the transfer the key must get removed from the keydir when deleted 
	ListKeys3 = bitcask_manager:fold_keys(B, FoldKeysFun, [], [{split, second}]),
	ct:pal("Listkeys3  after special mergeoutput: ~p~n", [lists:sort(ListKeys3)]),

	bitcask_manager:close(B),
	ok.

fold_keys_fun(FoldKeysFun, Bucket) ->
	fun(#bitcask_entry{key=BK}, Acc) ->
%%		lager:info("fold_keys_fun make222 key: ~p~n", [make_riak_key(BK)]),
		ct:pal("in fold key: ~p~n", [make_riak_key(BK)]),
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


%% TODO 2 issues here. 1. at some point the write file remains the same but it is also added to the read_files list?
%% TODO		2. The 10th key seems to take the current write file add it to the readfiles and create the same id file to write to? Or perhaps ots just added to read_files and remains as the read file too?
%% TODO ANSWER! For above two, a new read_file is created if attempted to do a get on the latest put data that is still in the write_file. So a readonly version is created and added to the read_files.
%% TODO 	3. File that gets created on an activate seems to stick around and isn't put to or cleared in a merge. It does not get picked up on "needs_merge" call. Put reason may be due to checking file size and key being bigger than allowed and file not being "fresh"
another_test() ->
	os:cmd("rm -rf /tmp/bc.man.merge2"),
	Dir = "/tmp/bc.man.merge2",

%%	Size = 16 + size(make_bitcask_key(3, {<<"b6">>, <<"default">>}, <<"k6">>)) + size(<<"Value6">>),

	B = bitcask_manager:open(Dir, [{read_write, true}, {split, default}, {max_file_size, 50}]),
	bitcask_manager:open(B, Dir, [{read_write, true}, {split, second}, {max_file_size, 50}]),

	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState = erlang:get(DefRef),
	SplitState = erlang:get(SplitRef),

	ct:pal("################# Test 22222222 #######################"),
	Keys = [make_bitcask_key(3, {<<"b1">>, <<"second">>}, integer_to_binary(N)) || N <- lists:seq(1,10)],
	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],

	[{ok, Key} = bitcask_manager:get(B, Key, [{split, default}]) || Key <- Keys],
	[{ok, Key} = bitcask_manager:get(B, Key, [{split, second}]) || Key <- Keys],

	Key1 = make_bitcask_key(3, {<<"b1">>, <<"second">>}, <<"k1">>),
	Key2 = make_bitcask_key(3, {<<"b2">>, <<"second">>}, <<"k2">>),
	Key3 = make_bitcask_key(3, {<<"b3">>, <<"second">>}, <<"k3">>),

	bitcask_manager:put(B, Key1, <<"Value1">>, [{split, second}]),
	bitcask_manager:put(B, Key2, <<"Value2">>, [{split, second}]),
	bitcask_manager:put(B, Key3, <<"Value3">>, [{split, second}]),

	_DefState001 = erlang:get(DefRef),
%%	ct:pal("DefState before activate1: ~p~n", [DefState001]),

	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, default}]),
	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, default}]),
	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, default}]),
	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, second}]),
	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, second}]),
	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, second}]),

	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1),
	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key2),
	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key3),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key1),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key2),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key3),

	DefDir = lists:concat([Dir, "/", default]),
	SplitDir = lists:concat([Dir, "/", second]),
	_Files4 = bitcask_fileops:data_file_tstamps(DefDir),
	_Files5 = bitcask_fileops:data_file_tstamps(SplitDir),

%%	ct:pal("Files in default location: ~p~n", [Files4]),
%%	ct:pal("Files in split location: ~p~n", [Files5]),

	_DefState00 = erlang:get(DefRef),
%%	ct:pal("DefState before activate2: ~p~n", [DefState00]),

	NewB = bitcask_manager:activate_split(B, second),	%% TODO find out why putting after activation doesn't use the current write file?
	BState0 = erlang:get(NewB),
	{default, DefRef0, _, _} = lists:keyfind(default, 1, BState0#state.open_instances),

	_DefState11 = erlang:get(DefRef0),
%%	ct:pal("DefState after activate: ~p~n", [DefState11]),

	bitcask_manager:put(B, make_bitcask_key(3, {<<"b6">>, <<"default">>}, <<"k6">>), <<"Value6">>, [{split, default}]),

%%	ct:pal("State: ~p~n", [erlang:get(B)]),
	_DefState1 = erlang:get(DefRef),
%%	ct:pal("DefState after 1 put: ~p~n", [DefState1]),

	[bitcask_manager:put(B, Key, Key, [{split, second}]) || Key <- Keys],

	_DefState2 = erlang:get(DefRef),
%%	ct:pal("DefState after after putting more: ~p~n", [DefState2]),

	_Files6 = bitcask_fileops:data_file_tstamps(DefDir),
	_Files7 = bitcask_fileops:data_file_tstamps(SplitDir),

%%	ct:pal("Files in default location: ~p~n", [Files6]),
%%	ct:pal("Files in split location: ~p~n", [Files7]),

	{true, Z} = needs_merge(B),
%%	ct:pal("==========Needs merge: ~p~n", [Z]),

	MergeResponses = bitcask_manager:merge(B, [], Z),
	timer:sleep(2000),

	_Files8 = bitcask_fileops:data_file_tstamps(DefDir),
	_Files9 = bitcask_fileops:data_file_tstamps(SplitDir),

%%	ct:pal("Files in default location: ~p~n", [Files8]),
%%	ct:pal("Files in split location: ~p~n", [Files9]),

	_DefState0 = erlang:get(DefRef),
%%	ct:pal("DefState after merge: ~p~n", [DefState0]),

	_Z0 = needs_merge(B),
%%	ct:pal("==========Needs merge again: ~p~n", [Z0]),

	_Files88 = bitcask_fileops:data_file_tstamps(DefDir),
	_Files99 = bitcask_fileops:data_file_tstamps(SplitDir),

%%	ct:pal("Files in default location after second need-merge: ~p~n", [Files88]),
%%	ct:pal("Files in split location after second needs-merge: ~p~n", [Files99]),

	_DefState9 = erlang:get(DefRef),
%%	ct:pal("DefState after merge: ~p~n", [DefState9]),

	bitcask_manager:put(B, make_bitcask_key(3, {<<"b6">>, <<"default">>}, <<"k7">>), <<"Value6">>, [{split, default}]),
	bitcask_manager:put(B, make_bitcask_key(3, {<<"b6">>, <<"default">>}, <<"k8">>), <<"Value6">>, [{split, default}]),

	_DefState999 = erlang:get(DefRef),
%%	ct:pal("DefState after merge: ~p~n", [DefState999]),

	_Files888 = bitcask_fileops:data_file_tstamps(DefDir),
	_Files999 = bitcask_fileops:data_file_tstamps(SplitDir),

%%	ct:pal("Files in default location after second need-merge: ~p~n", [Files888]),
%%	ct:pal("Files in split location after second needs-merge: ~p~n", [Files999]),


	?assertEqual(length(BState#state.open_instances), length(MergeResponses)),
	timer:sleep(10000),
	%% TODO finish test to conclude properly. Works for checking merge files and states are correct for now.
	{ok, something} = bitcask_manager:get(B, Key1, [{split, default}]), %% This is here to cause failure to check logs
	ok.



	-endif.