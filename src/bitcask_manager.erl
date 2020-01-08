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
	list_keys/2,
	fold_keys/4,
	fold_keys/7,
	merge/4,
	special_merge/4,
	make_bitcask_key/3,
	check_and_upgrade_key/2
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
	end ).
-define(CHECK_AND_UPGRADE_KEY, fun check_and_upgrade_key/2).

-ifdef(namespaced_types).
-type bitcask_set() :: sets:set().
-else.
-type bitcask_set() :: set().
-endif.

-record(mstate, { origin_dirname :: string(),
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
	open_instances 	:: [{atom(), reference(), boolean(), boolean()}], %% {split, ref, has_merged, is_activated}
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

	case lists:keyfind(Split, 1, OpenInstances) of
		false ->
			NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
			BitcaskRef = bitcask:open(NewDir, Opts),
			State1 = State#state{
				open_instances 	= [{Split, BitcaskRef, false, IsActive} | OpenInstances],
				open_dirs 		= [{Split, NewDir} | OpenDirs]},
			erlang:put(Ref, State1);
		_ ->
			io:format("Bitcask instance already open for Dir: ~p and Split: ~p~n", [Dir, Split]),
			Ref
	end.

close(Ref) ->
	State = erlang:get(Ref),
	[bitcask:close(BitcaskRef) || {_, BitcaskRef, _, _} <- State#state.open_instances].

activate_split(Split, Ref) ->
	State = erlang:get(Ref),
	{Split, SplitRef, HasMerged, IsActive} = lists:keyfind(Split, 1, State#state.open_instances),

	case IsActive of
		true ->
			ok;
		false ->
			{default, DefRef, _DefHasMerged, _DefIsActive} = lists:keyfind(default, 1, State#state.open_instances),
			DefState = erlang:get(DefRef),
			case DefState#bc_state.write_lock of
				undefined ->
					case bitcask_lockops:acquire(write, DefState#bc_state.dirname) of
						{ok, WriteLock} ->
							DefState1 = DefState#bc_state{write_lock = WriteLock},
							NewDefState = bitcask:wrap_write_file(DefState1),
							erlang:put(DefRef, NewDefState),
							OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
							NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
							erlang:put(Ref, NewState);
						Error ->
							throw({unrecorverable, Error})
					end;
				_ ->
					NewDefState = bitcask:wrap_write_file(DefState),
					erlang:put(DefRef, NewDefState),
					OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
					NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
					erlang:put(Ref, NewState)
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
%%	Split = ?FIND_SPLIT_FUN(Key1),
	Split = proplists:get_value(split, Opts),

	{default, DefRef, _, _} = lists:keyfind(default, 1, OpenInstances),
	DefState = erlang:get(DefRef),
	{Split, SplitRef, _, _Active} = lists:keyfind(Split, 1, OpenInstances),
	SplitState = erlang:get(SplitRef),

%%	ct:pal("DefState: ~p~n", [DefState]),
%%	ct:pal("SplitState: ~p~n", [SplitState]),

	case Split of	%% Avoids us doing a lookup twice if the request is for default data
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
%%	Split = ?FIND_SPLIT_FUN(Key1),
	Split = proplists:get_value(split, Opts, default),

	{Split, BitcaskRef, _HasMerged, IsActive} = lists:keyfind(Split, 1, OpenInstances),

	%% Firstly we want to check if the file is ready to be put to, is not then put to default.
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
	end

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
	AllSplits = proplists:get_value(all_splits, Opts, false),

	case AllSplits of
		false ->
			{Split, SplitRef, _, _} = lists:keyfind(Split, 1, OpenInstances),
			bitcask:fold_keys(SplitRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP);
		true ->
			lists:flatten([bitcask:fold_keys(SplitRef, Fun, Acc, MaxAge, MaxPuts, SeeTombStoneP) || {_Split0, SplitRef, _, _} <- OpenInstances])
	end.

%% TODO This is unfinished, need to check for `active` splits to merge.
merge(Ref, _Dirname, Opts, FilesToMerge) ->
	State = erlang:get(Ref),
	_OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,

	[bitcask:merge(Dir, Opts, FilesToMerge) || {_Split, Dir} <- OpenDirs].

special_merge(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
	{Split1, _SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, _SplitRef2, HasMerged2, IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

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
	ok = bitcask_lockops:release(MState#mstate.merge_lock).

merge_files(#mstate { input_files = [] } = State) ->
	State;
merge_files(#mstate {  origin_dirname = Dirname,
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
					 State1#mstate{delete_files = [File|DelFiles]}		%% TODO We don't want to delete files in this merge or else the main merge will miss data
			 catch
				 throw:{fold_error, Error, _PartialAcc} ->
					 error_logger:error_msg(
						 "merge_files: skipping file ~s in ~s: ~p\n",
						 [File#filestate.filename, Dirname, Error]),
					 State
			 end,
	merge_files(State2#mstate { input_files = Rest }).

transfer_split(KeyDirKey, DiskKey, V, Tstamp, TstampExpire, _FileId, {_, _, _Offset, _} = _Pos, State) ->
%%	ct:pal("State: ~p~n", [State]),
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
				State#mstate { out_file = NewFile };
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
				State#mstate { out_file = NewFile }
		end,

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
	OriginWriteFile = Split1State#bc_state.write_file,	%% TODO Check write file is not `fresh` which it will be if bitcask has just started and this merge triggers.

	case bitcask_nifs:keydir_get(State1#mstate.origin_live_keydir, KeyDirKey) of
		#bitcask_entry{tstamp=OldTstamp, file_id=OldFileId,
			offset=OldOffset} ->
			Tombstone = <<?TOMBSTONE2_STR, OldFileId:32>>,
			Tstamp = bitcask_time:tstamp(),
			case bitcask_fileops:write(OriginWriteFile, DiskKey,		%% Find out about using diskkey here or a tombstone key isntead?
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
							State1#mstate { out_file = Outfile2 };
						ok ->
%%							{ok, Split1State#bc_state { write_file = WriteFile2 }},
							State1#mstate { out_file = Outfile2 }
					end;
				{error, _} = ErrorTomb ->
					throw({unrecoverable, ErrorTomb, Split1State})
			end
	end.
	%% TODO Need ot chekc



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
								{ok, Fstate}    -> Fstate;
								{error, _}      -> skip
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

	InFiles = lists:sort(fun(#filestate{tstamp=FTL}, #filestate{tstamp=FTR}) ->
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

	#mstate { origin_dirname = Dirname1,
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
	bitcask_manager:put(B, Key4, <<"Value4">>, [{split, second}]),
	bitcask_manager:put(B, Key5, <<"Value5">>, [{split, second}]),
	bitcask_manager:put(B, Key6, <<"Value6">>, [{split, second}]),

	{ok, <<"Value1">>} = bitcask_manager:get(B, Key1, [{split, default}]),
	{ok, <<"Value2">>} = bitcask_manager:get(B, Key2, [{split, default}]),
	{ok, <<"Value3">>} = bitcask_manager:get(B, Key3, [{split, default}]),
	%% Here we test that data can be fetched from both splits. Since we
	%% have not merged to the new split the data is actually still in
	%% the default location.
	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, default}]),
	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, default}]),
	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, default}]),
	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),

	BState = erlang:get(B),
	{default, DefRef, _, _} = lists:keyfind(default, 1, BState#state.open_instances),
	{second, SplitRef, _, _} = lists:keyfind(second, 1, BState#state.open_instances),
	DefState = erlang:get(DefRef),
	SplitState = erlang:get(SplitRef),

	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key4),
	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key5),
	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key6),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key4),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key5),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key6),

	DefDir = lists:concat([Dir, "/", default]),
	ct:pal("DefDir: ~p~n", [DefDir]),
	Files = bitcask_fileops:data_file_tstamps(DefDir),
	ct:pal("Files in the default dir location: ~p~n", [Files]),

	DefDir2 = lists:concat([Dir, "/", second]),
	ct:pal("DefDir: ~p~n", [DefDir2]),
	Files2 = bitcask_fileops:data_file_tstamps(DefDir2),
	ct:pal("Files in the second dir location: ~p~n", [Files2]),

	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,
	MergeFiles = [{X, FileFun(X, default)} || X <- lists:seq(1,6)],

	?assertEqual(MergeFiles, Files),
	?assertEqual([], Files2),

	ct:pal("MergeFiles: ~p~n", [MergeFiles]),

%%	bitcask_manager:close(B),

%%	C = bitcask_manager:open(Dir, [{read_write, true}, {split, default}]),
%%	bitcask_manager:open(C, Dir, [{read_write, true}, {split, second}]),

%%	ct:pal("Reopened the instances: ~p~n", [erlang:get(C)]),

	bitcask_manager:activate_split(second, B),
	bitcask_manager:special_merge(B, default, second, [{max_file_size, 1}]),

	Files3 = bitcask_fileops:data_file_tstamps(DefDir2),
	ct:pal("Files in the second dir location again: ~p~n", [Files3]),
	MergeFiles1 = [{X, FileFun(X, second)} || X <- lists:seq(1,3)],
	?assertEqual(Files3, MergeFiles1),

	{ok, <<"Value4">>} = bitcask_manager:get(B, Key4, [{split, second}]),
	{ok, <<"Value5">>} = bitcask_manager:get(B, Key5, [{split, second}]),
	{ok, <<"Value6">>} = bitcask_manager:get(B, Key6, [{split, second}]),
	not_found = bitcask_manager:get(B, Key4, [{split, default}]),	%% TODO Check these are actually tombstones
	not_found = bitcask_manager:get(B, Key5, [{split, default}]),
	not_found = bitcask_manager:get(B, Key6, [{split, default}]),

	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key4),
	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key5),
	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key6),
	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key4),
	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key5),
	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key6),

	Key7 = make_bitcask_key(3, {<<"b7">>, <<"default">>}, <<"k7">>),
	Key8 = make_bitcask_key(3, {<<"b8">>, <<"second">>},  <<"k8">>),

	bitcask_manager:put(B, Key7, <<"Value7">>, [{split, default}]),
	bitcask_manager:put(B, Key8, <<"Value8">>, [{split, second}]),

	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),
%%	not_found = bitcask_manager:get(B, Key7, [{split, second}]),
	not_found = bitcask_manager:get(B, Key8, [{split, default}]),
	{ok, <<"Value8">>} = bitcask_manager:get(B, Key8, [{split, second}]),

	#bitcask_entry{} = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key7),
	not_found = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key7),
	not_found = bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key8),
	#bitcask_entry{} = bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key8),

	Files4 = bitcask_fileops:data_file_tstamps(DefDir),
	Files5 = bitcask_fileops:data_file_tstamps(DefDir2),
	ct:pal("Files in the default dir location final: ~p~n", [Files4]),
	ct:pal("Files in the second dir location final: ~p~n", [Files5]),

	{ok, something} = bitcask_manager:get(B, Key4, [{split, default}]), %% This is here to cause failure to check logs

	ok.



-endif.