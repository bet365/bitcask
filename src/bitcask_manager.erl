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
	list_keys/2,
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

-record(bc_state, {dirname :: string(),
	write_file :: [#filestate{}],     % File for writing, [write_file]
	write_lock :: [{atom(), reference()}],     % Reference to write lock
	read_files :: [#filestate{}],     % Files opened for reading
	max_file_size :: integer(),  % Max. size of a written file
	opts :: list(),           % Original options used to open the bitcask
	encode_disk_key_fun :: function(),
	decode_disk_key_fun :: function(),
	encode_riak_key_fun :: function(),
	decode_riak_key_fun :: function(),
	find_split_fun      :: function(),
	keydir :: reference(),       % Key directory
	read_write_p :: integer(),    % integer() avoids atom -> NIF
	% What tombstone style to write, for testing purposes only.
	% 0 = old style without file id, 2 = new style with file id
	tombstone_version = 2 :: 0 | 2
}).

-ifdef(namespaced_types).
-type bitcask_set() :: sets:set().
-else.
-type bitcask_set() :: set().
-endif.

-record(mstate, { origin_dirname :: string(),
	destination_dirname :: string(),
	origin_splitname :: atom(),
	destination_splitname :: atom(),
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
	State = #state{open_instances = [{Split, BitcaskRef, true, false}], open_dirs = [{Split, NewDir}]},
	Ref = make_ref(),
	erlang:put(Ref, State),
	Ref.
open(Ref, Dir, Opts) ->
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,
	Split = proplists:get_value(split, Opts, default),

	case lists:keyfind(Split, 1, OpenInstances) of
		false ->
			NewDir = lists:concat([Dir, "/", atom_to_list(Split)]),
			BitcaskRef = bitcask:open(NewDir, Opts),
			State1 = State#state{
				open_instances 	= [{Split, BitcaskRef, false, false} | OpenInstances],
				open_dirs 		= [{Split, NewDir} | OpenDirs]},
			erlang:put(Ref, State1);
		_ ->
			io:format("Bitcask instance already open for Dir: ~p and Split: ~p~n", [Dir, Split]),
			Ref
	end.

close(Ref) ->
	State = erlang:get(Ref),
	[bitcask:close(BitcaskRef) || {_, BitcaskRef} <- State#state.open_instances].

activate_split(Split, Ref) ->
	State = erlang:get(Ref),
	{Split, SplitRef, HasMerged, IsActive} = lists:keyfind(Split, 1, State#state.open_instances),

	case IsActive of
		true ->
			ok;
		false ->
			{default, DefRef, _DefHasMerged, _DefIsActive} = lists:keyfind(default, 1, State#state.open_instances),
			DefState = erlang:get(DefRef),
			NewDefState = bitcask:wrap_write_file(DefState),
			erlang:put(DefRef, NewDefState),
			OpenInstances = lists:keydelete(Split, 1, State#state.open_instances),
			NewState = State#state{open_instances = [{Split, SplitRef, HasMerged, true} | OpenInstances]},
			erlang:put(Ref, NewState)
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
	{Split, SplitRef, _, _} = lists:keyfind(Split, 1, OpenInstances),
	SplitState = erlang:get(SplitRef),

	%% TODO Pass the return entry to bitcask:get then it can handle the responses. bitcask:get(Ref, Key, Entry)

	case bitcask_nifs:keydir_get(SplitState#bc_state.keydir, Key1) of
		not_found ->
			case bitcask_nifs:keydir_get(DefState#bc_state.keydir, Key1) of
				not_found ->
					not_found;
				Entry ->
					bitcask:check_get(Key1, Entry, DefRef, DefState, 2)
			end;
		Entry ->
			bitcask:check_get(Key1, Entry, SplitRef, DefState, 2)
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
							error_could_not_delete
					end
			end
	end.


list_keys(Ref, Opts) ->
	Split = proplists:get_value(split, Opts, default),
	State = erlang:get(Ref),
	OpenInstances = State#state.open_instances,
	case Split of
		all ->
			lists:flatten([bitcask:list_keys(Ref0) || {_Split0, Ref0, _, _} <- OpenInstances]);
		_ ->
			{Split, BitcaskRef, _, _} = lists:keyfind(Split, 1, OpenInstances),
			bitcask:list_keys(BitcaskRef)
	end.


merge(Ref, _Dirname, Opts, FilesToMerge) ->
	State = erlang:get(Ref),
	_OpenInstances = State#state.open_instances,
	OpenDirs = State#state.open_dirs,

	case proplists:get_value(special_merge, Opts) of
		undefined ->
			[bitcask:merge(Dir, Opts, FilesToMerge) || {_Split, Dir} <- OpenDirs]
%%		true ->
%%			ct:pal("Triggering special mergecall"),
%%			MergeFun = fun(Split, Key, KeyDirKey, Value, Tstamp, TstampExpire) -> special_merge_fun(Split, Key, KeyDirKey, Value, Tstamp, TstampExpire, Ref) end,
%%			NewOpts = [{special_merge_fun, MergeFun} | Opts],
%%			[bitcask:merge(Dir, [{current_split, Split} | NewOpts], FilesToMerge) || {Split, Dir} <- OpenDirs],
%%			ok
	end.

special_merge(Ref, Split1, Split2, Opts) ->
	State = erlang:get(Ref),
%%	UnMergedSplits = [{Split, Ref, HasMerged, Active} || {Split, Ref, HasMerged, Active} <- State, HasMerged =:= false andalso Active =:= true],
	{Split1, _SplitRef1, _HasMerged1, _IsActive1} = lists:keyfind(Split1, 1, State#state.open_instances),
	{Split2, _SplitRef2, HasMerged2, IsActive2} = lists:keyfind(Split2, 1, State#state.open_instances),

	case IsActive2 of
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

					State = prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts),
					merge_files(State)

			end
	end.

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
					 State1#mstate{delete_files = [File|DelFiles]}
			 catch
				 throw:{fold_error, Error, _PartialAcc} ->
					 error_logger:error_msg(
						 "merge_files: skipping file ~s in ~s: ~p\n",
						 [File#filestate.filename, Dirname, Error]),
					 State
			 end,
	merge_files(State2#mstate { input_files = Rest }).
	%% TODO Need to call the fold here then recurse back into merge_files. Check the ABOVE!!!!

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
				State#mstate { out_file = NewFile };
			ok ->
				State;
			fresh ->
				%% create the output file and take the lock.
				{ok, NewFile} = bitcask_fileops:create_file(
					State#mstate.destination_dirname,
					State#mstate.opts,
					State#mstate.destination_live_keydir),
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
	%% TODO Still need to delete it from the origin keydir and write a tombstone to origin split
	State1#mstate { out_file = Outfile2 }.



prep_mstate(Split1, Split2, Dirname1, Dirname2, FilesToMerge, Opts) ->

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

	DestinationKeyDir = bitcask_nifs:maybe_keydir_new(Dirname2),

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

%%special_merge_fun(CurrentSplit, Key1, KeyDirKey, Value, Tstamp, TstampExpire, Ref) ->
%%	State = erlang:get(Ref),
%%	OpenInstances = State#state.open_instances,
%%	NewSplit = ?FIND_SPLIT_FUN(Key1),
%%	ct:pal("Special merge, New SPlit: ~p CurrentSPlit: ~p Value: ~p~n", [NewSplit, CurrentSplit, Value]),
%%
%%	case NewSplit of
%%		CurrentSplit ->
%%			false;
%%		_ ->
%%			case proplists:get_value(NewSplit, OpenInstances) of
%%				undefined ->
%%					ok;
%%				BitcaskRef ->
%%					BCState = erlang:get(BitcaskRef),
%%					KeyDir = BCState#bc_state.keydir,
%%					WriteFiles = BCState#bc_state.write_file,
%%					[WriteFile] = [W || W <- WriteFiles, W#filestate.split =:= NewSplit],
%%					%% TODO Now we have the file we should add all checks for size of the entry against the file and wrap if necessary
%%
%%					{ok, WriteFile2, Offset, Size} =
%%						bitcask_fileops:write(WriteFile, Key1, Value, Tstamp),
%%
%%					OutFileId = bitcask_fileops:file_tstamp(WriteFile2),
%%
%%					case bitcask_nifs:keydir_put(KeyDir, KeyDirKey,
%%						OutFileId,
%%						Size, Offset, Tstamp, TstampExpire,
%%						bitcask_time:tstamp(),
%%						0, 0) of
%%						ok ->
%%							WriteFile2;
%%						already_exists ->
%%							{ok, O} = bitcask_fileops:un_write(WriteFile2),
%%							O
%%					end
%%
%%			end
%%	end.





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
	Key7 = make_bitcask_key(3, {<<"b7">>, <<"second">>}, <<"k7">>),
	_Keys = [Key1, Key2, Key3, Key4, Key5, Key6, Key7],
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

	%% Put key to split it doesnt belong in and check if merge picks it up
	bitcask_manager:put(B, Key7, <<"Value7">>, [{split, default}]),
	{ok, <<"Value7">>} = bitcask_manager:get(B, Key7, [{split, default}]),

%%	NewKeys = bitcask_manager:list_keys(B, [{split, all}]),
%%
%%	ct:pal("Keys: ~p~n", [Keys]),
%%	ct:pal("Fetched Keys: ~p~n", [NewKeys]),
%%
%%	?assertEqual(NewKeys, Keys),
	DefDir = lists:concat([Dir, "/", default]),
	ct:pal("DefDir: ~p~n", [DefDir]),
	Files3 = bitcask_fileops:data_file_tstamps(DefDir),
	ct:pal("Files in the default dir location: ~p~n", [Files3]),


	FileFun =
		fun(N, Split) ->
			lists:concat([Dir, "/", Split, "/", N, ".", "bitcask.data"])
		end,
	MergeFiles = [FileFun(X, default) || X <- lists:seq(1,4)],
	MergeFiles2 = [FileFun(X, second) || X <- lists:seq(1,3)],

%%	MergeFiles3 = lists:flatten(MergeFiles, MergeFiles2),
	MergeFiles3 = lists:foldl(fun(X, A) -> [X | A] end, MergeFiles, MergeFiles2),
	ct:pal("MergeFiles1: ~p~n", [MergeFiles]),
	ct:pal("MergeFiles2: ~p~n", [MergeFiles2]),
	ct:pal("MergeFiles3: ~p~n", [MergeFiles3]),

	bitcask_manager:close(B),

	C = bitcask_manager:open(Dir, [{read_write, true}, {split, default}]),
	bitcask_manager:open(C, Dir, [{read_write, true}, {split, second}]),

	ct:pal("Reopened the instances: ~p~n", [erlang:get(C)]),
	Opts = [{find_split_fun, ?FIND_SPLIT_FUN}, {special_merge, true}],

	bitcask_manager:merge(C, Dir, Opts, MergeFiles3), %% TODO Find right options to send through

	{ok, <<"Value7">>} = bitcask_manager:get(C, Key7, [{split, default}]),
	{ok, <<"Value7">>} = bitcask_manager:get(C, Key7, [{split, second}]),

	ok.



-endif.