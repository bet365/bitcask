-define(TSTAMP_EXPIRE_KEY, tstamp_expire).
-define(DEFAULT_TSTAMP_EXPIRE, 0).

-define(DEFAULT_ENCODE_DISK_KEY_OPTS,
    [
        {?TSTAMP_EXPIRE_KEY, ?DEFAULT_TSTAMP_EXPIRE}
    ]).

-record(keyinfo, {
    key = <<>> :: binary(),
    tstamp_expire = ?DEFAULT_TSTAMP_EXPIRE :: integer()
}).

-record(bitcask_entry, { key :: binary(),
                         file_id :: integer(),
                         total_sz :: integer(),
                         offset :: integer() | binary(), 
                         tstamp :: integer(),
                         tstamp_expire = 0 :: integer()}).

%% @type filestate().
-record(filestate, {mode :: 'read_only' | 'read_write',     % File mode: read_only, read_write
                    filename :: string(), % Filename
                    tstamp :: integer(),   % Tstamp portion of filename
                    fd :: port(),       % File handle
                    hintfd :: port() | undefined,   % File handle for hints
                    hintcrc=0 :: integer(),  % CRC-32 of current hint
                    ofs :: non_neg_integer(), % Current offset for writing
                    l_ofs=0 :: non_neg_integer(),  % Last offset written to data file
                    l_hbytes=0 :: non_neg_integer(),% Last # bytes written to hint file
                    l_hintcrc=0 :: non_neg_integer()}). % CRC-32 of current hint prior to last write

-record(file_status, { filename :: string(),
                      fragmented :: integer(),
                      dead_bytes :: integer(),
                      total_bytes :: integer(),
                      oldest_tstamp :: integer(),
                      newest_tstamp :: integer(),
                      expiration_epoch :: non_neg_integer(),
                      oldest_tstamp_expire :: integer() }).


-record(bc_state, {dirname :: string(),
    write_file :: 'fresh' | 'undefined' | #filestate{},     % File for writing
    write_lock :: reference(),     % Reference to write lock
    read_files :: [#filestate{}],     % Files opened for reading
    max_file_size :: integer(),  % Max. size of a written file
    opts :: list(),           % Original options used to open the bitcask
    encode_disk_key_fun :: function(),
    decode_disk_key_fun :: function(),
    keydir :: reference(),       % Key directory
    read_write_p :: integer(),    % integer() avoids atom -> NIF
    % What tombstone style to write, for testing purposes only.
    % 0 = old style without file id, 2 = new style with file id
    tombstone_version = 2 :: 0 | 2
}).

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

-define(TOMBSTONE_PREFIX, "bitcask_tombstone").
-define(TOMBSTONE0_STR, ?TOMBSTONE_PREFIX).
-define(TOMBSTONE0, <<?TOMBSTONE0_STR>>).
-define(TOMBSTONE1_STR, ?TOMBSTONE_PREFIX "1").
-define(TOMBSTONE1_BIN, <<?TOMBSTONE1_STR>>).
-define(TOMBSTONE2_STR, ?TOMBSTONE_PREFIX "2").
-define(TOMBSTONE2_BIN, <<?TOMBSTONE2_STR>>).
-define(TOMBSTONE0_SIZE, size(?TOMBSTONE0)).
% Size of tombstone + 32 bit file id
-define(TOMBSTONE1_SIZE, (size(?TOMBSTONE1_BIN)+4)).
-define(TOMBSTONE2_SIZE, (size(?TOMBSTONE2_BIN)+4)).
% Change this to the largest size a tombstone value can have if more added.
-define(MAX_TOMBSTONE_SIZE, ?TOMBSTONE2_SIZE).
% Notice that tombstone version 1 and 2 are the same size, so not tested below
-define(IS_TOMBSTONE_SIZE(S), (S == ?TOMBSTONE0_SIZE orelse S == ?TOMBSTONE1_SIZE)).

-define(OFFSETFIELD_V1,  64).
-define(TOMBSTONEFIELD_V2, 1).
-define(OFFSETFIELD_V2,   63).
-define(TSTAMPFIELD,  32).
-define(KEYSIZEFIELD, 16).
-define(TOTALSIZEFIELD, 32).
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
-define(HEADER_SIZE,  14). % 4 + 4 + 2 + 4 bytes
-define(MAXKEYSIZE, 2#1111111111111111).
-define(MAXVALSIZE, 2#11111111111111111111111111111111).
-define(MAXOFFSET_V2, 16#7fffffffffffffff). % max 63-bit unsigned

%% for hintfile validation
-define(CHUNK_SIZE, 65535).
-define(MIN_CHUNK_SIZE, 1024).
-define(MAX_CHUNK_SIZE, 134217728).

-define(TEST_FILEPATH, "_build/test/log").
