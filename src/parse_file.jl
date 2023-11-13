const _API_DOCS_SHARED = """
## Arguments
- `input`: The input source to parse. Can be a `String` file path or an `IO` object.
- `schema`: An optional schema for the CSV file, if omitted, all columns would be parsed as `String`s. It can be
    - a single `DataType` in which case it will be used for all columns in the file,
    - a `Vector{DataType}` in which case each element of the vector would correspond to single column,
    - a `Dict{Symbol,DataType}` which will map a types to columns by name,
    - a `Dict{Int,DataType}` which will map types to columns by position, or
    - a `Base.Callable` which will be called with the column index and name and should return a `DataType`.
    For the vector case, the length must match the number of columns in the CSV file.
    For the dictionary case, the keys must be a subset of the column names in the CSV file (unless `validate_type_map` is set to `false`),
    columns that are not present in the mapping will be parsed as `String`s.
- `consume_ctx`: A user-defined `<:AbstractConsumeContext` object which will be used to dispatch on `consume!(::C, ::ParsedPayload)` to consume the parsed data om each of `nworkers` tasks in parallel.
## Keyword arguments
- `header`: How the column names should be determined.
    They can be given explicitly as a `Vector{Symbol}` or a `Vector{String}`, which must match the number of columns in the input.
    Alternatively a positive `Integer` can set the row number from which the header should be parsed.
    This number is relative to the first row that wasn't empty and/or commented if you set `ignoreemptyrows` and/or `comment`.
    A value of `0` or `false` indicates that no header is present in the CSV file.
    You can use `skipto` to skip over headers that fail to parse for whatever reason.
- `skipto`: The number of rows to skip before parsing the CSV file. Defaults to `0` (no skipping).
    This number is relative to the first row that wasn't empty and/or commented if you set `ignoreemptyrows` and/or `comment`.
- `limit`: The maximum number of rows to parse. Defaults to `0` (no limit). Used in [`ChunkedBase.ChunkingContext`](@ref).
### Parsing-related options
- `delim`: The delimiter character used in the CSV file. Defaults to `','`. Only single-byte characters are supported. `nothing` indicates that the delimiter should be inferred from the first chunk of data. Used in [`Parsers.Options`](@ref).
- `openquotechar`: The character used to open quoted fields in the CSV file. Defaults to `'"'`. Only single-byte characters are supported. Used in [`Parsers.Options`](@ref).
- `closequotechar`: The character used to close quoted fields in the CSV file. Defaults to `'"'`. Only single-byte characters are supported. Used in [`Parsers.Options`](@ref).
- `escapechar`: The character used to escape special characters in the CSV file. Defaults to `'"'`. Only single-byte characters are supported. Used in [`Parsers.Options`](@ref).
- `newlinechar`: The character used to represent newlines in the CSV file. Defaults to `'\\n'`. Only single-byte characters are supported. `nothing` indicates that the newline character should be inferred from the first chunk of data. Used in [`Parsers.Options`](@ref).
- `sentinel`: A sentinel value used to indicate missing values in the CSV file. Multiple sentinels might be provided as a `Vector{String}`. Used in [`Parsers.Options`](@ref).
    Defaults to `missing`, meaning that empty fields (two consecutive `delim`s) will be treated as missing values. Used in [`Parsers.Options`](@ref)
- `groupmark`: The character used to group digits in numbers in the CSV file. Defaults to `nothing` (group marks are not expected). Used in [`Parsers.Options`](@ref).
- `stripwhitespace`: Whether to strip whitespace from fields in the CSV file. Defaults to `false`. Used in [`Parsers.Options`](@ref).
- `ignorerepeated`: Whether to ignore repeated delimiters in the CSV file. Defaults to `false`. Used in [`Parsers.Options`](@ref).
- `truestrings`: A vector of strings representing `true` values in the CSV file. Defaults to `["true", "True", "TRUE", "1", "t", "T"]`. Used in [`Parsers.Options`](@ref).
- `falsestrings`: A vector of strings representing `false` values in the CSV file. Defaults to `["false", "False", "FALSE", "0", "f", "F"]`. Used in [`Parsers.Options`](@ref).
- `dateformat`: The date format used in the CSV file. Defaults to `nothing`. Consider using `GuessDateTime` as a schema type instead. Used in [`Parsers.Options`](@ref).
- `quoted`: Whether fields in the CSV file are quoted. Defaults to `true`. Used in [`Parsers.Options`](@ref).
- `decimal`: The character used as the decimal separator in numbers in the CSV file. Defaults to `'.'`. Only single-byte characters are supported. Used in [`Parsers.Options`](@ref).
- `ignoreemptyrows`: Whether to ignore empty rows in the CSV file. Defaults to `true`. Used in [`Parsers.Options`](@ref).
- `rounding`: The rounding mode used for `FixedDecimal` and `DateTime` values in the CSV file. Defaults to `RoundNearest`. Used in [`Parsers.Options`](@ref).
- `validate_type_map`: Whether to validate the type map in the CSV file. Defaults to `true`.
- `comment`: The string or byte prefix used to indicate comments in the CSV file. Defaults to `nothing` which means no comment skipping will be performed. Used in [`ChunkedBase.ChunkingContext`](@ref).
### Chunking and parallelism
- `nworkers`: The number of worker threads to use for parsing the CSV file. Defaults to `max(1, Threads.nthreads() - 1)`. Used in [`ChunkedBase.ChunkingContext`](@ref).
- `buffersize`: The size of the buffer used for parsing the CSV file, in bytes. Defaults to `nworkers * 1024 * 1024`. Must be larger than any single row in input and smaller than 2GiB.
    If the input is larger than `buffersize` and if we're using `nworkers` > 1, a secondary buffer will be allocated internally to facilitate double-buffering. Used in [`ChunkedBase.ChunkingContext`](@ref).
### Misc
- `default_colname_prefix`: The default prefix to use for generated column names in the CSV file. Defaults to `"COL_"`.
- `use_mmap`: Whether to use memory-mapped I/O for parsing the CSV file when the `input` is a `String` path. Defaults to `false`.
- `no_quoted_newlines`: Assert that all newline characters in the file are record delimiters and never part of string field data. This allows the lexer to find newlines more efficiently. Defaults to `false`.
- `deduplicate_names`: Whether to deduplicate column names in the CSV file. Defaults to `true`.
- `_force`: Force parallel or serial parsing regardless of input size of `nworkers`. One of `:default`, `:serial` or `:parallel`. Defaults to `:default`, which won't parallelize small files or use the parallel code-path with `nworkers == 1`. Useful for debugging.
"""

const _SEE_ALSO = """
## See also
- [`TaskResultBuffer`](@ref), [`ParsedPayload`](@ref), [`AbstractConsumeContext`](@ref), [`consume!`](@ref)
- `ChunkedBase.jl` for more information about the `ChunkedBase` API.
"""


"""
    setup_parser(input, schema=nothing; kwargs...) -> (Bool, ParsingContext, ChunkingContext, Lexer)

For when you need to know the header and / or the schema which will be used to parse the file before creating your `consume_ctx`.

`setup_parser` will validate user input, ingest enough data chunks to reach the first valid row in the input, and then examine the first row to ensure we have a valid header and schema.
You can then inspect the returned `ParsingContext` to see what the header and schema will be, and then call `parse_file` with the other objects returned by `setup_parser`.

$_API_DOCS_SHARED

## Returns
- `should_close::Bool`: `true` if we opened an `IO` object and we should close it later
- `parsing_ctx::ChunkedCSV.ParsingContext`: Internal object, which contains the header, schema and settings needed for `Parsers.jl`
- `chunking_ctx::ChunkedBase.ChunkingContext`: Internal object, which holds the ingested data, newline positions and other things required by `ChunkedBase.jl` internally
- `lexer::NewlineLexers.Lexer`: Internal object, which is used to find newlines in the ingested chunks and which is also needed by `ChunkedBase.jl`

$_SEE_ALSO
- [`parse_file`](@ref)
"""
function setup_parser(
    input,
    schema::Union{Nothing,DataType,Base.Callable,Vector{DataType},Dict{Symbol,DataType},Dict{Int,DataType}}=nothing;
    header::Union{Vector{Symbol},Vector{String},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    # Parsers.Options
    delim::Union{UInt8,Char,String,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char,Nothing}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    ignorerepeated::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "TRUE", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "FALSE", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    quoted::Bool=true,
    decimal::Union{Char,UInt8}='.',
    ignoreemptyrows::Bool=true,
    rounding::Union{Nothing,RoundingMode}=RoundNearest,
    #
    validate_type_map::Bool=true,
    comment::Union{Nothing,AbstractString,Char,UInt8}=nothing,
    nworkers::Integer=max(1, Threads.nthreads() - 1),
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=(nworkers * 1024 * 1024),
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
    no_quoted_newlines::Bool=false,
    deduplicate_names::Bool=true,
)
    0 <= skipto <= typemax(Int) || throw(ArgumentError("`skipto` argument must be positive and smaller than 9_223_372_036_854_775_808."))
    (header isa Integer && !(0 <= header <= typemax(Int))) && throw(ArgumentError("`header` row number must be positive and smaller than 9_223_372_036_854_775_808."))
    (header isa Vector{String}) && (header = map(Symbol, header))
    if skipto == 0 && header isa Integer
        skipto = header + 1
    end
    (header isa Integer && skipto <= header) && throw(ArgumentError("non-zero `skipto` argument ($skipto) must come after `header` row ($header)"))

    validate_parser_args(;openquotechar, closequotechar, delim, escapechar, decimal, newlinechar, ignorerepeated)

    _validate(header, schema, validate_type_map)

    settings = InputArguments(
        header, Int(skipto), validate_type_map, default_colname_prefix,
        no_quoted_newlines, isnothing(newlinechar) ? newlinechar : UInt8(newlinechar),
        deduplicate_names,
    )

    chunking_ctx = ChunkingContext(buffersize, nworkers, limit, comment)
    should_close, io = ChunkedBase._input_to_io(input, use_mmap)
    try
        (lexer, lines_skipped_total) = initial_read_and_lex_and_skip!(io, chunking_ctx, settings, escapechar, openquotechar, closequotechar, ignoreemptyrows)
        # At this point we have skipped `header` rows and subsequent commented rows.
        # The next row should contain the header and should contain clean enough data to infer
        # the delimiter.
        if delim === nothing
            eols = chunking_ctx.newline_positions
            delim = _detect_delim(chunking_ctx.bytes, first(eols)+1, last(eols), openquotechar, closequotechar, escapechar, settings.header_at > 0)
        end
        options = _create_options(;
            delim, openquotechar, closequotechar, escapechar, sentinel, groupmark, stripwhitespace,
            truestrings, falsestrings, dateformat, ignorerepeated, quoted,
            decimal, ignoreemptyrows, rounding,
        )

        parsing_ctx = ParsingContext(DataType[], Enums.CSV_TYPE[], Symbol[], escapechar, options)
        process_header_and_schema_and_finish_row_skip!(parsing_ctx, chunking_ctx, lexer, schema, settings, lines_skipped_total)
        deduplicate_names && (parsing_ctx.header .= makeunique(parsing_ctx.header))
        return should_close, parsing_ctx, chunking_ctx, lexer
    catch
        should_close && close(io)
        rethrow()
    end
end

"""
parse_file(input, schema=nothing, consume_ctx::C; kwargs...) where {C<:AbstractConsumeContext} -> Nothing
parse_file(
    should_close::Bool,
    parsing_ctx::ParsingContext,
    consume_ctx::C,
    chunking_ctx::ChunkingContext,
    lexer::Lexer;
    _force::Symbol=:default,
) where {C<:AbstractConsumeContext} -> Nothing

Parse a CSV input by chunks of size `buffersize` and parse them in parallel using `nworkers` tasks.

Before calling this function, you should define a custom `consume_ctx::C` which is a subtype of `AbstractConsumeContext` and implement a `consume!(::C, ::ParsedPayload)` method.
Then, the `consume_ctx` is used to consume the parsed data, by internally dispatching on `consume!(::C, ::ParsedPayload)` which are also called in parallel.
The parsed results can be found in the `results` field `ParsedPayload`, see `TaskResultBuffer` for more information about the format in which the results are stored.

If you need to know the header and / or the schema which will be used to parse the file before creating your `consume_ctx`, you can call `setup_parser`
and inspect the returned `ParsingContext`, then call `parse_file` with the other objects returned by `setup_parser`.

$_API_DOCS_SHARED

## Returns
- `Nothing`

$_SEE_ALSO
- [`setup_parser`](@ref)
"""
function parse_file end

function parse_file(
    input,
    schema::Union{Nothing,DataType,Base.Callable,Vector{DataType},Dict{Symbol,DataType},Dict{Int,DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    header::Union{Vector{Symbol},Vector{String},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    # Parsers.Options
    delim::Union{UInt8,Char,String,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char,Nothing}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    ignorerepeated::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "TRUE", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "FALSE", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing}=nothing,
    quoted::Bool=true,
    decimal::Union{Char,UInt8}='.',
    ignoreemptyrows::Bool=true,
    rounding::Union{Nothing,RoundingMode}=RoundNearest,
    #
    comment::Union{Nothing,String,Char,UInt8}=nothing,
    validate_type_map::Bool=true,
    nworkers::Integer=max(1, Threads.nthreads() - 1),
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=(nworkers * 1024 * 1024),
    _force::Symbol=:default,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
    no_quoted_newlines::Bool=false,
    deduplicate_names::Bool=false,
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    (should_close, parsing_ctx, chunking_ctx, lexer) = setup_parser(
        input, schema;
        header, skipto, delim, openquotechar, closequotechar, limit, escapechar, newlinechar,
        sentinel, groupmark, stripwhitespace, truestrings, falsestrings, dateformat, comment,
        rounding, validate_type_map, default_colname_prefix, buffersize, nworkers,
        use_mmap, no_quoted_newlines, ignorerepeated, quoted, decimal, ignoreemptyrows,
        deduplicate_names,
    )
    try
        parse_file(lexer, parsing_ctx, consume_ctx, chunking_ctx, _force)
    finally
        should_close && close(lexer.io)
    end
    return nothing
end

function parse_file(
    lexer::Lexer,
    parsing_ctx::ParsingContext,
    consume_ctx::AbstractConsumeContext,
    chunking_ctx::ChunkingContext,
    _force::Symbol=:default,
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    schema = parsing_ctx.schema
    CT = _custom_types(schema)

    nrows = length(chunking_ctx.newline_positions) - 1
    if ChunkedBase.should_use_parallel(chunking_ctx, _force)
        ntasks = tasks_per_chunk(chunking_ctx)
        nbuffers = total_result_buffers_count(chunking_ctx)
        result_buffers = _make_result_buffers(nbuffers, schema, cld(nrows, ntasks))
        parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buffers, CT)
    else
        result_buf = TaskResultBuffer(0, parsing_ctx.schema, nrows)
        parse_file_serial(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buf, CT)
    end

    return nothing
end
