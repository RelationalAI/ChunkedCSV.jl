module ChunkedCSV

export setup_parser, parse_file, DebugContext, AbstractConsumeContext
export consume!, setup_tasks!, task_done!

import Parsers
using Dates
using FixedPointDecimals
using TimeZones
using SnoopPrecompile
using ChunkedBase
using SentinelArrays.BufferedVectors

include("type_parsers/datetime_parser.jl")
include("Enums.jl")

# Temporary hack to register new DateTime
function __init__()
    Dates.CONVERSION_TRANSLATIONS[GuessDateTime] = Dates.CONVERSION_TRANSLATIONS[Dates.DateTime]
    return nothing
end

# What we need to forward to ChunkedBase.populate_result_buffer!
struct ParsingContext <: AbstractParsingContext
    schema::Vector{DataType}
    enum_schema::Vector{Enums.CSV_TYPE}
    header::Vector{Symbol}
    escapechar::UInt8
    options::Parsers.Options
end

# Hold most inputs during the initialization stage where we verify them and and set things up for ChunkedBase.
struct ParserSettings
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}
    header::Union{Nothing,Vector{Symbol}}
    header_at::Int
    data_at::Int
    validate_type_map::Bool
    default_colname_prefix::String
    no_quoted_newlines::Bool
    newlinechar::Union{Nothing,UInt8}
    function ParserSettings(schema, header::Vector{Symbol}, data_at, validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar)
        new(schema, header,  0,           Int(data_at), validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar)
    end

    function ParserSettings(schema, header::Integer,        data_at, validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar)
        new(schema, nothing, Int(header), Int(data_at), validate_type_map, default_colname_prefix, no_quoted_newlines, newlinechar)
    end
end

_is_supported_type(::Type{T}) where {T} = Parsers.supportedtype(T)
_is_supported_type(::Type{Nothing}) = true
_is_supported_type(::Type{GuessDateTime}) = true
function _is_supported_type(::Type{FixedDecimal{T,f}}) where {T,f}
    # https://github.com/JuliaMath/FixedPointDecimals.jl/blob/1328b9a372d2285765a7255f154f09ffdd692508/src/FixedPointDecimals.jl#L83-L91
    n = FixedPointDecimals.max_exp10(T)
    return f >= 0 && (n < 0 || f <= n)
end
function validate_schema(types::Vector{DataType})
    unsupported_types = unique!(filter(!_is_supported_type, types))
    if !isempty(unsupported_types)
        err_msg = "Provided schema contains unsupported types: $(join(unsupported_types, ", "))."
        throw(ArgumentError(err_msg))
    end
    return nothing
end

# Separate out the types that are not pre-compiled by the parser by default
# and return them as a single Tuple of unique types which can be passed to
# _parse_rows_forloop! to trigger recompilation.
function _custom_types(schema::Vector{DataType})
    # We sort the unique types to always produce the same Tuple for the same
    # schema. But maybe the default ordering from the IdDict is good enough?
    custom_types = sort!(collect(keys(
            IdDict{Type,Nothing}(
                T => nothing for T in schema if isnothing(get(Enums._MAPPING, T, nothing))
            ))),
        by=objectid
    )
    return Tuple{custom_types...}
end

include("result_buffer.jl")
include("detect.jl")
include("init_parsing.jl")
include("row_parsing.jl")
include("consume_contexts.jl")

function _create_options(;
    delim::Union{UInt8,Char,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
    ignorerepeated::Bool=false,
    quoted::Bool=true,
    decimal::Union{Char,UInt8}='.',
    ignoreemptyrows::Bool=true,
    rounding::Union{Nothing,RoundingMode}=RoundNearest,
)
    return Parsers.Options(
        sentinel=sentinel,
        wh1=delim ==  ' ' ? '\v' : ' ',
        wh2=delim == '\t' ? '\v' : '\t',
        openquotechar=UInt8(openquotechar),
        closequotechar=UInt8(closequotechar),
        escapechar=UInt8(escapechar),
        delim=UInt8(delim),
        quoted=quoted,
        stripwhitespace=stripwhitespace,
        trues=truestrings,
        falses=falsestrings,
        groupmark=groupmark,
        dateformat=dateformat,
        ignorerepeated=ignorerepeated,
        decimal=UInt8(decimal),
        ignoreemptylines=ignoreemptyrows,
        rounding=rounding,
    )
end

_validate(header::Vector, schema::Vector, validate_type_map) = length(header) == length(schema) || throw(ArgumentError("Provided header and schema lengths don't match. Header has $(length(header)) columns, schema has $(length(schema))."))
_validate(header::Vector, schema::Dict, validate_type_map) = !validate_type_map || issubset(keys(schema), header) || throw(ArgumentError("Provided header and schema names don't match. In schema, not in header: $(setdiff(keys(schema), header))). In header, not in schema: $(setdiff(header, keys(schema)))"))
_validate(header, schema, validate_type_map) = true

_nbytes(::UInt8) = 1
_nbytes(x::Char) = ncodeunits(x)

function validate_parser_args(;openquotechar, closequotechar, delim, escapechar, decimal, newlinechar, ignorerepeated)
    _nbytes(openquotechar) == 1 || throw(ArgumentError("`openquotechar` must be a single-byte character"))
    _nbytes(closequotechar) == 1 || throw(ArgumentError("`closequotechar` must be a single-byte character"))
    if isnothing(delim)
        ignorerepeated && throw(ArgumentError("auto-delimiter detection not supported when `ignorerepeated=true`; please provide delimiter like `delim=','`"))
    else
        _nbytes(delim) == 1 || throw(ArgumentError("`delim` must be a single-byte character"))
    end
    _nbytes(escapechar) == 1 || throw(ArgumentError("`escapechar` must be a single-byte character"))
    _nbytes(decimal) == 1 || throw(ArgumentError("`decimal` must be a single-byte character"))

    if !isnothing(newlinechar)
        _nbytes(newlinechar) > 1 && throw(ArgumentError("`newlinechar` must be a single-byte character."))
        ((newlinechar % UInt8) in ((openquotechar % UInt8), (closequotechar % UInt8), (escapechar % UInt8), (something(delim, 0x00) % UInt8))) &&
            throw(ArgumentError("`newlinechar` must be different from `delim`, `openquotechar`, `closequotechar`, and `escapechar`"))
    end

    return nothing
end

function setup_parser(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing;
    header::Union{Vector{Symbol},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    # Parsers.Options
    delim::Union{UInt8,Char,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char,Nothing}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    ignorerepeated::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
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
)
    0 <= skipto <= typemax(Int) || throw(ArgumentError("`skipto` argument must be positive and smaller than 9_223_372_036_854_775_808."))
    (header isa Integer && !(0 <= header <= typemax(Int))) && throw(ArgumentError("`header` row number must be positive and smaller than 9_223_372_036_854_775_808."))

    if skipto == 0 && header isa Integer
        skipto = header + 1
    end
    (header isa Integer && skipto < header) && throw(ArgumentError("`skipto` argument ($skipto) must come after `header` row ($header)"))

    validate_parser_args(;openquotechar, closequotechar, delim, escapechar, decimal, newlinechar, ignorerepeated)

    _validate(header, schema, validate_type_map)

    settings = ParserSettings(
        schema, header, Int(skipto), validate_type_map, default_colname_prefix,
        no_quoted_newlines, isnothing(newlinechar) ? newlinechar : UInt8(newlinechar),
    )
    # TRACING #  clear_traces!()

    chunking_ctx = ChunkingContext(buffersize, nworkers, limit, comment)
    should_close, io = ChunkedBase._input_to_io(input, use_mmap)
    try
        (lexer, lines_skipped_total) = initial_read_and_lex_and_skip!(io, chunking_ctx, settings, escapechar, openquotechar, closequotechar)
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
        process_header_and_schema_and_finish_row_skip!(parsing_ctx, chunking_ctx, lexer, settings, lines_skipped_total)
        return should_close, parsing_ctx, chunking_ctx, lexer
    catch
        should_close && close(io)
        rethrow()
    end
end

function parse_file(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    header::Union{Nothing,Vector{Symbol},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    # Parsers.Options
    delim::Union{UInt8,Char,Nothing}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char,Nothing}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    ignorerepeated::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    dateformat::Union{String, Dates.DateFormat, Nothing, AbstractDict}=nothing,
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
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    (should_close, parsing_ctx, chunking_ctx, lexer) = setup_parser(
        input, schema;
        header, skipto, delim, openquotechar, closequotechar, limit, escapechar, newlinechar,
        sentinel, groupmark, stripwhitespace, truestrings, falsestrings, dateformat, comment,
        rounding, validate_type_map, default_colname_prefix, buffersize, nworkers,
        use_mmap, no_quoted_newlines, ignorerepeated, quoted, decimal, ignoreemptyrows
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
        result_buffers = TaskResultBuffer[TaskResultBuffer(id, parsing_ctx.schema, cld(nrows, ntasks)) for id in 1:nbuffers]
        parse_file_parallel(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buffers, CT)
    else
        result_buf = TaskResultBuffer(0, parsing_ctx.schema, nrows)
        parse_file_serial(lexer, parsing_ctx, consume_ctx, chunking_ctx, result_buf, CT)
    end

    return nothing
end

include("precompile.jl")

end # module
