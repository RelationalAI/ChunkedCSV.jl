module ChunkedCSV

export parse_file, consume!, DebugContext, AbstractConsumeContext

import Parsers
using .Threads: @spawn
using CodecZlibNG
using Dates
using FixedPointDecimals
using TimeZones
using Mmap
using SnoopPrecompile

# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using "reinterpretable bytes"
# IDEA: For result_buffers, and possibly elsewhere, use "PreallocatedChannels" that don't call popfirst! and push!, but getindex and setindex! + index

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

include("Enums.jl")
include("BufferedVectors.jl")
using .BufferedVectors
include("TaskResults.jl")

include("type_parsers/fixed_decimals_parser.jl")
include("type_parsers/datetime_parser.jl")

include("newline_lexer.jl")
using .NewlineLexers
include("exceptions.jl")
# Temporary hack to register new DateTime
function __init__()
    Dates.CONVERSION_TRANSLATIONS[_GuessDateTime] = Dates.CONVERSION_TRANSLATIONS[Dates.DateTime]
    return nothing
end

const PARSER_TASKS_TIMES = [UInt[]]
const IO_TASK_TIMES = UInt[]
const LEXER_TASK_TIMES = UInt[]
const T1 = UInt[]
const T2 = UInt[]
get_parser_task_trace(i) = PARSER_TASKS_TIMES[i]
function clear_traces!()
    for _ in length(PARSER_TASKS_TIMES)+1:Threads.nthreads()
        push!(PARSER_TASKS_TIMES, UInt[])
    end
    empty!(ChunkedCSV.IO_TASK_TIMES)
    empty!(ChunkedCSV.LEXER_TASK_TIMES)
    empty!(ChunkedCSV.T1)
    empty!(ChunkedCSV.T2)
    foreach(empty!, ChunkedCSV.PARSER_TASKS_TIMES)
    return nothing
end

mutable struct TaskCondition
    ntasks::Int
    cond_wait::Threads.Condition
    exception::Union{Nothing,Exception}
end
TaskCondition() = TaskCondition(0, Threads.Condition(ReentrantLock()), nothing)
Base.lock(t::TaskCondition) = lock(t.cond_wait)
Base.unlock(t::TaskCondition) = unlock(t.cond_wait)

struct ParsingContext
    id::Int
    schema::Vector{DataType}
    enum_schema::Vector{Enums.CSV_TYPE}
    header::Vector{Symbol}
    bytes::Vector{UInt8}
    eols::BufferedVector{Int32}
    limit::Int
    nworkers::UInt8
    escapechar::UInt8
    cond::TaskCondition
    comment::Union{Nothing,Vector{UInt8}}
end
function estimate_task_size(parsing_ctx::ParsingContext)
    length(parsing_ctx.eols) == 1 && return 1 # empty file
    bytes_to_parse = last(parsing_ctx.eols)
    rows = length(parsing_ctx.eols) # actually rows + 1
    buffersize = length(parsing_ctx.bytes)
    # There are 2*nworkers result buffers total, but there are nworkers tasks per chunk
    prorated_maxtasks = ceil(Int, tasks_per_chunk(parsing_ctx) * (bytes_to_parse / buffersize))
    # Lower bound is 2 because length(eols) == 2 => 1 row
    # bump min rows if average row is much smaller than MIN_TASK_SIZE_IN_BYTES
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, cld(bytes_to_parse, rows)))
    return min(max(min_rows, cld(rows, prorated_maxtasks)), rows)
end
total_result_buffers_count(parsing_ctx::ParsingContext) = 2parsing_ctx.nworkers
tasks_per_chunk(parsing_ctx::ParsingContext) = parsing_ctx.nworkers
struct ParserSettings
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}
    header::Union{Nothing,Vector{Symbol}}
    header_at::Int
    data_at::Int
    limit::Int
    validate_type_map::Bool
    default_colname_prefix::String
    buffersize::Int32
    nworkers::UInt8
    comment::Union{Nothing,Vector{UInt8}}
    no_quoted_newlines::Bool
    newlinechar::UInt8
    function ParserSettings(schema, header::Vector{Symbol}, data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, comment, no_quoted_newlines, newlinechar)
        new(schema, header,  0,           Int(data_at), Int(limit), validate_type_map, default_colname_prefix, buffersize, nworkers, comment, no_quoted_newlines, newlinechar)
    end
    
    function ParserSettings(schema, header::Integer,        data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, comment, no_quoted_newlines, newlinechar)
        new(schema, nothing, Int(header), Int(data_at), Int(limit), validate_type_map, default_colname_prefix, buffersize, nworkers, comment, no_quoted_newlines, newlinechar)
    end
end

_is_supported_type(::Type{T}) where {T} = false
_is_supported_type(::Type{T}) where {T<:Union{Int,Float64,String,Char,Bool,DateTime,Date}} = true
_is_supported_type(::Type{FixedDecimal{T,f}}) where {T<:Union{Int8,Int16,Int32,Int64,Int128,UInt8,UInt16,UInt32,UInt64,UInt128},f} = f <= 8
function validate_schema(types::Vector{DataType})
    unsupported_types = filter(!_is_supported_type, types)
    if !isempty(unsupported_types)
        err_msg = "Provided schema contains unsupported types: $(join(unique!(unsupported_types), ", "))."
        any(T->(T <: FixedDecimal), unsupported_types) && (err_msg *= " Note: Currently, only decimals with less than 9 decimal places are supported.")
        throw(ArgumentError(err_msg))
    end
    return nothing
end

include("read_and_lex.jl")
include("init_parsing.jl")
include("ConsumeContexts.jl")
using .ConsumeContexts

include("row_parsing.jl")
include("parser_serial.jl")
include("parser_parallel.jl")

function _create_options(;
    delim::Union{UInt8,Char}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    newlinechar::Union{UInt8,Char}='\n', # only for validation
)
    (0xff in (UInt8(openquotechar), UInt8(closequotechar), UInt8(escapechar), UInt8(delim), UInt8(newlinechar))) &&
        throw(ArgumentError("`delim`, `escapechar`, `openquotechar`, `closequotechar` and `newlinechar` must not be `0xff`."))
    (UInt8(newlinechar) in (UInt8(openquotechar), UInt8(closequotechar), UInt8(escapechar), UInt8(delim))) &&
        throw(ArgumentError("`newlinechar` must be different from `delim`, `escapechar`, `openquotechar` and `closequotechar`"))
    return Parsers.Options(
        sentinel=sentinel,
        wh1=delim ==  ' ' ? '\v' : ' ',
        wh2=delim == '\t' ? '\v' : '\t',
        openquotechar=UInt8(openquotechar),
        closequotechar=UInt8(closequotechar),
        escapechar=UInt8(escapechar),
        delim=UInt8(delim),
        quoted=true,
        ignoreemptylines=true,
        stripwhitespace=stripwhitespace,
        trues=truestrings,
        falses=falsestrings,
        groupmark=groupmark,
    )
end

_validate(header::Vector, schema::Vector, validate_type_map) = length(header) == length(schema) || throw(ArgumentError("Provided header and schema lengths don't match. Header has $(length(header)) columns, schema has $(length(schema))."))
_validate(header::Vector, schema::Dict, validate_type_map) = !validate_type_map || issubset(keys(schema), header) || throw(ArgumentError("Provided header and schema names don't match. In schema, not in header: $(setdiff(keys(schema), header))). In header, not in schema: $(setdiff(header, keys(schema)))"))
_validate(header, schema, validate_type_map) = true

_comment_to_bytes(x::AbstractString) = Vector{UInt8}(x)
_comment_to_bytes(x::Char) = _comment_to_bytes(ncodeunits(x) > 1 ? string(x) : UInt8(x))
_comment_to_bytes(x::UInt8) = [x]
_comment_to_bytes(x::Nothing) = nothing

function setup_parser(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing;
    header::Union{Vector{Symbol},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    delim::Union{UInt8,Char}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    validate_type_map::Bool=true,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    comment::Union{Nothing,AbstractString,Char,UInt8}=nothing,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    nworkers::Integer=max(1, Threads.nthreads() - 1),
    buffersize::Integer=(nworkers * 1024 * 1024),
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
    no_quoted_newlines::Bool=false,
)
    4 <= buffersize <= typemax(Int32) || throw(ArgumentError("`buffersize` argument must be larger than 4 and smaller than 2_147_483_648 bytes."))
    0 < nworkers < 256 || throw(ArgumentError("`nworkers` argument must be larger than 0 and smaller than 256."))
    0 <= skipto <= typemax(Int) || throw(ArgumentError("`skipto` argument must be positive and smaller than 9_223_372_036_854_775_808."))
    (header isa Integer && !(0 <= header <= typemax(Int))) && throw(ArgumentError("`header` row number must be positive and smaller than 9_223_372_036_854_775_808."))
    0 <= limit <= typemax(Int) || throw(ArgumentError("`limit` argument must be positive and smaller than 9_223_372_036_854_775_808."))

    if skipto == 0 && header isa Integer
        skipto = header + 1
    end
    (header isa Integer && skipto < header) && throw(ArgumentError("`skipto` argument ($skipto) must come after `header` row ($header)"))

    sizeof(openquotechar) > 1 || throw(ArgumentError("`openquotechar` must be a single-byte character."))
    sizeof(closequotechar) > 1 || throw(ArgumentError("`closequotechar` must be a single-byte character."))
    sizeof(delim) > 1 || throw(ArgumentError("`delim` must be a single-byte character."))
    sizeof(escapechar) > 1 || throw(ArgumentError("`escapechar` must be a single-byte character."))
    sizeof(newlinechar) > 1 || throw(ArgumentError("`newlinechar` must be a single-byte character."))

    _validate(header, schema, validate_type_map)

    should_close, io = _input_to_io(input, use_mmap)
    settings = ParserSettings(
        schema, header, Int(skipto), Int(limit), validate_type_map, default_colname_prefix,
        Int32(buffersize), UInt8(nworkers), _comment_to_bytes(comment), no_quoted_newlines, 
        UInt8(newlinechar),
    )
    options = _create_options(;
        delim, openquotechar, closequotechar, escapechar, sentinel, groupmark, stripwhitespace,
        truestrings, falsestrings, newlinechar
    )
    (parsing_ctx, lexer) = init_parsing!(io, settings, options)
    return should_close, parsing_ctx, lexer, options
end

function parse_file(
    lexer::Lexer,
    parsing_ctx::ParsingContext,
    consume_ctx::AbstractConsumeContext,
    options::Parsers.Options,
    _force::Symbol=:default,
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    schema = parsing_ctx.schema
    M = _bounding_flag_type(length(schema))
    if _force === :parallel
        _parse_file_parallel(lexer, parsing_ctx, consume_ctx, options, Val(M))
    elseif _force === :serial || Threads.nthreads() == 1 || parsing_ctx.nworkers == 1 || last(parsing_ctx.eols) < MIN_TASK_SIZE_IN_BYTES
              _parse_file_serial(lexer, parsing_ctx, consume_ctx, options, Val(M))
    else
        _parse_file_parallel(lexer, parsing_ctx, consume_ctx, options, Val(M))
    end
    return nothing
end

function parse_file(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    header::Union{Nothing,Vector{Symbol},Integer}=true,
    skipto::Integer=0,
    limit::Integer=0,
    delim::Union{UInt8,Char}=',',
    openquotechar::Union{UInt8,Char}='"',
    closequotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    newlinechar::Union{UInt8,Char}='\n',
    sentinel::Union{Missing,Nothing,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    validate_type_map::Bool=true,
    truestrings::Union{Nothing,Vector{String}}=["true", "True", "1", "t", "T"],
    falsestrings::Union{Nothing,Vector{String}}=["false", "False", "0", "f", "F"],
    comment::Union{Nothing,String,Char,UInt8}=nothing,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    nworkers::Integer=max(1, Threads.nthreads() - 1),
    buffersize::Integer=(nworkers * 1024 * 1024),
    _force::Symbol=:default,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
    no_quoted_newlines::Bool=false,
)
    _force in (:default, :serial, :parallel) || throw(ArgumentError("`_force` argument must be one of (:default, :serial, :parallel)."))
    clear_traces!()
    (should_close, parsing_ctx, lexer, options) = setup_parser(
        input, schema;
        header, skipto, delim, openquotechar, closequotechar, limit, escapechar, newlinechar,
        sentinel, groupmark, stripwhitespace, truestrings, falsestrings, comment,
        validate_type_map, default_colname_prefix, buffersize, nworkers,
        use_mmap, no_quoted_newlines,
    )
    parse_file(lexer, parsing_ctx, consume_ctx, options, _force)
    should_close && close(lexer.io)
    return nothing
end

# NOTE: Disabled for now to try and work around PackageCompiler issues
include("precompile.jl")

end # module
