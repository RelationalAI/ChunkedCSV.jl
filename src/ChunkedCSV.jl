module ChunkedCSV

export parse_file, consume!, DebugContext, AbstractConsumeContext

using ScanByte
import Parsers
using .Threads: @spawn
using TranscodingStreams
using CodecZlib
using Dates
using FixedPointDecimals
using TimeZones
using Mmap
using SnoopPrecompile

# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using "reinterpretable bytes"
# IDEA: For result_buffers, and possibly elsewhere, use "PreallocatedChannels" that don't call popfirst! and push!, but getindex and setindex! + index

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

include("BufferedVectors.jl")
include("TaskResults.jl")

include("fixed_decimals_utils.jl")
include("datetime_utils.jl")

include("exceptions.jl")
# Temporary hack to register new DateTime
function __init__()
    Dates.CONVERSION_TRANSLATIONS[_GuessDateTime] = Dates.CONVERSION_TRANSLATIONS[Dates.DateTime]
    nothing
end

mutable struct TaskCondition
    ntasks::Int
    cond_wait::Threads.Condition
end
TaskCondition() = TaskCondition(0, Threads.Condition(ReentrantLock()))

struct ParsingContext
    schema::Vector{DataType}
    header::Vector{Symbol}
    bytes::Vector{UInt8}
    eols::BufferedVector{Int32}
    limit::Int
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
    escapechar::UInt8
    cond::TaskCondition
    comment::Union{Nothing,Vector{UInt8}}
end
function estimate_task_size(parsing_ctx::ParsingContext)
    length(parsing_ctx.eols) == 1 && return 1 # empty file
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, ceil(Int, last(parsing_ctx.eols)  / length(parsing_ctx.eols))))
    return max(min_rows, cld(ceil(Int, length(parsing_ctx.eols) * ((1 + length(parsing_ctx.bytes)) / (1 + last(parsing_ctx.eols)))), parsing_ctx.maxtasks))
end
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
    maxtasks::UInt8
    nresults::UInt8
    comment::Union{Nothing,Vector{UInt8}}
end
function ParserSettings(schema, header::Vector{Symbol}, data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults, comment)
    ParserSettings(schema, header, 0, Int(data_at), Int(limit), validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults, comment)
end

function ParserSettings(schema, header::Integer, data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults, comment)
    ParserSettings(schema, nothing, Int(header), Int(data_at), Int(limit), validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults, comment)
end

function limit_eols!(parsing_ctx::ParsingContext, row_num)
    parsing_ctx.limit == 0 && return false
    if row_num > parsing_ctx.limit
        return true
    elseif row_num <= parsing_ctx.limit < row_num + length(parsing_ctx.eols) - 1
        parsing_ctx.eols.occupied -= (row_num + length(parsing_ctx.eols) - 1) - parsing_ctx.limit - 1
    end
    return false
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
include("consume_context.jl")

include("row_parsing.jl")
include("parser_serial.jl")
include("parser_singlebuffer.jl")
include("parser_doublebuffer.jl")

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
        throw(ArgumentError("`delim`, `escapechar`, `openquotechar`, `closequotechar` and `newlinechar` must not be a `0xff` byte."))
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
    buffersize::Integer=(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2nworkers,
    nresults::Integer=maxtasks,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
)
    0 < buffersize <= typemax(Int32) || throw(ArgumentError("`buffersize` argument must be larger than 0 and smaller than 2_147_483_648 bytes."))
    0 < nworkers < 256 || throw(ArgumentError("`nworkers` argument must be larger than 0 and smaller than 256."))
    0 < nresults < 256 || throw(ArgumentError("`nresults` argument must be larger than 0 and smaller than 256."))
    0 < maxtasks < 256 || throw(ArgumentError("`maxtasks` argument must be larger than 0 and smaller than 256."))
    0 <= skipto <= typemax(Int) || throw(ArgumentError("`skipto` argument must be positive and smaller than 9_223_372_036_854_775_808 rows."))
    0 <= limit <= typemax(Int) || throw(ArgumentError("`limit` argument must be positive and smaller than 9_223_372_036_854_775_808 rows."))

    sizeof(openquotechar) > 1 || throw(ArgumentError("`openquotechar` must be a single-byte character."))
    sizeof(closequotechar) > 1 || throw(ArgumentError("`closequotechar` must be a single-byte character."))
    sizeof(delim) > 1 || throw(ArgumentError("`delim` must be a single-byte character."))
    sizeof(escapechar) > 1 || throw(ArgumentError("`escapechar` must be a single-byte character."))
    sizeof(newlinechar) > 1 || throw(ArgumentError("`newlinechar` must be a single-byte character."))

    maxtasks >= nworkers || throw(ArgumentError("`maxtasks` argument must be larger of equal to the `nworkers` argument."))
    # otherwise not implemented; implementation postponed once we know why take!/wait allocates
    nresults == maxtasks || throw(ArgumentError("Currently, `nresults` argument must be equal to the `maxtasks` argument."))
    _validate(header, schema, validate_type_map)

    should_close, io = _input_to_io(input, use_mmap)
    settings = ParserSettings(
        schema, header, Int(skipto), Int(limit), validate_type_map, default_colname_prefix,
        Int32(buffersize), UInt8(nworkers), UInt8(maxtasks), UInt8(nresults), _comment_to_bytes(comment),
    )
    options = _create_options(;
        delim, openquotechar, closequotechar, escapechar, sentinel, groupmark, stripwhitespace,
        truestrings, falsestrings, newlinechar
    )
    byteset = Val(ByteSet((UInt8(options.e), UInt8(options.oq.token),  UInt8(options.cq.token), UInt8(newlinechar))))
    (parsing_ctx, lexer_state) = init_parsing!(io, settings, options, Val(byteset))
    return should_close, parsing_ctx, lexer_state, options
end

function parse_file(
    lexer_state::LexerState,
    parsing_ctx::ParsingContext,
    consume_ctx::AbstractConsumeContext,
    options::Parsers.Options,
    _force::Symbol=:none,
)
    _force in (:none, :serial, :singlebuffer, :doublebuffer) || throw(ArgumentError("`_force` argument must be one of (:none, :serial, :singlebuffer, :doublebuffer)."))
    schema = parsing_ctx.schema
    M = _bounding_flag_type(length(schema))
    if _force === :doublebuffer
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(M))
    elseif _force === :singlebuffer
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(M))
    elseif _force === :serial || Threads.nthreads() == 1 || parsing_ctx.nworkers == 1 || parsing_ctx.maxtasks == 1 || lexer_state.last_newline_at < MIN_TASK_SIZE_IN_BYTES
              _parse_file_serial(lexer_state, parsing_ctx, consume_ctx, options, Val(M))
    elseif !lexer_state.done
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(M))
    else
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(M))
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
    buffersize::Integer=Int32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2nworkers,
    nresults::Integer=maxtasks,
    _force::Symbol=:none,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
)
    (should_close, parsing_ctx, lexer_state, options) = setup_parser(
        input, schema;
        header, skipto, delim, openquotechar, closequotechar, limit, escapechar, newlinechar,
        sentinel, groupmark, stripwhitespace, truestrings, falsestrings, comment,
        validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults,
        use_mmap,
    )
    parse_file(lexer_state, parsing_ctx, consume_ctx, options, _force)
    should_close && close(lexer_state.io)
    return nothing
end

# NOTE: Disabled for now to try and work around PackageCompiler issues
# include("precompile.jl")

end # module
