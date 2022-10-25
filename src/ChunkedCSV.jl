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

# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using "reinterpretable bytes"
# IDEA: For result_buffers, and possibly elsewhere, use "PreallocatedChannels" that don't call popfirst! and push!, but getindex and setindex! + index

const MIN_TASK_SIZE_IN_BYTES = 16 * 1024

include("BufferedVectors.jl")
include("TaskResults.jl")

include("fixed_decimals_utils.jl")
include("datetime_utils.jl")

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
    eols::BufferedVector{UInt32}
    limit::UInt32
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
    escapechar::UInt8
    cond::TaskCondition
end
function estimate_task_size(parsing_ctx::ParsingContext)
    length(parsing_ctx.eols) == 1 && return 1 # empty file
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, ceil(Int, last(parsing_ctx.eols)  / length(parsing_ctx.eols))))
    return max(min_rows, cld(ceil(Int, length(parsing_ctx.eols) * ((1 + length(parsing_ctx.bytes)) / (1 + last(parsing_ctx.eols)))), parsing_ctx.maxtasks))
end
struct ParserSettings
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}
    header::Union{Nothing,Vector{Symbol}}
    header_at::UInt
    data_at::UInt
    limit::UInt32
    validate_type_map::Bool
    default_colname_prefix::String
    buffersize::UInt32
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
end
function ParserSettings(schema, header::Vector{Symbol}, data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults)
    ParserSettings(schema, header, UInt(0), UInt(data_at), limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults)
end

function ParserSettings(schema, header::Integer, data_at, limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults)
    ParserSettings(schema, nothing, UInt(header), UInt(data_at), limit, validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults)
end

function limit_eols!(parsing_ctx::ParsingContext, row_num)
    parsing_ctx.limit == 0 && return false
    if row_num > parsing_ctx.limit
        return true
    elseif row_num <= parsing_ctx.limit < row_num + UInt32(length(parsing_ctx.eols) - 1)
        parsing_ctx.eols.occupied -= (row_num + UInt32(length(parsing_ctx.eols) - 1) - parsing_ctx.limit - UInt32(1))
    end
    return false
end

include("init_parsing.jl")
include("read_and_lex.jl")
include("consume_context.jl")

include("row_parsing.jl")
include("parser_serial.jl")
include("parser_singlebuffer.jl")
include("parser_doublebuffer.jl")

function _create_options(delim::Char=',', quotechar::Char='"', escapechar::Char='"', sentinel::Union{Missing,String,Vector{String}}=missing, groupmark::Union{Char,UInt8,Nothing}=nothing, stripwhitespace::Bool=false)
    (UInt8(quotechar) == 0xff || UInt8(escapechar) == 0xff) && error("`escapechar` and/or `quotechar` must not be a `0xff` byte.")
    return Parsers.Options(
        sentinel=sentinel,
        wh1=delim ==  ' ' ? '\v' : ' ',
        wh2=delim == '\t' ? '\v' : '\t',
        openquotechar=UInt8(quotechar),
        closequotechar=UInt8(quotechar),
        escapechar=UInt8(escapechar),
        delim=UInt8(delim),
        quoted=true,
        ignoreemptylines=true,
        stripwhitespace=stripwhitespace,
        trues=["true", "True", "1", "t", "T"],
        falses=["false", "False", "0", "f", "F"],
        groupmark=groupmark,
    )
end

_validate(header::Vector, schema::Vector, validate_type_map) = length(header) == length(schema) || error("Provided header and schema lengths don't match. Header has $(length(header)) columns, schema has $(length(schema)).")
_validate(header::Vector, schema::Dict, validate_type_map) = !validate_type_map || issubset(keys(schema), header) || error("Provided header and schema names don't match. In schema, not in header: $(setdiff(keys(schema), header))). In header, not in schema: $(setdiff(header, keys(schema)))")
_validate(header, schema, validate_type_map) = true

function setup_parser(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing;
    header::Union{Vector{Symbol},Integer}=true,
    skipto::Integer=UInt32(0),
    limit::Integer=UInt32(0),
    delim::Union{UInt8,Char}=',',
    quotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,String,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    validate_type_map::Bool=true,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    nresults::Integer=maxtasks,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
)
    @assert 0 < buffersize < typemax(UInt32)
    @assert skipto >= 0
    @assert limit >= 0
    @assert nworkers > 0
    @assert maxtasks >= nworkers
    @assert nresults == maxtasks # otherwise not implemented; implementation postponed once we know why take!/wait allocates
    _validate(header, schema, validate_type_map)

    should_close, io = _input_to_io(input, use_mmap)
    settings = ParserSettings(schema, header, UInt(skipto), UInt32(limit), validate_type_map, default_colname_prefix, UInt32(buffersize), UInt8(nworkers), UInt8(maxtasks), UInt8(nresults))
    options = _create_options(delim, quotechar, escapechar, sentinel, groupmark, stripwhitespace)
    byteset = Val(ByteSet((UInt8(options.e), UInt8(options.oq), UInt8('\n'), UInt8('\r'))))
    (parsing_ctx, lexer_state) = init_parsing!(io, settings, options, Val(byteset))
    options = _create_options(delim, quotechar, escapechar, sentinel, groupmark, stripwhitespace)
    return should_close, parsing_ctx, lexer_state, options
end

function parse_file(
    lexer_state::LexerState,
    parsing_ctx::ParsingContext,
    consume_ctx::AbstractConsumeContext,
    options::Parsers.Options,
    _force::Symbol=:none,
)
    @assert _force in (:none, :serial, :singlebuffer, :doublebuffer)
    schema = parsing_ctx.schema
    N = length(schema)
    M = _bounding_flag_type(N)
    if _force === :doublebuffer
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(N), Val(M))::Nothing
    elseif _force === :singlebuffer
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(N), Val(M))::Nothing
    elseif _force === :serial || Threads.nthreads() == 1 || parsing_ctx.nworkers == 1 || parsing_ctx.maxtasks == 1 || lexer_state.last_newline_at < MIN_TASK_SIZE_IN_BYTES
              _parse_file_serial(lexer_state, parsing_ctx, consume_ctx, options, Val(N), Val(M))::Nothing
    elseif !lexer_state.done
        _parse_file_doublebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(N), Val(M))::Nothing
    else
        _parse_file_singlebuffer(lexer_state, parsing_ctx, consume_ctx, options, Val(N), Val(M))::Nothing
    end
end

function parse_file(
    input,
    schema::Union{Nothing,Vector{DataType},Dict{Symbol,DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    header::Union{Nothing,Vector{Symbol},Integer}=true,
    skipto::Integer=UInt32(0),
    limit::Integer=UInt32(0),
    delim::Union{UInt8,Char}=',',
    quotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,String,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    validate_type_map::Bool=true,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    nresults::Integer=maxtasks,
    _force::Symbol=:none,
    default_colname_prefix::String="COL_",
    use_mmap::Bool=false,
)
    (should_close, parsing_ctx, lexer_state, options) = setup_parser(
        input, schema; 
        header, skipto, delim, quotechar, limit, escapechar, sentinel, groupmark, stripwhitespace, 
        validate_type_map, default_colname_prefix, buffersize, nworkers, maxtasks, nresults, use_mmap
    )
    parse_file(lexer_state, parsing_ctx, consume_ctx, options, _force)
    should_close && close(lexer_state.io)
    return nothing
end

end # module