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
    cond::TaskCondition
end
function estimate_task_size(parsing_ctx::ParsingContext)
    min_rows = max(2, cld(MIN_TASK_SIZE_IN_BYTES, ceil(Int, last(parsing_ctx.eols)  / length(parsing_ctx.eols))))
    return max(min_rows, cld(ceil(Int, length(parsing_ctx.eols) * ((1 + length(parsing_ctx.bytes)) / (1 + last(parsing_ctx.eols)))), parsing_ctx.maxtasks))
end
struct ParserSettings
    schema::Union{Nothing,Vector{DataType}}
    header::Union{Nothing,Vector{Symbol}}
    hasheader::Bool
    skiprows::Int
    limit::UInt32
    buffersize::UInt32
    nworkers::UInt8
    maxtasks::UInt8
    nresults::UInt8
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
        trues=["true", "1", "True", "t"],
        falses=["false", "0", "False", "f"],
        groupmark=groupmark,
    )
end

function setup_parser(
    input,
    schema::Union{Nothing,Vector{DataType}}=nothing;
    header::Union{Nothing,Vector{Symbol}}=nothing,
    hasheader::Bool=true,
    skiprows::Integer=UInt32(0),
    limit::Integer=UInt32(0),
    delim::Union{UInt8,Char}=',',
    quotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,String,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    nresults::Integer=maxtasks,
    use_mmap::Bool=false,
)
    @assert 0 < buffersize < typemax(UInt32)
    @assert skiprows >= 0
    @assert limit >= 0
    @assert nworkers > 0
    @assert maxtasks >= nworkers
    @assert nresults == maxtasks # otherwise not implemented; implementation postponed once we know why take!/wait allocates
    !isnothing(header) && !isnothing(schema) && length(header) != length(schema) && error("Provided header doesn't match the number of column of schema ($(length(header)) names, $(length(schema)) types).")

    should_close, io = _input_to_io(input, use_mmap)
    settings = ParserSettings(schema, header, hasheader, Int(skiprows), UInt32(limit), UInt32(buffersize), UInt8(nworkers), UInt8(maxtasks), UInt8(nresults))
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
    schema::Union{Nothing,Vector{DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    header::Union{Nothing,Vector{Symbol}}=nothing,
    hasheader::Bool=true,
    skiprows::Integer=UInt32(0),
    limit::Integer=UInt32(0),
    delim::Union{UInt8,Char}=',',
    quotechar::Union{UInt8,Char}='"',
    escapechar::Union{UInt8,Char}='"',
    sentinel::Union{Missing,String,Vector{String}}=missing,
    groupmark::Union{Char,UInt8,Nothing}=nothing,
    stripwhitespace::Bool=false,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(Threads.nthreads() * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    nresults::Integer=maxtasks,
    _force::Symbol=:none,
    use_mmap::Bool=false,
)
    (should_close, parsing_ctx, lexer_state, options) = setup_parser(
        input, schema; 
        header, hasheader, skiprows, delim, quotechar, limit, escapechar, sentinel, groupmark, stripwhitespace, 
        buffersize, nworkers, maxtasks, nresults, use_mmap
    )
    parse_file(lexer_state, parsing_ctx, consume_ctx, options, _force)
    should_close && close(lexer_state.io)
    return nothing
end

end # module