module ChunkedCSV

export parse_file, consume!, AbstractConsumeContext

using ScanByte
import Parsers
using .Threads: @spawn
using TranscodingStreams # TODO: ditch this
using Dates: Date, DateTime
using FixedPointDecimals

# IDEA: We could make a 48bit PosLen string type (8MB -> 23 bits if we represent 8MB as 0, 2 bits for metadata)
# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using "reinterpretable bytes"

include("BufferedVectors.jl")
include("TaskResults.jl")

include("fixed_decimals_utils.jl")

mutable struct TaskCondition
    ntasks::Int
    cond_wait::Threads.Condition
end

struct ParsingContext
    schema::Vector{DataType}
    header::Vector{Symbol}
    bytes::Vector{UInt8}
    eols::BufferedVector{UInt32}
    limit::UInt32
    nworkers::UInt8
    maxtasks::UInt8
    cond::TaskCondition
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
end

function limit_eols!(parsing_ctx::ParsingContext, row_num)
    parsing_ctx.limit == 0 && return false
    if row_num > parsing_ctx.limit
        return true
    elseif row_num <= parsing_ctx.limit < row_num + UInt32(length(parsing_ctx.eols) - 1)
        parsing_ctx.eols.occupied -= ((UInt32(parsing_ctx.eols.occupied) + row_num) - parsing_ctx.limit - UInt32(2))
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

function parse_file(
    input,
    schema::Union{Nothing,Vector{DataType}}=nothing,
    consume_ctx::AbstractConsumeContext=DebugContext();
    quotechar::Union{UInt8,Char}='"',
    delim::Union{UInt8,Char}=',',
    escapechar::Union{UInt8,Char}='"',
    header::Union{Nothing,Vector{Symbol}}=nothing,
    hasheader::Bool=true,
    skiprows::Integer=UInt32(0),
    limit::Integer=UInt32(0),
    doublebuffer::Bool=true,
    # In bytes. This absolutely has to be larger than any single row.
    # Much safer if any two consecutive rows are smaller than this threshold.
    buffersize::Integer=UInt32(8 * 1024 * 1024),
    nworkers::Integer=Threads.nthreads(),
    maxtasks::Integer=2Threads.nthreads(),
    _force::Symbol=:none,
)
    @assert 0 < buffersize < typemax(UInt32)
    @assert skiprows >= 0
    @assert limit >= 0
    @assert nworkers > 0
    @assert maxtasks >= nworkers
    @assert _force in (:none, :serial, :singlebuffer, :doublebuffer)
    !isnothing(header) && !isnothing(schema) && length(header) != length(schema) && error("Provided header doesn't match the number of column of schema ($(length(header)) names, $(length(schema)) types).")

    io = _input_to_io(input)
    settings = ParserSettings(schema, header, hasheader, Int(skiprows), UInt32(limit), UInt32(buffersize), UInt8(nworkers), UInt8(maxtasks))
    options = _create_options(delim, quotechar, escapechar)
    byteset = Val(ByteSet((UInt8(options.e), UInt8(options.oq), UInt8('\n'), UInt8('\r'))))
    (parsing_ctx, last_chunk_newline_at, quoted, done) = init_parsing!(io, settings, options, Val(byteset))
    schema = parsing_ctx.schema

    if _force === :doublebuffer
        _parse_file_doublebuffer(io, parsing_ctx, consume_ctx, options, last_chunk_newline_at, quoted, done, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))::Nothing
    elseif _force === :singlebuffer
        _parse_file_singlebuffer(io, parsing_ctx, consume_ctx, options, last_chunk_newline_at, quoted, done, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))::Nothing
    elseif _force === :serial || Threads.nthreads() == 1 || settings.nworkers == 1 || settings.maxtasks == 1 || buffersize < 32 * 1024 || last_chunk_newline_at < 32 * 1024
              _parse_file_serial(io, parsing_ctx, consume_ctx, options, last_chunk_newline_at, quoted, done, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))::Nothing
    elseif doublebuffer && !done
        _parse_file_doublebuffer(io, parsing_ctx, consume_ctx, options, last_chunk_newline_at, quoted, done, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))::Nothing
    else
        _parse_file_singlebuffer(io, parsing_ctx, consume_ctx, options, last_chunk_newline_at, quoted, done, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))::Nothing
    end
    close(io)
    return nothing
end

end # module