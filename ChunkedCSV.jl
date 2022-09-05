using ScanByte
import Parsers
using TranscodingStreams # TODO: ditch this

# IDEA: We could make a 48bit PosLen string type (8MB -> 23 bits if we represent 8MB as 0, 2 bits for metadata)
# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using Tuples of Refs (this might be more cache friendly?)
# IDEA: Introduce `unsafe_setindex!` to buffered vectors and ensure capacity every x iterations without checking it

# TODO: Use information from initial buffer fill to see if we're deaing with a small file (and have a fast path for that)

# In bytes. This absolutely has to be larger than any single row.
# Much safer if any two consecutive rows are smaller than this threshold.
const BUFFER_SIZE = UInt32(8 * 1024 * 1024)  # 8 MiB

include("BufferedVectors.jl")
include("TaskResults.jl")

struct ParsingContext
    schema::Vector{DataType}
    bytes::Vector{UInt8}
    eols::BufferedVector{UInt32}
end

function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_chunk_newline_at)
    ptr = pointer(buf)
    if last_chunk_newline_at == 0 # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = UInt32(Base.readbytes!(io, buf, BUFFER_SIZE; all = true))
    elseif last_chunk_newline_at < BUFFER_SIZE
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_chunk_newline_at, BUFFER_SIZE - last_chunk_newline_at)
        bytes_read_in = @inbounds UInt32(Base.readbytes!(io, @view(buf[BUFFER_SIZE - last_chunk_newline_at:end]), last_chunk_newline_at; all = true))
    else
        # Last chunk was consumed entirely
        bytes_read_in = UInt32(Base.readbytes!(io, buf, BUFFER_SIZE; all = true))
    end
    return bytes_read_in
end
function prepare_buffer!(io::NoopStream, buf::Vector{UInt8}, last_chunk_newline_at)
    bytes_read_in = prepare_buffer!(io.stream, buf, last_chunk_newline_at)
    TranscodingStreams.supplied!(io.state.buffer1, bytes_read_in)
    return bytes_read_in
end

# We process input data iteratively by populating a buffer from IO.
# In each iteration we first lex the newlines and then parse them in parallel.
# Assumption: we can find all valid endlines only by observing quotes (currently hardcoded to double quote)
#             and newline characters.
# TODO: '\n\r' currently produces 2 newlines...
findmark(ptr, bytes_to_search, ::Val{B}) where B = UInt(something(memchr(ptr, bytes_to_search, B), 0))
function lex_newlines_in_buffer(io::IO, parsing_ctxfs::ParsingContext, options, byteset::Val{B}, bytes_to_search::UInt32, quoted::Bool=false) where B
    ptr = pointer(parsing_ctxfs.bytes) # We never resize the buffer, the array shouldn't need to relocate
    e, q = options.e, options.oq
    orig_bytes_to_search = bytes_to_search
    buf = parsing_ctxfs.bytes
    eols = parsing_ctxfs.eols
    # ScanByte.memchr only accepts UInt for the `len` argument, but we want to store our data in UInt32,
    # so we do a little conversion dance here to avoid converting the input on every iteration.
    _orig_bytes_to_search = UInt(orig_bytes_to_search)
    _bytes_to_search = UInt(bytes_to_search)

    offset = UInt32(0)
    while bytes_to_search > 0
        pos_to_check = findmark(ptr, _bytes_to_search, byteset)
        offset = UInt32(-_bytes_to_search + _orig_bytes_to_search + pos_to_check)
        if pos_to_check == 0
            length(eols) < 2 && !eof(io) && error("CSV parse job failed on lexing newlines. There was no linebreak in the entire buffer of $bytes_to_search bytes.")
            break
        else
            byte_to_check = @inbounds buf[offset]
            if quoted
                if byte_to_check == e && get(buf, offset+1, 0xFF) == q
                    pos_to_check += 1
                elseif byte_to_check == q
                    quoted = false
                end
            else
                if byte_to_check == q
                    quoted = true
                elseif byte_to_check != e
                    push!(eols, offset)
                end
            end
            ptr += pos_to_check
            _bytes_to_search -= pos_to_check
        end
    end

    if eof(io)
        quoted && error("CSV parse job failed on lexing newlines. There file has ended with an unmatched quote.")
        done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        @inbounds eols.elements[eols.occupied] != orig_bytes_to_search && push!(eols, orig_bytes_to_search)
        last_chunk_newline_at = orig_bytes_to_search
    else
        done = false
        last_chunk_newline_at = @inbounds eols.elements[eols.occupied]
    end
    return last_chunk_newline_at, quoted, done
end
function lex_newlines_in_buffer(io::NoopStream, parsing_ctxfs::ParsingContext, options::Parsers.Options, byteset::Val{B}, bytes_to_search::UInt32, quoted::Bool=false) where B
    return lex_newlines_in_buffer(io.stream, parsing_ctxfs, options, byteset, bytes_to_search, quoted)
end


abstract type AbstractParsingContext end
struct DebugContext <: AbstractParsingContext; end
consume!(taks_buf::TaskResultBuffer{N}, parsing_ctxs::ParsingContext, row_num::UInt32, context::DebugContext) where {N} = nothing


macro _parse_file_setup()
    esc(quote
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        parsing_ctxs = ParsingContext(
            schema,
            Vector{UInt8}(undef, BUFFER_SIZE),
            BufferedVector{UInt32}([UInt32(0)], 1),
        )
        result_bufs = [TaskResultBuffer{N}(schema) for _ in 1:Threads.nthreads()] # has to match the number of spawned tasks
        done = false
        quoted = false
        row_num = UInt32(1)
        bytes_read_in = prepare_buffer!(io, parsing_ctxs.bytes, UInt32(0)) # init buffer
        # TODO: actually detect and parse header, now we only pretend we have done it
        header = ["a","b","c","d"]
        @assert N == 4
        bytes_read_in -= prepare_buffer!(io, parsing_ctxs.bytes, UInt32(8)) - UInt32(8) # "read header"
    end)
end

macro _parse_rows_forloop()
    esc(quote
    @inbounds result_buf = result_bufs[task_id]
    empty!(result_buf)
    Base.ensureroom(result_buf, length(task)+1)
    for chunk_row_idx in 2:length(task)
        @inbounds prev_newline = task[chunk_row_idx - 1]
        @inbounds curr_newline = task[chunk_row_idx]
        (curr_newline - prev_newline) == 1 && continue # ignore empty lines
        # +1 -1 to exclude delimiters
        @inbounds row_bytes = view(buf, prev_newline+1:curr_newline-1)

        pos = 1
        len = length(row_bytes)
        code = Parsers.OK
        row_status = NoMissing
        column_indicators = zero(M)
        for col_idx in 1:N
            type = schema[col_idx]
            if Parsers.eof(code)
                row_status = TooFewColumnsError
                break # from column parsing (does this need to be a @goto?)
            end
            if type === Int
                (;val, tlen, code) = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Int}, val)
            elseif type === Float64
                (;val, tlen, code) = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Float64}, val)
            elseif type === String
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options)::Parsers.Result{Parsers.PosLen}
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Parsers.PosLen}, Parsers.PosLen(prev_newline+pos, val.len))
            else
                row_status = UnknownTypeError
                break # from column parsing (does this need to be a @goto?)
            end
            if Parsers.invalid(code)
                row_status = ValueParsingError
                break # from column parsing (does this need to be a @goto?)
            elseif Parsers.sentinel(code)
                row_status = HasMissing
                column_indicators |= M(1) << (col_idx - 1)
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status = TooManyColumnsError
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        !iszero(column_indicators) && push!(result_buf.column_indicators, column_indicators) # No inbounds as we're growing this buffer lazily
    end # for row_idx
    end)
end

function _parse_file(io, schema::Vector{DataType}, ctx::AbstractParsingContext, options::Parsers.Options, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    @_parse_file_setup
    while !done
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctxs, options, byteset, bytes_read_in, quoted)
        eols = parsing_ctxs.eols[]
        # At most one task per thread (per iteration), fewer if not enough rows to warrant spawning extra tasks
        task_size = max(5_000, cld(length(eols), Threads.nthreads()))
        @sync for (task_id, task) = enumerate(Iterators.partition(eols, task_size))
            Threads.@spawn begin
                buf = $(parsing_ctxs.bytes)
                @_parse_rows_forloop
                consume!(result_buf, $parsing_ctxs, $row_num, ctx) # Note we interpolated `row_num` to this task!
            end # @spawn
            row_num += UInt32(length(task))
        end #@sync
        empty!(parsing_ctxs.eols)
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        push!(parsing_ctxs.eols, UInt32(0))
        bytes_read_in = prepare_buffer!(io, parsing_ctxs.bytes, last_chunk_newline_at)
    end # while !done
end


function _parse_file_doublebuffer(io, schema::Vector{DataType}, ctx::AbstractParsingContext, options::Parsers.Options, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    @_parse_file_setup
    # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
    (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctxs, options, byteset, bytes_read_in, quoted)
    parsing_ctxs_next = ParsingContext(
        schema,
        Vector{UInt8}(undef, BUFFER_SIZE),
        BufferedVector{UInt32}(Vector{UInt32}(undef, parsing_ctxs.eols.occupied), 0),
    )
    while !done
        eols = parsing_ctxs.eols[]
        # At most one task per thread (per iteration), fewer if not enough rows to warrant spawning extra tasks
        task_size = max(5_000, cld(length(eols), Threads.nthreads()))
        @sync begin
            parsing_ctxs_next.bytes[last_chunk_newline_at:end] .= parsing_ctxs_next.bytes[last_chunk_newline_at:end]
            Threads.@spawn begin
                empty!(parsing_ctxs_next.eols)
                push!(parsing_ctxs_next.eols, UInt32(0))
                bytes_read_in = prepare_buffer!(io, parsing_ctxs_next.bytes, last_chunk_newline_at)
                (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, parsing_ctxs_next, options, byteset, bytes_read_in, quoted)
            end
            for (task_id, task) = enumerate(Iterators.partition(eols, task_size))
                Threads.@spawn begin
                     # We have to interpolate the buffer into the task otherwise this allocates like crazy
                     # We interpolate here because interpolation doesn't work in nested macros (`@spawn @inbounds $buf` doesn't work)
                    buf = $(parsing_ctxs.bytes)
                    @_parse_rows_forloop
                    consume!(result_buf, $parsing_ctxs, $row_num, ctx) # Note we interpolated `row_num` to this task!
                end # @spawn
                row_num += UInt32(length(task))
            end # for (task_id, task)
        end #@sync
        parsing_ctxs, parsing_ctxs_next = parsing_ctxs_next, parsing_ctxs
    end # while !done
end

_input_to_io(input::IO) = input
function _input_to_io(input::String)
    io = NoopStream(open(input, "r"))
    TranscodingStreams.changemode!(io, :read)
    return io
end

function _create_options(delim::Char, quotechar::Char, escapechar::Char; for_header::Bool)
    # Parsers.jl doesn't allow ambiguity between whitespace and delimiters, so we set
    # the whitespace to bogus when the delimiters are spaces and tabs.
    if delim == ' ' || delim == '\t'
        wh1 = 'x'
        wh2 = 'x'
    else
        wh1 = ' '
        wh2 = '\t'
    end

    # Allows to parse various strings into Boolean.
    mytrues = String["true", "1", "True", "t"]
    myfalses = String["false", "0", "False", "f"]

    # Within the header's fields, trim whitespace aggressively.
    stripwhitespace_header = for_header && (delim !== ' ' && delim !== '\t')

    return Parsers.Options(
        sentinel=missing,
        wh1=wh1,
        wh2=wh2,
        openquotechar=UInt8(quotechar),
        closequotechar=UInt8(quotechar),
        escapechar=UInt8(escapechar),
        delim=UInt8(delim),
        quoted=true,
        ignoreemptylines=true,
        stripwhitespace=stripwhitespace_header,
        trues=mytrues,
        falses=myfalses,
    )
end

function parse_file(
    input,
    schema,
    doublebuffer::Bool=false,
    context::AbstractParsingContext=DebugContext(),
    quotechar::Char='"',
    delim::Char=',',
    escapechar::Char='"',
)
    io = _input_to_io(input)
    options = _create_options(delim, quotechar, escapechar; for_header=false)
    byteset = Val(ByteSet((UInt8(options.e),UInt8(options.oq),UInt8('\n'),UInt8('\r'))))
    if doublebuffer
        _parse_file_doublebuffer(io, schema, context, options, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))
    else
        _parse_file(io, schema, context, options, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))
    end
    close(io)
    return nothing
end
