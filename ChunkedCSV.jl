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
function lex_newlines_in_buffer(io::IO, buf, eols::BufferedVector{UInt32}, options, byteset::Val{B}, bytes_to_search::UInt32, quoted::Bool=false) where B
    ptr = pointer(buf) # We never resize the buffer, the array shouldn't need to relocate
    e, q = options.e, options.oq
    orig_bytes_to_search = bytes_to_search
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
                    eols[] = offset
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
        @inbounds eols.elements[eols.occupied] != orig_bytes_to_search && (eols[] = orig_bytes_to_search)
        last_chunk_newline_at = orig_bytes_to_search
    else
        done = false
        last_chunk_newline_at = @inbounds eols.elements[eols.occupied]
    end
    return last_chunk_newline_at, quoted, done
end
function lex_newlines_in_buffer(io::NoopStream, buf, eols::BufferedVector{UInt32}, options, byteset::Val{B}, bytes_to_search::UInt32, quoted::Bool=false) where B
    return lex_newlines_in_buffer(io.stream, buf, eols, options, byteset, bytes_to_search, quoted)
end


abstract type AbstractContext end
# This is where the parsed results get consumed.
# Users could dispatch on AbstractContext. Currently WIP sketch of what will be needed for RAI.
function consume!(taks_buf::TaskResultBuffer{N}, row_num::Int, byte_offset::UInt32, context::Union{AbstractContext,Nothing}=nothing) where {N}
    # # errsink = context.errsink
    # # eols = context.eols
    # @inbounds for c in 1:N
    #     row = row_num
    #     col = taks_buf.cols[c].elements
    #     # sink = context.sinks[c]
    #     for r in 1:length(taks_buf.row_statuses)
    #         row_status = taks_buf.row_statuses.elements[r]
    #         val = col[r]
    #         if row_status === NoMissing
    #         elseif row_status === HasMissing
    #         elseif row_status === TooFewColumnsError
    #         elseif row_status === UnknownTypeError
    #         elseif row_status === ValueParsingError
    #         elseif row_status === TooManyColumnsError
    #         else
    #             @assert false "unreachable"
    #         end
    #         row += 1
    #     end
    # end
    # @info taks_buf.cols[1].elements[1:10]
    # @info (row_num,byte_offset)
    return nothing
end

macro _parse_file_setup()
    esc(quote
        buf = Vector{UInt8}(undef, BUFFER_SIZE)

        result_bufs = [TaskResultBuffer{N}(schema) for _ in 1:Threads.nthreads()] # has to match the number of spawned tasks
        done = false
        quoted = false
        eols_buf = BufferedVector{UInt32}() # end-of-line buffer
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        eols_buf[] = UInt32(0)
        row_num = 1
        byte_offset = UInt32(1)
        bytes_read_in = prepare_buffer!(io, buf, UInt32(0)) # init buffer
        # TODO: actually detect and parse header, now we only pretend we have done it
        header = ["a","b","c","d"]
        @assert N == 4
        bytes_read_in -= prepare_buffer!(io, buf, UInt32(8)) - UInt32(8) # "read header"
    end)
end

macro _parse_rows_forloop()
    esc(quote
    for chunk_row_idx in 2:length(task)
        prev_newline = task[chunk_row_idx - 1]
        curr_newline = task[chunk_row_idx]
        # +1 -1 to exclude delimiters
        row_bytes = view(_buf, prev_newline+1:curr_newline-1)

        pos = 1
        len = length(row_bytes)
        code = Parsers.OK
        row_status = NoMissing
        missing_flags = zero(M)
        for col_idx in 1:N
            type = schema[col_idx]
            if Parsers.eof(code)
                row_status = TooFewColumnsError
                # TODO: bump capacity of the rest of the columns
                break # from column parsing (does this need to be a @goto?)
            end
            if type === Int
                (;val, tlen, code) = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                (getindex(result_buf.cols, col_idx)::BufferedVector{Int})[] = val
            elseif type === Float64
                (;val, tlen, code) = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                (getindex(result_buf.cols, col_idx)::BufferedVector{Float64})[] = val
            elseif type === String
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options)::Parsers.Result{Parsers.PosLen}
                (getindex(result_buf.cols, col_idx)::BufferedVector{Parsers.PosLen})[] = Parsers.PosLen(prev_newline+pos, val.len)
            else
                row_status = UnknownTypeError
                # TODO: bump capacity of the rest of the columns
                break # from column parsing (does this need to be a @goto?)
            end
            if Parsers.invalid(code)
                row_status = ValueParsingError
                # TODO: bump capacity of the rest of the columns
                break # from column parsing (does this need to be a @goto?)
            elseif Parsers.sentinel(code)
                row_status = HasMissing
                missing_flags |= 1 << (col_idx - 1)
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status = TooManyColumnsError
        end
        result_buf.row_statuses[] = row_status
        !iszero(missing_flags) && (result_buf.missings_flags[] = missing_flags)
    end # for row_idx
    end)
end

function _parse_file(io, schema::Vector{DataType}, options, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    @_parse_file_setup
    while !done
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, buf, eols_buf, options, byteset, bytes_read_in, quoted)
        eols = eols_buf[]
        # At most one task per thread (per iteration), fewer if not enough rows to warrant spawning extra tasks
        task_size = max(5_000, cld(length(eols), Threads.nthreads()))
        @sync for (task_id, task) = enumerate(Iterators.partition(eols, task_size))
            # TODO: currently, `byte_offset` computation is not correct, it is only used in `consume!`, i.e. not needed for parsing itself
            @inbounds byte_offset += task[1]
            Threads.@spawn begin
                @inbounds result_buf = result_bufs[task_id]
                empty!(result_buf)
                _buf = $buf
                @inbounds @_parse_rows_forloop
                consume!(result_buf, $row_num, $byte_offset, nothing) # Note we interpolated `row_num` and `byte_offset` to this task!
            end # @spawn
            row_num += length(task)
        end #@sync
        empty!(eols_buf)
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        eols_buf[] = UInt32(0)
        bytes_read_in = prepare_buffer!(io, buf, last_chunk_newline_at)
        byte_offset -= BUFFER_SIZE - bytes_read_in
    end # while !done
end


function _parse_file_doublebuffer(io, schema::Vector{DataType}, options, ::Val{N}, ::Val{M}, byteset::Val{B}) where {N,M,B}
    @_parse_file_setup
    buf_next = Vector{UInt8}(undef, BUFFER_SIZE)
    # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
    (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, buf, eols_buf, options, byteset, bytes_read_in, quoted)
    eols_buf_next = BufferedVector{UInt32}(Vector{UInt32}(undef, eols_buf.occupied), 0) # double-buffering
    while !done
        eols = eols_buf[]
        # At most one task per thread (per iteration), fewer if not enough rows to warrant spawning extra tasks
        task_size = max(5_000, cld(length(eols), Threads.nthreads()))
        @sync begin
            buf_next[last_chunk_newline_at:end] .= buf[last_chunk_newline_at:end]
            Threads.@spawn begin
                empty!(eols_buf_next)
                eols_buf_next[] = UInt32(0)
                bytes_read_in = prepare_buffer!(io, buf_next, last_chunk_newline_at)
                (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(io, buf_next, eols_buf_next, options, byteset, bytes_read_in, quoted)
            end
            for (task_id, task) = enumerate(Iterators.partition(eols, task_size))
                # TODO: currently, `byte_offset` computation is not correct, it is only used in `consume!`, i.e. not needed for parsing itself
                @inbounds byte_offset += task[1]
                Threads.@spawn begin
                    @inbounds result_buf = result_bufs[task_id]
                    empty!(result_buf)
                     # We have to interpolate the buffer into the task otherwise this allocates like crazy
                     # We interpolate here because interpolation doesn't work in nested macros (`@spawn @inbounds $buf` doesn't work)
                    _buf = $buf
                    @inbounds @_parse_rows_forloop
                    consume!(result_buf, $row_num, $byte_offset, nothing) # Note we interpolated `row_num` and `byte_offset` to this task!
                end # @spawn
                row_num += length(task)
            end # for (task_id, task)
        end #@sync
        byte_offset -= BUFFER_SIZE - bytes_read_in
        buf, buf_next = buf_next, buf
        eols_buf, eols_buf_next = eols_buf_next, eols_buf
    end # while !done
end

function parse_file(input, schema, doublebuffer=false, quotechar='"', delim=',', escapechar='"')
    io = _input_to_io(input)
    options = Parsers.Options(openquotechar=quotechar, closequotechar=quotechar, delim=delim, escapechar=escapechar)
    byteset = Val(ByteSet((UInt8(options.e),UInt8(options.oq),UInt8('\n'),UInt8('\r'))))
    if doublebuffer
        _parse_file_doublebuffer(io, schema, options, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))
    else
        _parse_file(io, schema, options, Val(length(schema)), Val(_bounding_flag_type(length(schema))), Val(byteset))
    end
    close(io)
end

_input_to_io(input::IO) = input
function _input_to_io(input::String)
    io = NoopStream(open(input, "r"))
    TranscodingStreams.changemode!(io, :read)
    return io
end