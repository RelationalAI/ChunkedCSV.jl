using ScanByte
import Parsers
using TranscodingStreams # TODO: ditch this

# IDEA: We could make a 48bit PosLen string type (8MB -> 23 bits if we represent 8MB as 0, 2 bits for metadata)
# IDEA: Instead of having SoA layout in TaskResultBuffer, we could try AoS using Tuples of Refs (this might be more cache friendly?)
# IDEA: Introduce `unsafe_setindex!` to buffered vectors and ensure capacity every x iterations without checking it

# TODO: Instead of `GLOBAL_BYTE_BUFFER`, create a local buffer in `parse_file`
# TODO: Use information from initial

# In bytes. This absolutely has to be larger than any single row.
# Much safer if any two consecutive rows are smaller than this threshold.
const BUFFER_SIZE = UInt(8 * 1024 * 1024)  # 8 MiB
const GLOBAL_BYTE_BUFFER = Vector{UInt8}(undef, BUFFER_SIZE)

include("BufferedVectors.jl")
include("TaskResults.jl")

function prepare_buffer!(f::NoopStream, buf::Vector{UInt8}, last_chunk_newline_at)
    ptr = pointer(buf)
    if last_chunk_newline_at == 0 # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = UInt(Base.readbytes!(f.stream, buf, BUFFER_SIZE; all = true))
    elseif last_chunk_newline_at < BUFFER_SIZE
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_chunk_newline_at, BUFFER_SIZE - last_chunk_newline_at)
        bytes_read_in = @inbounds UInt(Base.readbytes!(f.stream, @view(buf[BUFFER_SIZE - last_chunk_newline_at:end]), last_chunk_newline_at; all = true))
    else
        # Last chunk was consumed entirely
        bytes_read_in = UInt(Base.readbytes!(f.stream, buf, BUFFER_SIZE; all = true))
    end
    TranscodingStreams.supplied!(f.state.buffer1, bytes_read_in)
    return bytes_read_in
end

# We process input data iteratively by populating a buffer from IO.
# In each iteration we first lex the newlines and then parse them in parallel.
# Assumption: we can find all valid endlines only by observing quotes (currently hardcoded to double quote)
#             and newline characters.
# TODO: '\n\r' currently produces 2 newlines...
const BYTESET = Val(ByteSet((UInt8('"'),UInt8('\n'),UInt8('\r'))))
findmark(ptr, bytes_to_search) = UInt(something(memchr(ptr, bytes_to_search, BYTESET), 0))
function lex_newlines_in_buffer(f::NoopStream, eols::BufferedVector{UInt}, bytes_to_search::UInt, quoted::Bool=false)
    buf = GLOBAL_BYTE_BUFFER
    ptr = pointer(buf) # We never resize the buffer, the array shouldn't need to relocate
    orig_bytes_to_search = bytes_to_search

    offset = UInt(0)
    while bytes_to_search > 0
        pos_to_check = findmark(ptr, bytes_to_search)
        offset = UInt(-bytes_to_search + orig_bytes_to_search + pos_to_check)::UInt
        if pos_to_check == 0
            isempty(eols) && !eof(f.stream) && error("CSV parse job failed on lexing newlines. There was no linebreak in the entire buffer of $bytes_to_search bytes.")
            break
        else
            byte_to_check = @inbounds buf[offset]
            if quoted
                if byte_to_check == UInt8('"') && get(buf, offset+1, 0xFF) == UInt8('"')
                    pos_to_check += 1
                else
                    quoted = false
                end
            else
                if byte_to_check == UInt8('"')
                    quoted = true
                else
                    eols[] = offset
                end
            end
            ptr += pos_to_check
            bytes_to_search -= pos_to_check
        end
    end

    if eof(f.stream)
        done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        @inbounds eols.elements[eols.occupied] != orig_bytes_to_search && (eols[] = orig_bytes_to_search)
        last_chunk_newline_at = orig_bytes_to_search
    else
        done = false
        last_chunk_newline_at = eols.elements[eols.occupied]
    end
    return last_chunk_newline_at, quoted, done
end

abstract type AbstractContext end
# This is where the parsed results get consumed.
# Users could dispatch on AbstractContext. Currently WIP sketch of what will be needed for RAI.
function consume!(buf::TaskResultBuffer{N}, row_num::Int, byte_offset::UInt, context::Union{AbstractContext,Nothing}=nothing) where {N}
    # # errsink = context.errsink
    # # eols = context.eols
    # @inbounds for c in 1:N
    #     row = row_num
    #     col = buf.cols[c].elements
    #     # sink = context.sinks[c]
    #     for r in 1:length(buf.row_statuses)
    #         row_status = buf.row_statuses.elements[r]
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
    return nothing
end

function _parse_file(name, schema::Vector{DataType}, ::Val{N}, ::Val{M}) where {N,M}
    f = NoopStream(open(name, "r"))
    TranscodingStreams.changemode!(f, :read)

    options = Parsers.Options(openquotechar='"', closequotechar='"', delim=',')
    result_bufs = [TaskResultBuffer{N}(schema) for _ in 1:Threads.nthreads()] # has to match the number of spawned tasks
    done = false
    quoted = false
    eols_buf = BufferedVector{UInt}() # end-of-line buffer
    # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
    # signal that. This works out even for the very first chunk.
    eols_buf[] = UInt(0)
    row_num = 1
    byte_offset = UInt(1)
    bytes_read_in = prepare_buffer!(f, GLOBAL_BYTE_BUFFER, UInt(0)) # init buffer
    # TODO: actually detect and parse header, now we only pretend we have done it
    header = ["a","b","c","d"]
    @assert N == 4
    bytes_read_in -= prepare_buffer!(f, GLOBAL_BYTE_BUFFER, UInt(8)) - UInt(8) # "read header"
    while !done
        # Updates eols_buf with new newlines, byte buffer was updated either from initialization stage or at the end of the loop
        (last_chunk_newline_at, quoted, done) = lex_newlines_in_buffer(f, eols_buf, bytes_read_in, quoted)
        eols = eols_buf[]

        # At most one task per thread (per iteration), fewer if not enough rows to warrant spawning extra tasks
        task_size = max(5_000, cld(length(eols), Threads.nthreads()))
        @sync for (task_id, task) = enumerate(Iterators.partition(eols, task_size))
            # TODO: currently, `byte_offset` computation is not correct, it is only used in `consume!`, i.e. not needed for parsing itself
            @inbounds byte_offset += task[1]
            Threads.@spawn begin
                result_buf = result_bufs[task_id]
                empty!(result_buf)
                @inbounds for chunk_row_idx in 2:length(task)
                    prev_newline = task[chunk_row_idx - 1]
                    curr_newline = task[chunk_row_idx]
                    # +1 -1 to exclude delimiters
                    row_bytes = @view GLOBAL_BYTE_BUFFER[prev_newline+1:curr_newline-1]

                    pos = 1
                    len = length(row_bytes)
                    code = Parsers.OK
                    row_status = NoMissing
                    missing_flags = zero(M)
                    for col_idx in 1:N
                        type = schema[col_idx]
                        if Parsers.eof(code)
                            row_status = TooFewColumnsError
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
                            break # from column parsing (does this need to be a @goto?)
                        end
                        if Parsers.invalid(code)
                            row_status = ValueParsingError
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
                    # TODO: should we store missing_flags even when there were no missing values?
                    !iszero(missing_flags) && (result_buf.missings_flags[] = missing_flags)
                end # for row_idx
                consume!(result_buf, $row_num, $byte_offset, nothing) # Note we interpolated `row_num` and `byte_offset` to this task!
            end # @spawn
            row_num += length(task)
        end #@sync
        empty!(eols_buf)
        # We always end on a newline when processing a chunk, so we're inserting a dummy variable to
        # signal that. This works out even for the very first chunk.
        eols_buf[] = UInt(0)
        bytes_read_in = prepare_buffer!(f, GLOBAL_BYTE_BUFFER, last_chunk_newline_at)
        byte_offset -= BUFFER_SIZE - bytes_read_in
    end # while !done
    close(f)
end


parse_file(name, schema) = _parse_file(name, schema, Val(length(schema)), Val(_bounding_flag_type(length(schema))))

