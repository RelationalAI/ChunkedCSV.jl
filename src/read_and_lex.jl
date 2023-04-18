mutable struct LexerState{B #=`ByteSet` for ScanByte.jl=#, IO_t<:IO}
    io::IO_t
    # The position of the last newline in the previous chunk. We don't need to check the first
    # `buffersize - last_newline_at` bytes for newlines, as we already search them last time.
    last_newline_at::Int32
    # Last chunk ended inside quoted field, next chunk will start looking for newlines in "quoted" mode
    quoted::Bool
    # We've reached end of stream
    done::Bool
    # Last chunk ended with an escape control character (we'll skip one byte in the next chunk)
    ended_on_escape::Bool

    LexerState{B}(input::IO) where {B} = new{B, typeof(input)}(input, 0, false, false, false)
end

end_of_stream(io::IO) = eof(io)
end_of_stream(io::GzipDecompressorStream) = eof(io)

readbytesall!(io::IOStream, buf, n::Int) = Base.readbytes!(io, buf, n; all = true)
readbytesall!(io::IO, buf, n::Int) = Base.readbytes!(io, buf, n)
function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_newline_at::Int)
    ptr = pointer(buf)
    buffersize = length(buf)
    @inbounds if last_newline_at == 0 # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = readbytesall!(io, buf, buffersize)
        if bytes_read_in > 2 && hasBOM(buf)
            n = prepare_buffer!(io, buf, 3)
            bytes_read_in -= 3 - n
        end
    elseif last_newline_at < buffersize
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_newline_at, buffersize - last_newline_at)
        bytes_read_in = @inbounds readbytesall!(io, @view(buf[buffersize - last_newline_at + 1:end]), last_newline_at)
    else
        # Last chunk was consumed entirely
        bytes_read_in = readbytesall!(io, buf, buffersize)
    end
    return bytes_read_in
end

struct InvalidStateException <: Exception
    msg::String
end

function check_any_valid_rows(eols, lexer_state, reached_end_of_file, buffersize)
    if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !reached_end_of_file
        close(lexer_state.io)
        throw(NoValidRowsInBufferError(buffersize))
    end
end

@noinline function invalid_state_exception(ptr, base_ptr, buffersize, bytes_to_search, offset, pos_to_check, bytes_read_in, reached_end_of_file, bytes_carried_over_from_previous_chunk)
    throw(InvalidStateException(string((;ptr, base_ptr, buffersize, bytes_to_search, offset, pos_to_check, bytes_read_in, reached_end_of_file, bytes_carried_over_from_previous_chunk))))
end
# We process input data iteratively by populating a buffer from IO.
# In each iteration we first lex the newlines and then parse them in parallel.
# Assumption: we can find all valid endlines only by observing quotes (currently hardcoded to double quote)
#             and newline characters.
function findmark(ptr, bytes_to_search, ::Val{B}) where B
    return Base.bitcast(Int, something(memchr(ptr, Base.bitcast(UInt, bytes_to_search), B), 0))
end
function read_and_lex!(lexer_state::LexerState{B}, parsing_ctx::ParsingContext, options::Parsers.Options) where B
    @assert !lexer_state.done
    # local offset::Int32
    ptr = pointer(parsing_ctx.bytes) # We never resize the buffer, the array shouldn't need to relocate
    base_ptr = ptr
    (e, oq, cq) = (options.e, options.oq.token, options.cq.token)::Tuple{UInt8,UInt8,UInt8}
    buf = parsing_ctx.bytes

    empty!(parsing_ctx.eols)
    push!(parsing_ctx.eols, Int32(0))
    eols = parsing_ctx.eols
    quoted = lexer_state.quoted
    buffersize = length(buf)
    @assert 0 < buffersize <= typemax(Int32)

    bytes_read_in = prepare_buffer!(lexer_state.io, parsing_ctx.bytes, Int(lexer_state.last_newline_at))
    @assert 0 <= bytes_read_in <= buffersize
    reached_end_of_file = end_of_stream(lexer_state.io)

    bytes_to_search = bytes_read_in
    if lexer_state.last_newline_at == UInt(0) # first time populating the buffer
        bytes_carried_over_from_previous_chunk = Int32(0)
        offset = bytes_carried_over_from_previous_chunk
    else
        # First length(buf) - last_newline_at bytes we've already seen in the previous round
        bytes_carried_over_from_previous_chunk = Int32(buffersize) - lexer_state.last_newline_at
        offset = bytes_carried_over_from_previous_chunk
        if lexer_state.ended_on_escape
            if e == cq # Here is where we resolve the ambiguity from the last chunk
                if @inbounds buf[offset+1] == e
                    # The last byte of the previous chunk was actually escaping a quote or escape char
                    # we can just skip over it
                    offset += Int32(1)
                    bytes_to_search -= 1
                else
                    # The last byte of the previous chunk was actually a closing quote
                    quoted = false
                end
            else
                offset += Int32(1)
                bytes_to_search -= 1
            end
        end
        ptr += offset
    end

    pos_to_check = 0
    last_byte = bytes_carried_over_from_previous_chunk + Int32(bytes_read_in)
    lexer_state.ended_on_escape = false
    (0 <= bytes_to_search <= bytes_read_in <= buffersize &&
        0 <= offset <= buffersize &&
        (reached_end_of_file || ((bytes_read_in + bytes_carried_over_from_previous_chunk) == buffersize)) &&
        (ptr - base_ptr + bytes_to_search == bytes_read_in + bytes_carried_over_from_previous_chunk) &&
        (ptr - base_ptr == offset)) ||
        invalid_state_exception(ptr, base_ptr, buffersize, bytes_to_search, offset, pos_to_check, bytes_read_in, reached_end_of_file, bytes_carried_over_from_previous_chunk)
    @inbounds while bytes_to_search > 0
        ((ptr - base_ptr + bytes_to_search == bytes_read_in + bytes_carried_over_from_previous_chunk) &&
            (ptr - base_ptr == offset)) ||
            invalid_state_exception(ptr, base_ptr, buffersize, bytes_to_search, offset, pos_to_check, bytes_read_in, reached_end_of_file, bytes_carried_over_from_previous_chunk)
        pos_to_check = findmark(ptr, bytes_to_search, B)
        offset += Int32(pos_to_check)
        if pos_to_check == 0
            break
        else
            byte_to_check = buf[offset]
            if quoted # We're inside a quoted field
                if byte_to_check == e
                    if offset < last_byte
                        if buf[offset+Int32(1)] in (e, cq)
                            pos_to_check += 1
                            offset += Int32(1)
                        elseif e == cq
                            quoted = false
                        end
                    else # end of chunk
                        # Note that when e == cq, we can't be sure if we saw a closing char
                        # or an escape char and we won't be sure until the next chunk arrives
                        # Since this is the last byte of the chunk it could be also the last
                        # byte of the entire file and ending on an unmatched quote is an error
                        if e == cq
                            quoted = !reached_end_of_file
                        end
                        lexer_state.ended_on_escape = true
                    end
                elseif byte_to_check == cq
                    quoted = false
                end
            else
                if byte_to_check == oq
                    quoted = true
                elseif byte_to_check == e || byte_to_check == cq
                    # this will most likely trigger a parser error
                else # newline
                    push!(eols, offset)
                end
            end
            ptr += pos_to_check
            bytes_to_search -= pos_to_check
        end
    end

    check_any_valid_rows(eols, lexer_state, reached_end_of_file, buffersize)

    @inbounds if reached_end_of_file
        quoted && (close(lexer_state.io); throw(UnmatchedQuoteError()))
        lexer_state.done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last(eols) < last_byte && push!(eols, last_byte + Int32(1))
    else
        lexer_state.done = false
    end
    lexer_state.quoted = quoted
    lexer_state.last_newline_at = last(eols)
    return nothing
end

mutable struct MmapStream{IO_t<:IO} <: IO
    ios::IO_t
    x::Vector{UInt8}
    pos::Int
end
MmapStream(ios::IO) = MmapStream(ios, mmap(ios, grow=false, shared=false), 1)
Base.close(m::MmapStream) = close(m.ios)
Base.eof(m::MmapStream) = m.pos == length(m.x)
function readbytesall!(io::MmapStream, buf, n::Int)
    bytes_to_read = min(bytesavailable(io), n)
    unsafe_copyto!(pointer(buf), pointer(io.x) + io.pos - 1, bytes_to_read)
    io.pos += bytes_to_read
    return bytes_to_read
end
# Interop with GzipDecompressorStream
Base.bytesavailable(m::MmapStream) = length(m.x) - m.pos
Base.isopen(m::MmapStream) = isopen(m.ios) && !eof(m)
function Base.unsafe_read(from::MmapStream, p::Ptr{UInt8}, nb::UInt)
    avail = bytesavailable(from)
    adv = min(avail, nb)
    GC.@preserve from unsafe_copyto!(p, pointer(from.x) + from.pos - 1, adv)
    from.pos += adv
    if nb > avail
        throw(EOFError())
    end
    nothing
end

_input_to_io(input::IO, use_mmap::Bool) = false, input
function _input_to_io(input::String, use_mmap::Bool)
    ios = open(input, "r")
    if !eof(ios) && peek(ios, UInt16) == 0x8b1f
        # TODO: GzipDecompressorStream doesn't respect MmapStream reaching EOF for some reason
        # io = CodecZlib.GzipDecompressorStream(use_mmap ? MmapStream(ios) : ios, stop_on_end=use_mmap)
        use_mmap && @warn "`use_mmap=true` is currently unsupported when reading gzipped files, using file io."
        io = CodecZlib.GzipDecompressorStream(ios)
        TranscodingStreams.changemode!(io, :read)
    elseif use_mmap
        io = MmapStream(ios)
    else
        io = ios
    end
    return true, io
end
