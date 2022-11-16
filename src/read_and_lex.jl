mutable struct LexerState{B, IO_t<:IO}
    io::IO_t
    last_newline_at::UInt32
    quoted::Bool
    done::Bool

    LexerState{B}(input::IO) where {B} = new{B, typeof(input)}(input, UInt32(0), false, false)
end

end_of_stream(io::IO) = eof(io)
end_of_stream(io::GzipDecompressorStream) = eof(io)

readbytesall!(io::IOStream, buf, n) = UInt32(Base.readbytes!(io, buf, n; all = true))
readbytesall!(io::IO, buf, n) = UInt32(Base.readbytes!(io, buf, n))
function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_newline_at)
    ptr = pointer(buf)
    buffersize = UInt32(length(buf))
    if last_newline_at == 0 # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = readbytesall!(io, buf, buffersize)
        if bytes_read_in > 2 && hasBOM(buf)
            bytes_read_in -= prepare_buffer!(io, buf, UInt32(3)) - UInt32(3)
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

# We process input data iteratively by populating a buffer from IO.
# In each iteration we first lex the newlines and then parse them in parallel.
# Assumption: we can find all valid endlines only by observing quotes (currently hardcoded to double quote)
#             and newline characters.
findmark(ptr, bytes_to_search, ::Val{B}) where B = something(memchr(ptr, bytes_to_search, B), zero(UInt))
function read_and_lex!(lexer_state::LexerState{B}, parsing_ctx::ParsingContext, options) where B
    ptr = pointer(parsing_ctx.bytes) # We never resize the buffer, the array shouldn't need to relocate
    (e, oq, cq) = (options.e, options.oq.token, options.cq.token)::Tuple{UInt8,UInt8,UInt8}
    buf = parsing_ctx.bytes

    empty!(parsing_ctx.eols)
    push!(parsing_ctx.eols, UInt32(0))
    eols = parsing_ctx.eols
    quoted = lexer_state.quoted
    buffersize = UInt32(length(buf))

    bytes_read_in = prepare_buffer!(lexer_state.io, parsing_ctx.bytes, lexer_state.last_newline_at)
    reached_end_of_file = end_of_stream(lexer_state.io)

    bytes_to_search = UInt(bytes_read_in)
    if lexer_state.last_newline_at == UInt(0) # first time populating the buffer
        bytes_carried_over_from_previous_chunk = UInt32(0)
        offset = bytes_carried_over_from_previous_chunk
    else
        # First length(buf) - last_newline_at bytes we've already seen in the previous round
        bytes_carried_over_from_previous_chunk = buffersize - lexer_state.last_newline_at
        offset = bytes_carried_over_from_previous_chunk
        ptr += offset
    end
    @inbounds while bytes_to_search > UInt(0)
        pos_to_check = findmark(ptr, bytes_to_search, B)
        offset += UInt32(pos_to_check)
        if pos_to_check == UInt(0)
            if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !reached_end_of_file
                close(lexer_state.io)
                throw(NoValidRowsInBufferError(UInt32(length(buf))))
            end
            break
        else
            byte_to_check = buf[offset]
            if quoted
                if byte_to_check == e && (offset < buffersize && buf[offset+UInt32(1)] == cq)
                    pos_to_check += UInt(1)
                    offset += UInt32(1)
                elseif byte_to_check == cq
                    quoted = false
                end
            else
                if byte_to_check == oq
                    quoted = true
                elseif byte_to_check == UInt8('\r')
                    if offset < buffersize && buf[offset+UInt32(1)] == UInt8('\n')
                        pos_to_check += UInt(1)
                        offset += UInt32(1)
                    end
                    push!(eols, offset)
                elseif byte_to_check == e
                    if (offset < buffersize && buf[offset+UInt32(1)] == oq)
                        pos_to_check += UInt(1)
                        offset += UInt32(1)
                    end
                else
                    push!(eols, offset)
                end
            end
            ptr += pos_to_check
            bytes_to_search -= pos_to_check
        end
    end

    @inbounds if reached_end_of_file
        quoted && (close(lexer_state.io); throw(UnmatchedQuoteError()))
        lexer_state.done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last_byte = bytes_carried_over_from_previous_chunk + bytes_read_in
        last(eols) < last_byte && push!(eols, last_byte + UInt32(1))
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
function readbytesall!(io::MmapStream, buf, n)
    bytes_to_read = min(bytesavailable(io), n)
    unsafe_copyto!(pointer(buf), pointer(io.x) + io.pos - 1, bytes_to_read)
    io.pos += bytes_to_read
    return UInt32(bytes_to_read)
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
