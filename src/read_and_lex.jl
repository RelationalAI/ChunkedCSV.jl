readbytesall!(io::IO, buf, n) = UInt32(Base.readbytes!(io, buf, n; all = true))
readbytesall!(io::IOBuffer, buf, n) = UInt32(Base.readbytes!(io, buf, n))
function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_chunk_newline_at)
    ptr = pointer(buf)
    buffersize = UInt32(length(buf))
    if last_chunk_newline_at == 0 # this is the first time we saw the buffer, we'll just fill it up
        bytes_read_in = readbytesall!(io, buf, buffersize)
    elseif last_chunk_newline_at < buffersize
        # We'll keep the bytes that are past the last newline, shifting them to the left
        # and refill the rest of the buffer.
        unsafe_copyto!(ptr, ptr + last_chunk_newline_at, buffersize - last_chunk_newline_at)
        bytes_read_in = @inbounds readbytesall!(io, @view(buf[buffersize - last_chunk_newline_at + 1:end]), last_chunk_newline_at)
        bytes_read_in += buffersize - last_chunk_newline_at
    else
        # Last chunk was consumed entirely
        bytes_read_in = readbytesall!(io, buf, buffersize)
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
# TODO: '\n\r' currently produces 2 newlines... but we skip empty lines, so no biggie?
findmark(ptr, bytes_to_search, ::Val{B}) where B = something(memchr(ptr, bytes_to_search, B), zero(UInt))
function lex_newlines_in_buffer(io::IO, parsing_ctxfs::ParsingContext, options, byteset::Val{B}, bytes_read_in::UInt32, quoted::Bool) where B
    ptr = pointer(parsing_ctxfs.bytes) # We never resize the buffer, the array shouldn't need to relocate
    e, q = options.e, options.oq
    buf = parsing_ctxfs.bytes
    eols = parsing_ctxfs.eols
    iseof = eof(io)

    bytes_to_search = UInt(bytes_read_in)
    if iseof
        # If the file is smaller than the buffer, don't want to skip anything
        offset = UInt32(0)
    else
        # First length(buf) - bytes_read_in bytes we've already seen in the previous round
        offset = UInt32(length(buf)) - bytes_read_in
        ptr += offset
    end
    @inbounds while bytes_to_search > UInt(0)
        pos_to_check = findmark(ptr, bytes_to_search, byteset)
        offset += UInt32(pos_to_check)
        if pos_to_check == UInt(0)
            if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !iseof
                close(io)
                error("CSV parse job failed on lexing newlines. There was no linebreak in the entire buffer of $(length(buf)) bytes. \n")
            end
            break
        else
            byte_to_check = buf[offset]
            if quoted
                if byte_to_check == e && get(buf, offset+UInt32(1), 0xFF) == q
                    pos_to_check += UInt(1)
                    offset += UInt32(1)
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
            bytes_to_search -= pos_to_check
        end
    end

    @inbounds if iseof
        # quoted && (close(io); error("CSV parse job failed on lexing newlines. There file has ended with an unmatched quote."))
        done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last(eols) != bytes_read_in && push!(eols, bytes_read_in + UInt32(1))
        last_chunk_newline_at = bytes_read_in
    else
        done = false
        last_chunk_newline_at = last(eols)
    end
    return last_chunk_newline_at, quoted, done
end
function lex_newlines_in_buffer(io::NoopStream, parsing_ctxfs::ParsingContext, options::Parsers.Options, byteset::Val{B}, bytes_to_search::UInt32, quoted::Bool) where B
    return lex_newlines_in_buffer(io.stream, parsing_ctxfs, options, byteset, bytes_to_search, quoted)
end

_input_to_io(input::IO) = input
function _input_to_io(input::String)
    io = NoopStream(open(input, "r"))
    TranscodingStreams.changemode!(io, :read)
    return io
end