readbytesall!(io::IOStream, buf, n::Int) = Base.readbytes!(io, buf, n; all = true)
readbytesall!(io::IO, buf, n::Int) = Base.readbytes!(io, buf, n)
function prepare_buffer!(io::IO, buf::Vector{UInt8}, last_newline_at::Int)
    # TRACING # push!(IO_TASK_TIMES, time_ns())
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
    # TRACING # push!(IO_TASK_TIMES, time_ns())
    return bytes_read_in
end

function check_any_valid_rows(lexer, parsing_ctx)
    eols = parsing_ctx.eols
    if (length(eols) == 0 || (length(eols) == 1 && first(eols) == 0)) && !eof(lexer.io) # TODO: check done instead of eof?
        close(lexer.io)
        throw(NoValidRowsInBufferError(length(parsing_ctx.bytes)))
    end
end

function handle_file_end!(lexer::Lexer, eols, end_pos)
    @inbounds if eof(lexer.io)
        # If the file ended with an unmatched quote, we throw an error
        !NewlineLexers.possibly_not_in_string(lexer) && (close(lexer.io); throw(UnmatchedQuoteError()))
        lexer.done = true
        # Insert a newline at the end of the file if there wasn't one
        # This is just to make `eols` contain both start and end `pos` of every single line
        last(eols) < end_pos && push!(eols, unsafe_trunc(Int32, end_pos) + Int32(1))
    end
end

function limit_eols!(parsing_ctx::ParsingContext, row_num::Int)
    parsing_ctx.limit == 0 && return false
    if row_num > parsing_ctx.limit
        return true
    elseif row_num <= parsing_ctx.limit < row_num + length(parsing_ctx.eols) - 1
        parsing_ctx.eols.occupied -= (row_num + length(parsing_ctx.eols) - 1) - parsing_ctx.limit - 1
    end
    return false
end

function read_and_lex!(lexer::Lexer, parsing_ctx::ParsingContext, last_newline_at=Int(last(parsing_ctx.eols)))
    @assert !lexer.done
    empty!(parsing_ctx.eols)
    push!(parsing_ctx.eols, Int32(0))
    if eof(lexer.io) # Catches the empty input case
        lexer.done = true
        return nothing
    end

    bytes_read_in = prepare_buffer!(lexer.io, parsing_ctx.bytes, last_newline_at)

    # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
    start_pos = last_newline_at == 0 ? 1 : length(parsing_ctx.bytes) - last_newline_at + 1
    end_pos = start_pos + bytes_read_in - 1
    find_newlines!(lexer, parsing_ctx.bytes, parsing_ctx.eols, start_pos, end_pos)

    handle_file_end!(lexer, parsing_ctx.eols, end_pos)
    check_any_valid_rows(lexer, parsing_ctx)

    # TRACING #  push!(LEXER_TASK_TIMES, time_ns())
    return nothing
end

mutable struct MmapStream <: IO
    ios::IOStream
    x::Vector{UInt8}
    pos::Int
end
MmapStream(ios::IO) = MmapStream(ios, Mmap.mmap(ios, grow=false, shared=false), 1)
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
Base.filesize(io::MmapStream) = length(io.x)
function Base.unsafe_read(from::MmapStream, p::Ptr{UInt8}, nb::UInt)
    avail = bytesavailable(from)
    adv = min(avail, nb)
    GC.@preserve from unsafe_copyto!(p, pointer(from.x) + from.pos - 1, adv)
    from.pos += adv
    if nb > avail
        throw(EOFError())
    end
    return nothing
end
function Base.read(io::MmapStream, ::Type{UInt8})
    avail = bytesavailable(io)
    if avail == 0
        throw(EOFError())
    end
    io.pos += 1
    return io.x[io.pos]
end

_input_to_io(input::IO, use_mmap::Bool) = false, input
function _input_to_io(input::String, use_mmap::Bool)
    ios = open(input, "r")
    if !eof(ios) && peek(ios, UInt16) == 0x8b1f
        # TODO: GzipDecompressorStream doesn't respect MmapStream reaching EOF for some reason
        # io = CodecZlibNG.GzipDecompressorStream(use_mmap ? MmapStream(ios) : ios)
        use_mmap && @warn "`use_mmap=true` is currently unsupported when reading gzipped files, using file io."
        io = CodecZlibNG.GzipDecompressorStream(ios)
    elseif use_mmap
        io = MmapStream(ios)
    else
        io = ios
    end
    return true, io
end
