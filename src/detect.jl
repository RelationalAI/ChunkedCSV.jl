# Common delimiters used in textual, delimited files
const CANDIDATE_DELIMS = (UInt8(','), UInt8('\t'), UInt8(' '), UInt8('|'), UInt8(';'), UInt8(':'))
const DEFAULT_LINES_TO_DETECT_DELIM = 10
@assert !(0xFF in CANDIDATE_DELIMS)

# Called when we do the initial skipping of rows -- we're either on header or the
# first row of data. TODO: Should we only do the header count here and continue after
# ALL skipping is done?
function _detect_delim(buf, pos, len, oq, cq, ec, has_header)
    len == 0 && return UInt8(',') # empty file
    @assert 1 <= pos <= len <= length(buf)

    byte_counts = zeros(UInt32, length(CANDIDATE_DELIMS))
    byte_counts_buf = zeros(UInt32, length(CANDIDATE_DELIMS))
    byte_counts_header = zeros(UInt32, length(CANDIDATE_DELIMS))
    b = 0xFF
    nlines = 0
    @inbounds while pos <= len && nlines < DEFAULT_LINES_TO_DETECT_DELIM
        b = buf[pos]
        pos += 1
        if b == oq
            while pos <= len
                b = buf[pos]
                pos += 1
                if b == ec # We're at an escape character, so we should skip over the next char
                    if pos > len
                        break
                    # In the ambiguos case where ec == cq, this is only an escapechar iff
                    # the next character is also a ec/cq. Otherwise it is an end of a string.
                    elseif ec == cq && buf[pos] != cq
                        break
                    end
                    # Skip over the char
                    b = buf[pos]
                    pos += 1
                elseif b == cq # end of string
                    break
                end
            end
        elseif b == UInt8('\n')
            nlines += 1
            for i in 1:length(CANDIDATE_DELIMS) # only count complete rows
                byte_counts[i] += byte_counts_buf[i]
                byte_counts_buf[i] = 0
            end
        elseif b == UInt8('\r')
            pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
            nlines += 1
            for i in 1:length(CANDIDATE_DELIMS)
                byte_counts[i] += byte_counts_buf[i]
                byte_counts_buf[i] = 0
            end
        else
            for (i, c) in enumerate(CANDIDATE_DELIMS)
                if b == c
                    if has_header && nlines == 0
                        byte_counts_header[i] += 1
                    end
                    byte_counts_buf[i] += 1
                    break
                end
            end
        end
    end
    # @info "byte_counts:        $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts))"
    # @info "byte_counts_buf:    $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts_buf))"
    # @info "byte_counts_header: $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts_header))"

    delim = 0xFF
    if nlines > 0
        # reuse byte_counts_buf to hold the delim order by count
        sortperm!(byte_counts_buf, byte_counts, rev=true)
        for i in byte_counts_buf
            delim_candidate = CANDIDATE_DELIMS[i]
            cnt = byte_counts[i]
            # The number of delim occurences is a multiple of the number of lines
            if cnt > 0 && cnt % nlines == 0
                delim = delim_candidate
                break
            end
        end
    else
        cnt, i = findmax(byte_counts_header)
        if cnt > 0
            delim = CANDIDATE_DELIMS[i]
        end
    end
    if delim == 0xFF
        delim = UInt8(',')
    end

    return delim
end

function _detect_newline(buf, pos, len)
    len == 0 && return UInt8('\n') # empty file
    @assert 1 <= pos <= len <= length(buf)

    v = view(buf, pos:len)
    if isnothing(findfirst(==(UInt8('\n')), v))
        if isnothing(findfirst(==(UInt8('\r')), v))
            throw(ArgumentError("No newline detected. Specify the newline character explicitly via the `newline` keyword argument. Use `\n` even for CRLF."))
        else
            return UInt8('\r')
        end
    else
        return UInt8('\n')
    end
end
