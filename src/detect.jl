# Common delimiters used in textual, delimited files
const CANDIDATE_DELIMS = (UInt8(','), UInt8(';'), UInt8('|'), UInt8(':'), UInt8('\t'), UInt8(' '))
const DEFAULT_LINES_TO_DETECT_DELIM = 11
@assert !(0xFF in CANDIDATE_DELIMS)

# Called when we do the initial skipping of rows -- we're either on header or the
# first row of data. TODO: Should we only do the header count here and continue after
# ALL skipping is done?
function _detect_delim(buf, pos, len, oq, cq, ec, has_header)
    len == 0 && return UInt8(',') # empty file
    @assert 1 <= pos <= len <= length(buf)

    byte_counts = zeros(UInt32, length(CANDIDATE_DELIMS))        # Total counts
    byte_counts_buf = zeros(UInt32, length(CANDIDATE_DELIMS))    # Counts in the current row
    byte_counts_header = zeros(UInt32, length(CANDIDATE_DELIMS)) # Counts in the header
    b = 0xFF
    nlines = 0
    buf_needs_reset = false
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
            if nlines == 0 && has_header # if there is a header, we can only use the delimiter that is unique to the header
                count(!iszero, byte_counts_header) == 1 && return CANDIDATE_DELIMS[findfirst(!iszero, byte_counts_header)]
            end
            nlines += 1
            for i in 1:length(CANDIDATE_DELIMS) # only count complete rows
                byte_counts[i] += byte_counts_buf[i]
            end
            buf_needs_reset = true
        elseif b == UInt8('\r')
            pos <= len && buf[pos] == UInt8('\n') && (pos += 1)
            nlines += 1
            for i in 1:length(CANDIDATE_DELIMS)
                byte_counts[i] += byte_counts_buf[i]
            end
            buf_needs_reset = true
        else
            buf_needs_reset && (byte_counts_buf .= 0)
            buf_needs_reset = false
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
    # @info "byte_counts ($nlines lines):  $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts))"
    # @info "byte_counts_buf:        $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts_buf))"
    # @info "byte_counts_header:     $(Char.(CANDIDATE_DELIMS) .=> Int.(byte_counts_header))"

    delim = 0xFF
    if nlines > 1
        curmax = 0
        @inbounds for (candidate, cnt_header, cnt_last, cnt_total) in zip(CANDIDATE_DELIMS, byte_counts_header, byte_counts_buf, byte_counts)
            cnt_total == 0 && continue
            has_header && cnt_header == 0 && continue
            # if the total count is a multiple of nlines, that's a point
            points = cnt_total % nlines == 0
            # if the total count is a multiple of last line count, that's a point
            points += (cnt_last > 0) && (cnt_total % cnt_last == 0)
            # if the total count is a multiple of header count, that's a point
            points += (cnt_header > 0) && (cnt_total % cnt_header == 0)
            # if the header count and the last count matches, thats a point
            points += (cnt_header > 0) && (cnt_header == cnt_last)
            if points > curmax
                curmax = points
                delim = candidate
            end
        end
    else # at most single complete line -- just take the most common delimiter candidate
        byte_counts .+= byte_counts_buf
        cnt, i = findmax(byte_counts)
        if cnt > 0
            delim = CANDIDATE_DELIMS[i]
        end
    end
    if delim == 0xFF
        delim = UInt8(',')
    end

    return delim
end
