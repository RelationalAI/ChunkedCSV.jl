function _parse_rows_forloop!(result_buf::TaskResultBuffer{N,M}, task::AbstractVector{UInt32}, buf, schema, options) where {N,M}
    empty!(result_buf)
    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))
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
        @inbounds for col_idx in 1:N
            type = schema[col_idx]
            if Parsers.eof(code)
                row_status = TooFewColumnsError
                skip_element!(getindex(result_buf.cols, col_idx))
                for _col_idx in col_idx+1:N
                    skip_element!(getindex(result_buf.cols, _col_idx))
                end
                break
            end
            if type === Int
                (;val, tlen, code) = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Int}, val)
            elseif type === Float64
                (;val, tlen, code) = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Float64}, val)
            elseif type === String
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options)::Parsers.Result{Parsers.PosLen}
                is_quoted = Parsers.quoted(code)
                unsafe_push!(getindex(result_buf.cols, col_idx)::BufferedVector{Parsers.PosLen}, Parsers.PosLen(prev_newline+pos+is_quoted, val.len-1))
            else
                row_status = UnknownTypeError
                break
            end
            if Parsers.invalid(code)
                row_status = ValueParsingError
                for _col_idx in col_idx+1:N
                    skip_element!(getindex(result_buf.cols, _col_idx))
                end
                break
            elseif Parsers.sentinel(code)
                row_status = HasMissing
                column_indicators |= M(1) << (col_idx - 1)
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code) && (row_status === NoMissing || row_status === HasMissing)
            row_status = TooManyColumnsError
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        !iszero(column_indicators) && push!(result_buf.column_indicators, column_indicators) # No inbounds as we're growing this buffer lazily
    end # for row_idx
    return nothing
end