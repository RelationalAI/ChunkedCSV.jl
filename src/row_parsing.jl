skip_commented_row!(result_buf::TaskResultBuffer{N,M}, row_bytes, comment::Nothing) where {N,M} = false
function skip_commented_row!(result_buf::TaskResultBuffer{N,M}, row_bytes, comment::Vector{UInt8}) where {N,M}
    if _startswith(row_bytes, comment)
        foreach(skip_element!, result_buf.cols)
        unsafe_push!(result_buf.row_statuses, RowStatus.SkippedRow)
        push!(result_buf.column_indicators::BufferedVector{M}, initflagset(M))
        return true
    end
    return false
end

function _parse_rows_forloop!(result_buf::TaskResultBuffer{N,M}, task::AbstractVector{UInt32}, buf, schema, options, comment::Union{Nothing,Vector{UInt8}}) where {N,M}
    empty!(result_buf)
    Base.ensureroom(result_buf, ceil(Int, length(task) * 1.01))
    for chunk_row_idx in 2:length(task)
        @inbounds prev_newline = task[chunk_row_idx - 1]
        @inbounds curr_newline = task[chunk_row_idx]
        # +1 -1 to exclude newline chars
        @inbounds row_bytes = view(buf, prev_newline+UInt32(1):curr_newline-UInt32(1))
        skip_commented_row!(result_buf, row_bytes, comment) && continue

        pos = 1
        len = length(row_bytes)
        # Empty lines are treated as having too few columns
        code = (curr_newline - prev_newline) == 1 ? Parsers.EOF : Int16(0)
        row_status = RowStatus.Ok
        column_indicators = initflag(M)
        cols = result_buf.cols
        @inbounds for col_idx in 1:N
            type = schema[col_idx]
            if Parsers.eof(code) && !(col_idx == N && Parsers.delimited(code))
                row_status |= RowStatus.TooFewColumns
                row_status |= RowStatus.HasColumnIndicators
                skip_element!(cols[col_idx])
                column_indicators = setflag(column_indicators, col_idx)
                for _col_idx in col_idx+1:N
                    skip_element!(getindex(cols, _col_idx))
                    column_indicators = setflag(column_indicators, _col_idx)
                end
                break
            end
            if type === Int
                (;val, tlen, code) = Parsers.xparse(Int, row_bytes, pos, len, options)::Parsers.Result{Int}
                unsafe_push!(cols[col_idx]::BufferedVector{Int}, val)
            elseif type === Bool
                (;val, tlen, code) = Parsers.xparse(Bool, row_bytes, pos, len, options)::Parsers.Result{Bool}
                unsafe_push!(cols[col_idx]::BufferedVector{Bool}, val)
            elseif type === Float64
                (;val, tlen, code) = Parsers.xparse(Float64, row_bytes, pos, len, options)::Parsers.Result{Float64}
                unsafe_push!(cols[col_idx]::BufferedVector{Float64}, val)
            elseif type === Dates.Date
                (;val, tlen, code) = Parsers.xparse(Dates.Date, row_bytes, pos, len, options)::Parsers.Result{Dates.Date}
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.Date}, val)
            elseif type === Dates.DateTime
                (;val, tlen, code) = Parsers.xparse(_GuessDateTime, row_bytes, pos, len, options, Dates.DateTime)::Parsers.Result{Dates.DateTime}
                unsafe_push!(cols[col_idx]::BufferedVector{Dates.DateTime}, val)
            elseif type === Char
                (;val, tlen, code) = Parsers.xparse(Char, row_bytes, pos, len, options)::Parsers.Result{Char}
                unsafe_push!(cols[col_idx]::BufferedVector{Char}, val)
            elseif type === String
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options)::Parsers.Result{Parsers.PosLen}
                unsafe_push!(cols[col_idx]::BufferedVector{Parsers.PosLen}, Parsers.PosLen(prev_newline+val.pos, val.len, val.missingvalue, val.escapedvalue))
            # TODO: We currently support only 8 digits after decimal point. We need to update Parsers.jl to accept runtime params, then we'd provide `f`
            #       param at runtime, and we'll only have to unroll on T (Int8,Int16,Int32,Int64,Int128)
            elseif type === FixedDecimal{Int8,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,0}}, val.x)
            elseif type === FixedDecimal{Int8,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,1}}, val.x)
            elseif type === FixedDecimal{Int8,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int8,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int8,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int8,2}}, val.x)
            elseif type === FixedDecimal{Int16,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,0}}, val.x)
            elseif type === FixedDecimal{Int16,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,1}}, val.x)
            elseif type === FixedDecimal{Int16,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,2}}, val.x)
            elseif type === FixedDecimal{Int16,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,3}}, val.x)
            elseif type === FixedDecimal{Int16,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int16,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int16,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int16,4}}, val.x)
            elseif type === FixedDecimal{Int32,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,0}}, val.x)
            elseif type === FixedDecimal{Int32,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,1}}, val.x)
            elseif type === FixedDecimal{Int32,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,2}}, val.x)
            elseif type === FixedDecimal{Int32,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,3}}, val.x)
            elseif type === FixedDecimal{Int32,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,4}}, val.x)
            elseif type === FixedDecimal{Int32,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,5}}, val.x)
            elseif type === FixedDecimal{Int32,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,6}}, val.x)
            elseif type === FixedDecimal{Int32,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,7}}, val.x)
            elseif type === FixedDecimal{Int32,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int32,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int32,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int32,8}}, val.x)
            elseif type === FixedDecimal{Int64,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,0}}, val.x)
            elseif type === FixedDecimal{Int64,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,1}}, val.x)
            elseif type === FixedDecimal{Int64,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,2}}, val.x)
            elseif type === FixedDecimal{Int64,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,3}}, val.x)
            elseif type === FixedDecimal{Int64,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,4}}, val.x)
            elseif type === FixedDecimal{Int64,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,5}}, val.x)
            elseif type === FixedDecimal{Int64,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,6}}, val.x)
            elseif type === FixedDecimal{Int64,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,7}}, val.x)
            elseif type === FixedDecimal{Int64,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int64,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int64,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int64,8}}, val.x)
            elseif type === FixedDecimal{Int128,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,0}}, val.x)
            elseif type === FixedDecimal{Int128,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,1}}, val.x)
            elseif type === FixedDecimal{Int128,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,2}}, val.x)
            elseif type === FixedDecimal{Int128,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,3}}, val.x)
            elseif type === FixedDecimal{Int128,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,4}}, val.x)
            elseif type === FixedDecimal{Int128,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,5}}, val.x)
            elseif type === FixedDecimal{Int128,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,6}}, val.x)
            elseif type === FixedDecimal{Int128,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,7}}, val.x)
            elseif type === FixedDecimal{Int128,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{Int128,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{Int128,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{Int128,8}}, val.x)

            elseif type === FixedDecimal{UInt8,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,0}}, val.x)
            elseif type === FixedDecimal{UInt8,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,1}}, val.x)
            elseif type === FixedDecimal{UInt8,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt8,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt8,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt8,2}}, val.x)
            elseif type === FixedDecimal{UInt16,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,0}}, val.x)
            elseif type === FixedDecimal{UInt16,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,1}}, val.x)
            elseif type === FixedDecimal{UInt16,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,2}}, val.x)
            elseif type === FixedDecimal{UInt16,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,3}}, val.x)
            elseif type === FixedDecimal{UInt16,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt16,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt16,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt16,4}}, val.x)
            elseif type === FixedDecimal{UInt32,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,0}}, val.x)
            elseif type === FixedDecimal{UInt32,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,1}}, val.x)
            elseif type === FixedDecimal{UInt32,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,2}}, val.x)
            elseif type === FixedDecimal{UInt32,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,3}}, val.x)
            elseif type === FixedDecimal{UInt32,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,4}}, val.x)
            elseif type === FixedDecimal{UInt32,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,5}}, val.x)
            elseif type === FixedDecimal{UInt32,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,6}}, val.x)
            elseif type === FixedDecimal{UInt32,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,7}}, val.x)
            elseif type === FixedDecimal{UInt32,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt32,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt32,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt32,8}}, val.x)
            elseif type === FixedDecimal{UInt64,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,0}}, val.x)
            elseif type === FixedDecimal{UInt64,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,1}}, val.x)
            elseif type === FixedDecimal{UInt64,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,2}}, val.x)
            elseif type === FixedDecimal{UInt64,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,3}}, val.x)
            elseif type === FixedDecimal{UInt64,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,4}}, val.x)
            elseif type === FixedDecimal{UInt64,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,5}}, val.x)
            elseif type === FixedDecimal{UInt64,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,6}}, val.x)
            elseif type === FixedDecimal{UInt64,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,7}}, val.x)
            elseif type === FixedDecimal{UInt64,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt64,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt64,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt64,8}}, val.x)
            elseif type === FixedDecimal{UInt128,0}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,0}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,0}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,0}}, val.x)
            elseif type === FixedDecimal{UInt128,1}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,1}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,1}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,1}}, val.x)
            elseif type === FixedDecimal{UInt128,2}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,2}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,2}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,2}}, val.x)
            elseif type === FixedDecimal{UInt128,3}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,3}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,3}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,3}}, val.x)
            elseif type === FixedDecimal{UInt128,4}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,4}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,4}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,4}}, val.x)
            elseif type === FixedDecimal{UInt128,5}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,5}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,5}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,5}}, val.x)
            elseif type === FixedDecimal{UInt128,6}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,6}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,6}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,6}}, val.x)
            elseif type === FixedDecimal{UInt128,7}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,7}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,7}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,7}}, val.x)
            elseif type === FixedDecimal{UInt128,8}
                (;val, tlen, code) = Parsers.xparse(_FixedDecimal{UInt128,8}, row_bytes, pos, len, options)::Parsers.Result{_FixedDecimal{UInt128,8}}
                unsafe_push!(cols[col_idx]::BufferedVector{FixedDecimal{UInt128,8}}, val.x)
            else
                row_status |= RowStatus.UnknownTypeError
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
                skip_element!(cols[col_idx])
                (;val, tlen, code) = Parsers.xparse(String, row_bytes, pos, len, options)::Parsers.Result{Parsers.PosLen}
                # NOTE: Trying out parsing as much as possible now
                # for _col_idx in col_idx+1:N
                #     skip_element!(getindex(cols, _col_idx))
                #     column_indicators = setflag(column_indicators, _col_idx)
                # end
                # break
            end
            if Parsers.sentinel(code)
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
            elseif !Parsers.ok(code)
                row_status |= RowStatus.ValueParsingError
                row_status |= RowStatus.HasColumnIndicators
                column_indicators = setflag(column_indicators, col_idx)
                # NOTE: Trying out parsing as much as possible now
                # for _col_idx in col_idx+1:N
                #     skip_element!(getindex(cols, _col_idx))
                #     column_indicators = setflag(column_indicators, _col_idx)
                # end
                # break
            end
            pos += tlen
        end # for col_idx
        if !Parsers.eof(code)
            row_status |= RowStatus.TooManyColumns
            row_status |= RowStatus.HasColumnIndicators
            column_indicators = setflag(column_indicators, N)
        end
        unsafe_push!(result_buf.row_statuses, row_status)
        # TODO: replace anyflagset with a local variable?
        anyflagset(column_indicators) && push!(result_buf.column_indicators::BufferedVector{M}, column_indicators) # No inbounds as we're growing this buffer lazily
    end # for row_idx
    return nothing
end
