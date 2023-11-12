# A consume context that prints out simple debug information about the parsed chunks
struct DebugContext <: AbstractConsumeContext
    error_only::Bool  # whether we should only print errored rows
    n::Int            # number of rows valid rows to print when `error_only is false`
    err_len::Int      # number of bytes to print for errored rows
    show_values::Bool # whether we should print the parsed values for errored rows

    DebugContext(error_only::Bool=true, n::Int=3, err_len::Int=255, show_values::Bool=false) = new(error_only, n, err_len, show_values)
end

function debug(x::BufferedVector{Parsers.PosLen31}, i, parsing_ctx, consume_ctx, chunking_ctx)
    pl = x.elements[i]
    pl.missingvalue && return "missing"
    repr(Parsers.getstring(chunking_ctx.bytes, pl, parsing_ctx.escapechar))
end
debug(x::BufferedVector, i, parsing_ctx, consume_ctx, chunking_ctx) = string(x.elements[i])
function debug_eols(x::BufferedVector{Int32}, parsing_ctx, consume_ctx, chunking_ctx)
    eols = x.elements[1:min(consume_ctx.n+1, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(chunking_ctx.bytes[s+1:e-1])
    end
end

function ChunkedBase.consume!(consume_ctx::DebugContext, payload::ParsedPayload)
    parsing_ctx = payload.parsing_ctx
    chunking_ctx = payload.chunking_ctx
    task_buf  = payload.results
    row_num = payload.row_num
    eol_idx = payload.eols_buffer_index
    status_counts = zeros(Int, length(RowStatus.Marks))
    io = IOBuffer()
    @inbounds for i in 1:length(task_buf.row_statuses)
        s = task_buf.row_statuses[i]
        status_counts[1] += s <= 0x01
        for (j, f) in enumerate(RowStatus.Flags[2:end])
            status_counts[j + 1] += f & s > 0
        end
    end
    write(io, string("Start row: ", row_num, ", nrows: ", isempty(task_buf.cols) ? 0 : length(task_buf.cols[1]), ", $(Base.current_task()) "))
    printstyled(IOContext(io, :color => true), "❚", color=Int(hash(Base.current_task()) % UInt8))
    println(io)
    anyerrs = sum(status_counts[3:end]) > 0
    if anyerrs || !consume_ctx.error_only
        write(io, "Row count by status: ")
        join(io, zip(RowStatus.Marks, status_counts), " | ")
        println(io)
    end
    if consume_ctx.n > 0 && length(task_buf.row_statuses) > 0
        if !consume_ctx.error_only && status_counts[1] > 0
            c = 1
            println(io, "Ok ($(RowStatus.Marks[1])) rows:")
            for (k, (name, col)) in enumerate(zip(parsing_ctx.header, task_buf.cols))
                n = min(consume_ctx.n, status_counts[1])
                print(io, "\t$(name): [")
                for j in 1:length(task_buf.row_statuses)
                    if task_buf.row_statuses[j] == RowStatus.Ok
                        write(io, debug(col, j, parsing_ctx, consume_ctx, chunking_ctx))
                        n != 1 && print(io, ", ")
                        n -= 1
                    elseif task_buf.row_statuses[j] == RowStatus.HasColumnIndicators
                        write(io, k in task_buf.column_indicators[c] ? "?" : debug(col, j, parsing_ctx, consume_ctx, chunking_ctx))
                        n != 1 && print(io, ", ")
                        c += 1
                        n -= 1
                    end
                    n == 0 && break
                end
                print(io, "]\n")
            end
        end

        i = 2
        for cnt in status_counts[3:end]
            i += 1
            cnt == 0 && continue
            consume_ctx.show_values && print(io, RowStatus.Names[i])
            consume_ctx.show_values && print(io, " ($(RowStatus.Marks[i]))")
            consume_ctx.show_values && println(io, " rows:")
            S = RowStatus.Flags[i]
            for (k, (name, col)) in enumerate(zip(parsing_ctx.header, task_buf.cols))
                c = 1
                n = min(consume_ctx.n, cnt)
                consume_ctx.show_values && print(io, "\t$(name): [")
                for j in 1:length(task_buf.row_statuses)
                    if (task_buf.row_statuses[j] & S) > 0
                        has_missing = task_buf.row_statuses[j] > RowStatus.Ok && task_buf.column_indicators[c, k]
                        consume_ctx.show_values && write(io, has_missing ? "?" : debug(col, j, parsing_ctx, consume_ctx, chunking_ctx))
                        consume_ctx.show_values && n != 1 && print(io, ", ")
                        has_missing && (c += 1)
                        n -= 1
                    end
                    n == 0 && break
                end
                consume_ctx.show_values && print(io, "]\n")
            end
        end
    end
    errcnt = 0
    if anyerrs
        println(io, "Example rows with errors:")
        for i in 1:length(task_buf.row_statuses)
            if Int(task_buf.row_statuses[i]) >= 2
                write(io, "\t($(row_num+i-1)): ")
                s = chunking_ctx.newline_positions[eol_idx + i - 1]+1
                e = chunking_ctx.newline_positions[eol_idx + i]-1
                l = consume_ctx.err_len
                if e - s > l
                    println(io, repr(String(chunking_ctx.bytes[s:s+l-3])), "...")
                else
                    println(io, repr(String(chunking_ctx.bytes[s:e])))
                end
                errcnt += 1
                errcnt > consume_ctx.n && break
            end
        end
    end
    @info String(take!(io))
    return nothing
end

# Used in tests to collect the results in sorted order
# and to materialize the strings for each column
struct TestContext <: AbstractConsumeContext
    results::Vector{TaskResultBuffer}
    strings::Vector{Vector{Vector{String}}}
    header::Vector{Symbol}
    schema::Vector{DataType}
    lock::ReentrantLock
    rownums::Vector{Int}
end
TestContext() = TestContext([], [], [], [], ReentrantLock(), [])
function ChunkedBase.consume!(ctx::TestContext, payload::ParsedPayload)
    parsing_ctx = payload.parsing_ctx
    chunking_ctx = payload.chunking_ctx
    task_buf  = payload.results
    cols = task_buf.cols
    row_num = payload.row_num
    strings = Vector{String}[]
    @inbounds for (i, T) in enumerate(parsing_ctx.schema)
        str_col = String[]
        push!(strings, str_col)
        if T === Parsers.PosLen31
            col_iter = ColumnIterator(cols[i]::BufferedVector{Parsers.PosLen31}, i, task_buf.row_statuses, task_buf.column_indicators)
            for (value, isinvalidrow, ismissingvalue) in col_iter
                if ismissingvalue
                    push!(str_col, "")
                else
                    push!(str_col, Parsers.getstring(chunking_ctx.bytes, value, parsing_ctx.escapechar))
                end
            end
        end
    end
    Base.@lock ctx.lock begin
        isempty(ctx.header) && append!(ctx.header, copy(parsing_ctx.header))
        isempty(ctx.schema) && append!(ctx.schema, copy(parsing_ctx.schema))
        idx = ChunkedBase.insertsorted!(ctx.rownums, row_num)
        insert!(ctx.results, idx, deepcopy(task_buf))
        insert!(ctx.strings, idx, strings)
    end
    return nothing
end

Base.empty!(ctx::TestContext) = (empty!(ctx.results); empty!(ctx.strings); empty!(ctx.header); empty!(ctx.schema); empty!(ctx.rownums); ctx)

function Base.collect(testctx::TestContext)
    init = [Vector{T}() for T in testctx.schema]
    vals = [
        [
            T === String ? s[i] : r.cols[i]
            for (i, T)
            in enumerate(testctx.schema)
        ]
        for (s, r)
        in zip(testctx.strings, testctx.results)
    ]
    (; zip(testctx.header, reduce((x,y)-> append!.(x, y), vals, init=init))...)
end
