abstract type AbstractConsumeContext end
abstract type AbstractTaskLocalConsumeContext <: AbstractConsumeContext end

function setup_tasks! end
function consume! end
function task_done! end
function sync_tasks end

maybe_deepcopy(x::AbstractConsumeContext) = x
maybe_deepcopy(x::AbstractTaskLocalConsumeContext) = deepcopy(x)

function setup_tasks!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    ntasks == 0 && return nothing # opt-out for :serial
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks = ntasks
    end
end
function task_done!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, result_buf::TaskResultBuffer{N,M}) where {N,M}
    result_buf.id == 0 && return nothing # opt-out for :serial
    @lock parsing_ctx.cond.cond_wait begin
        parsing_ctx.cond.ntasks -= 1
        notify(parsing_ctx.cond.cond_wait)
    end
end
function sync_tasks(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    ntasks == 0 && return nothing # opt-out for :serial
    @lock parsing_ctx.cond.cond_wait begin
        while true
            parsing_ctx.cond.ntasks == 0 && break
            wait(parsing_ctx.cond.cond_wait)
        end
    end
end

struct DebugContext <: AbstractConsumeContext
    error_only::Bool
    n::Int
    err_len::Int
    show_values::Bool

    DebugContext(error_only::Bool=true, n::Int=3, err_len::Int=255, show_values::Bool=false) = new(error_only, n, err_len, show_values)
end

function debug(x::BufferedVector{Parsers.PosLen}, i, parsing_ctx, consume_ctx)
    pl = x.elements[i]
    pl.missingvalue && return "missing"
    repr(String(parsing_ctx.bytes[pl.pos:pl.pos+pl.len-1]))
end
debug(x::BufferedVector, i, parsing_ctx, consume_ctx) = string(x.elements[i])
function debug_eols(x::BufferedVector{UInt32}, parsing_ctx, consume_ctx)
    eols = x.elements[1:min(consume_ctx.n+1, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(parsing_ctx.bytes[s+1:e-1])
    end
end


function consume!(consume_ctx::DebugContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{N,M}, row_num::UInt32, eol_idx::UInt32) where {N,M}
    status_counts = zeros(Int, length(RowStatus.Marks))
    io = IOBuffer()
    @inbounds for i in 1:length(task_buf.row_statuses)
        s = task_buf.row_statuses[i]
        status_counts[1] += s <= 0x01
        status_counts[2] += 0x01 & s > 0
        status_counts[3] += 0x02 & s > 0
        status_counts[4] += 0x04 & s > 0
        status_counts[5] += 0x08 & s > 0
        status_counts[6] += 0x10 & s > 0
    end
    write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.cols[1]), ", $(Base.current_task()) "))
    printstyled(IOContext(io, :color => true), "❚", color=Int(hash(Base.current_task()) % UInt8))
    println(io)
    anyerrs = sum(status_counts[3:end]) > 0
    if anyerrs
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
                        write(io, debug(col, j, parsing_ctx, consume_ctx))
                        n != 1 && print(io, ", ")
                    elseif task_buf.row_statuses[j] == RowStatus.HasColumnIndicators
                        write(io, isflagset(task_buf.column_indicators[c], k) ? "?" : debug(col, j, parsing_ctx, consume_ctx))
                        n != 1 && print(io, ", ")
                        c += 1
                    end
                    n -= 1
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
                        has_missing = isflagset(task_buf.row_statuses[j], 1) && isflagset(task_buf.column_indicators[c], k)
                        consume_ctx.show_values && write(io, has_missing ? "?" : debug(col, j, parsing_ctx, consume_ctx))
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
                s = parsing_ctx.eols[eol_idx + i - 1]+1
                e = parsing_ctx.eols[eol_idx + i]-1
                l = consume_ctx.err_len
                if e - s > l
                    println(io, repr(String(parsing_ctx.bytes[s:s+l-3])), "...")
                else
                    println(io, repr(String(parsing_ctx.bytes[s:e])))
                end
                errcnt += 1
                errcnt > consume_ctx.n && break
            end
        end
    end
    @info String(take!(io))
    return nothing
end


struct SkipContext <: AbstractConsumeContext
    SkipContext() = new()
end
function consume!(consume_ctx::SkipContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{N,M}, row_num::UInt32, eol_idx::UInt32) where {N,M}
    return nothing
end
