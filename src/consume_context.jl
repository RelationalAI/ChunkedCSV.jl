abstract type AbstractConsumeContext end
abstract type AbstractTaskLocalConsumeContext <: AbstractConsumeContext end

maybe_deepcopy(x::AbstractConsumeContext) = x
maybe_deepcopy(x::AbstractTaskLocalConsumeContext) = deepcopy(x)

struct DebugContext <: AbstractConsumeContext;
    n::Int

    DebugContext() = new(3)
    DebugContext(n::Int) = new(n)
end

function debug(x::BufferedVector{Parsers.PosLen}, i, parsing_ctx, consume_ctx)
    pl = x.elements[i]
    pl.missingvalue && return "missing"
    repr(String(parsing_ctx.bytes[pl.pos+1:pl.pos+pl.len]))
end
debug_eols(x::BufferedVector, i, parsing_ctx, consume_ctx) = x.elements[i]
function debug(x::BufferedVector{UInt32}, parsing_ctx, consume_ctx)
    eols = x.elements[1:min(consume_ctx.n+1, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(parsing_ctx.bytes[s+1:e-1])
    end
end


function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, consume_ctx::DebugContext) where {N}
    consume_ctx.n == 0 && return nothing
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
    write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.cols[1]), ", $(Base.current_task())\n"))
    write(io, "Row count by status: ")
    join(io, zip(RowStatus.Marks, status_counts), " | ")
    println(io)
    length(task_buf.row_statuses) == 0 && return nothing

    if status_counts[1] > 0
        c = 1
        println(io, "Ok ($(RowStatus.Marks[1])) rows:")
        for (k, (name, col)) in enumerate(zip(parsing_ctx.header, task_buf.cols))
            n = min(consume_ctx.n, status_counts[1])
            print(io, "\t$(name): [")
            for j in 1:length(task_buf.row_statuses)
                if task_buf.row_statuses[j] == 0x00
                    write(io, debug(col, j, parsing_ctx, consume_ctx))
                    n != 1 && print(io, ", ")
                elseif task_buf.row_statuses[j] == 0x01
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
        print(io, RowStatus.Names[i])
        print(io, " ($(RowStatus.Marks[i]))")
        println(io, " rows:")
        S = RowStatus.Flags[i]
        for (k, (name, col)) in enumerate(zip(parsing_ctx.header, task_buf.cols))
            c = 1
            n = min(consume_ctx.n, cnt)
            print(io, "\t$(name): [")
            for j in 1:length(task_buf.row_statuses)
                if task_buf.row_statuses[j] & S > 0
                    has_missing = isflagset(task_buf.row_statuses[j], 1) && isflagset(task_buf.column_indicators[c], k)
                    write(io,has_missing ? "?" : debug(col, j, parsing_ctx, consume_ctx))
                    n != 1 && print(io, ", ")
                    has_missing && (c += 1)
                end
                n -= 1
                n == 0 && break
            end
            print(io, "]\n")
        end
    end

    # println(io, "")
    # for (name, col) in zip(parsing_ctx.header, task_buf.cols)
    #     write(io, string(name, ": ", string(debug(task_buf.row_statuses, parsing_ctx, consume_ctx) .=> debug(col, parsing_ctx, consume_ctx)), '\n'))
    # end
    # write(io, string("Rows samples:\n\t", join(debug_eols(parsing_ctx.eols, parsing_ctx, consume_ctx), "\n\t")))
    @info String(take!(io))
    return nothing
end


struct SkipContext <: AbstractConsumeContext
    SkipContext() = new()
end
function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, consume_ctx::SkipContext) where {N}
    return nothing
end