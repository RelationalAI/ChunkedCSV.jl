abstract type AbstractConsumeContext end

# TODO: Investigate when we hit the lock
struct DebugContext <: AbstractConsumeContext;
    n::Int
    status_counts::Vector{Int}
    lock::ReentrantLock

    DebugContext() = new(10, zeros(Int, length(Base.Enums.namemap(ChunkedCSV.RowStatus))), ReentrantLock())
    DebugContext(n::Int) = new(n, zeros(Int, length(Base.Enums.namemap(ChunkedCSV.RowStatus))), ReentrantLock())
end

function debug(x::BufferedVector{Parsers.PosLen}, parsing_ctx, consume_ctx)
    pls = x.elements[1:min(consume_ctx.n, x.occupied)]
    return map(pl->String(parsing_ctx.bytes[pl.pos:pl.pos+pl.len]), pls)
end
debug(x::BufferedVector, parsing_ctx, consume_ctx) = x.elements[1:min(consume_ctx.n, x.occupied)]
function debug(x::BufferedVector{UInt32}, parsing_ctx, consume_ctx)
    eols = x.elements[1:min(consume_ctx.n+1, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(parsing_ctx.bytes[s+1:e-1])
    end
end

function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, consume_ctx::DebugContext) where {N}
    @lock consume_ctx.lock begin
        consume_ctx.n == 0 && return nothing
        io = IOBuffer()
        @inbounds for i in 1:length(task_buf.row_statuses)
            consume_ctx.status_counts[Int(task_buf.row_statuses[i]) + 1] += 1
        end
        write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.cols[1]), ", $(Base.current_task())\n"))
        write(io, "Statuses: ")
        join(io, zip(['✓', '?', '<', 'T', '!', '>'], consume_ctx.status_counts), " | ")
        println(io)
        for (name, col) in zip(parsing_ctx.header, task_buf.cols)
            write(io, string(name, ": ", string(debug(task_buf.row_statuses, parsing_ctx, consume_ctx) .=> debug(col, parsing_ctx, consume_ctx)), '\n'))
        end
        write(io, string("Rows samples:\n\t", join(debug(parsing_ctx.eols, parsing_ctx, consume_ctx), "\n\t")))
        @info String(take!(io))
        consume_ctx.status_counts .= 0
        return nothing
    end
end


struct SkipContext <: AbstractConsumeContext
    lock::ReentrantLock
    SkipContext() = new(ReentrantLock())
end
function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, consume_ctx::SkipContext) where {N}
    @lock consume_ctx.lock return nothing
end