abstract type AbstractConsumeContext end

const DEBUG_ELEMENTS = 10
struct DebugContext <: AbstractConsumeContext; end
function debug(x::BufferedVector{Parsers.PosLen}, ctx)
    pls = x.elements[1:min(DEBUG_ELEMENTS, x.occupied)]
    return map(pl->String(ctx.bytes[pl.pos:pl.pos+pl.len]), pls)
end
debug(x::BufferedVector, ctx) = x.elements[1:min(DEBUG_ELEMENTS, x.occupied)]
function debug(x::BufferedVector{UInt32}, ctx)
    eols = x.elements[1:min(DEBUG_ELEMENTS, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(ctx.bytes[s+1:e-1])
    end
end

function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, context::DebugContext) where {N}
    io = IOBuffer()
    write(io, string("Start row: ", row_num, ", nrows: ", length(task_buf.cols[1]), ", $(Base.current_task())\n"))
    for (name, col) in zip(parsing_ctx.header, task_buf.cols)
        write(io, string(name, ": ", string(debug(task_buf.row_statuses, parsing_ctx) .=> debug(col, parsing_ctx)), '\n'))
    end
    write(io, string("Rows samples:\n\t", join(debug(parsing_ctx.eols, parsing_ctx), "\n\t")))
    @info String(take!(io))
    return nothing
end


struct SkipContext <: AbstractConsumeContext; end
function consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, context::SkipContext) where {N}
    return nothing
end