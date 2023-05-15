module ConsumeContexts

using ..ChunkedCSV: TaskResultBuffer, ParsingContext, BufferedVector, RowStatus, ChunkedCSV, isflagset, MIN_TASK_SIZE_IN_BYTES
using ..ChunkedCSV: set!, dec!
import Parsers

export AbstractConsumeContext, DebugContext, SkipContext, TestContext
export setup_tasks!, consume!, task_done!, sync_tasks, cleanup
export insertsorted!

abstract type AbstractConsumeContext end

"""
    consume!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{M}, row_num::Int, eol_idx::Int32) where {M}

Override with your `AbstractConsumeContext` to provide a custom logic for processing the parsed results in `TaskResultBuffer`.
The method is called from multiple tasks in parallel, just after each corresponding `task_buf` has been populated.
`task_buf` is only filled once per chunk.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`setup_tasks!`](@ref), [`sync_tasks`](@ref), [`cleanup`](@ref), [`TaskResultBuffer`](@ref)
"""
function consume! end
function setup_tasks! end
function task_done! end
function sync_tasks end
function cleanup end

"""
    setup_tasks!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)

Set the number of units of work that the parser/consume tasks need to finish
(and report back as done via e.g. `task_done!`) before the current chunk of input data
is considered to be entirely processed.

This function is called just after the we're done detecting newline positions in the current
chunk of data and we are about to submit partitions of the detected newlines to the parse/consume tasks.

`ntasks` is between 1 and two times the `nworkers` agrument to `parse_file`, depeneding on
the size of the input. Most of the time, the value is `2*nworkers` is used, but for smaller
buffer sizes, smaller files or when handling the last bytes of the file, `ntasks` will be
smaller as we try to ensure the minimal average tasks size if terms of bytes of input is at least
$(Base.format_bytes(MIN_TASK_SIZE_IN_BYTES)). For `:serial` parsing mode, `ntasks` is always 1.

You should override this method when you further subdivide the amount of concurrent work on the chunk,
e.g. when you want to process each column separately in `@spawn` tasks, in which case you'd expect
there to be `ntasks * (1 + length(parsing_ctx.schema))` units of work per chunk
(in which case you'd have to manually call `task_done!` in the column-processing tasks).

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`sync_tasks`](@ref), [`cleanup`](@ref)
"""
function setup_tasks!(::AbstractConsumeContext, parsing_ctx::ParsingContext, ntasks::Int)
    # TRACING # parsing_ctx.id == 1 ? push!(ChunkedCSV.T1, time_ns()) : push!(ChunkedCSV.T2, time_ns())
    set!(parsing_ctx.counter, ntasks)
    return nothing
end

"""
    task_done!(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext)

Decrement the expected number of remaining work units by one. Called after each `consume!` call.

The this function should be called `ntasks` times where `ntasks` comes from the corresponding
`setup_tasks!` call.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`sync_tasks`](@ref), [`cleanup`](@ref)
"""
function task_done!(::AbstractConsumeContext, parsing_ctx::ParsingContext)
    dec!(parsing_ctx.counter, 1)
    return nothing
end

# TODO: This probably shouldn't be public.
# TODO: If we want to support schema inference, this would be a good place to sync a `Vector{TaskResultBuffer}` belonging to current `parsing_ctx`
"""
    sync_tasks(consume_ctx::AbstractConsumeContext, parsing_ctx::ParsingContext)

Wait for all parse/consume tasks to report all expected units of work to be done.
Called after all work for the current chunk has been submitted to the parser/consume tasks
and we're about to refill it.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`cleanup`](@ref)
"""
function sync_tasks(::AbstractConsumeContext, parsing_ctx::ParsingContext)
    wait(parsing_ctx.counter)
    # TRACING # parsing_ctx.id == 1 ? push!(ChunkedCSV.T1, time_ns()) : push!(ChunkedCSV.T2, time_ns()) # TRACING
    return nothing
end
"""
    cleanup(consume_ctx::AbstractConsumeContext, e::Exception)

You can override this method do custom exception handling with your consume context.
This method is called immediately before a `rethrow()` which forwards the `e` further.

See also [`consume!`](@ref), [`setup_tasks!`](@ref), [`task_done!`](@ref), [`sync_tasks`](@ref)
"""
cleanup(::AbstractConsumeContext, ::Exception) = nothing

struct DebugContext <: AbstractConsumeContext
    error_only::Bool
    n::Int
    err_len::Int
    show_values::Bool

    DebugContext(error_only::Bool=true, n::Int=3, err_len::Int=255, show_values::Bool=false) = new(error_only, n, err_len, show_values)
end

function debug(x::BufferedVector{Parsers.PosLen31}, i, parsing_ctx, consume_ctx)
    pl = x.elements[i]
    pl.missingvalue && return "missing"
    repr(Parsers.getstring(parsing_ctx.bytes, pl, parsing_ctx.escapechar))
end
debug(x::BufferedVector, i, parsing_ctx, consume_ctx) = string(x.elements[i])
function debug_eols(x::BufferedVector{Int32}, parsing_ctx, consume_ctx)
    eols = x.elements[1:min(consume_ctx.n+1, x.occupied)]
    return map(zip(eols[1:end-1], eols[2:end])) do (i)
        (s,e) = i
        s+1:e-1 => String(parsing_ctx.bytes[s+1:e-1])
    end
end

function consume!(consume_ctx::DebugContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{M}, row_num::Int, eol_idx::Int32) where {M}
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
                        write(io, debug(col, j, parsing_ctx, consume_ctx))
                        n != 1 && print(io, ", ")
                        n -= 1
                    elseif task_buf.row_statuses[j] == RowStatus.HasColumnIndicators
                        write(io, k in task_buf.column_indicators[c] ? "?" : debug(col, j, parsing_ctx, consume_ctx))
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
                        has_missing = task_buf.row_statuses[j] > RowStatus.Ok && isflagset(task_buf.column_indicators[c], k)
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
function consume!(consume_ctx::SkipContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{M}, row_num::Int, eol_idx::Int32) where {M}
    return nothing
end

function insertsorted!(arr::Vector{T}, x::T, by=identity) where {T}
    idx = searchsortedfirst(arr, x, by=by)
    insert!(arr, idx, x)
    return idx
end

struct TestContext <: AbstractConsumeContext
    results::Vector{TaskResultBuffer}
    strings::Vector{Vector{Vector{String}}}
    header::Vector{Symbol}
    schema::Vector{DataType}
    lock::ReentrantLock
    rownums::Vector{Int}
end
TestContext() = TestContext([], [], [], [], ReentrantLock(), [])
function consume!(ctx::TestContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{M}, row_num::Int, eol_idx::Int32) where {M}
    strings = Vector{String}[]
    for col in task_buf.cols
        if eltype(col) === Parsers.PosLen31
            push!(strings, [Parsers.getstring(parsing_ctx.bytes, x, parsing_ctx.escapechar) for x in col::BufferedVector{Parsers.PosLen31}])
        else
            push!(strings, String[])
        end
    end
    Base.@lock ctx.lock begin
        isempty(ctx.header) && append!(ctx.header, copy(parsing_ctx.header))
        isempty(ctx.schema) && append!(ctx.schema, copy(parsing_ctx.schema))
        idx = insertsorted!(ctx.rownums, row_num)
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

end # module
