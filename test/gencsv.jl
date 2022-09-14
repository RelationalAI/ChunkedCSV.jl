using Random: randstring

csvrand(::Type{Int}, quotechar, escapechar, delim) = rand((1, 22, 333, 4444, 55555, 666666, 7777777, 88888888, 999999999, -1, -22, -333, -4444, -55555, -666666, -7777777, -88888888, -999999999))
csvrand(::Type{Float64}, quotechar, escapechar, delim) = rand((1.1, 22.2, 333.3, 4444.4, 55555.5, 666666.6, 7777777.7, 88888888.8, 999999999.9, -1.1, -22.2, -333.3, -4444.4, -55555.5, -666666.6, -7777777.7, -88888888.8, -999999999.9))
csvrand(::Type{String}, quotechar, escapechar, delim) = rand(Bool) ?
    string(quotechar, 'S', randstring(['\n', delim, 'a', 'z'], rand(4:27)), quotechar, escapechar, quotechar, escapechar, 'E', quotechar) :
    string('S', randstring(['a', 'z'], rand(4:27)), 'E')

function csvrand!(::Type{T}, buf::AbstractVector{String}, quotechar, escapechar, delim) where {T}
    if T === Int
        map!(x->string(csvrand(Int, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === Float64
        map!(x->string(csvrand(Float64, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === String
        map!(x->csvrand(String, quotechar, escapechar, delim), buf, 1:length(buf))
    end
end


function generatecsv(path, schema, rows, header=string.("COL_", 1:length(schema)+1), delim=',', quotechar='"', escapechar='"')
    iobuf = IOBuffer()
    max_task_size = min(rows, 32768)
    colbufs = [Vector{String}(undef, max_task_size) for _ in schema]
    open(path, "w") do io
        println(iobuf, join(header, delim))
        for rows_chunk in Iterators.partition(1:rows, max_task_size)
            @sync for col_ids in Iterators.partition(1:length(schema), 8)
                Threads.@spawn for col_id in col_ids
                    @inbounds csvrand!(schema[col_id], view(colbufs[col_id], 1:length(rows_chunk)), quotechar, escapechar, delim)
                end
            end
            @inbounds for (i, j) in zip(rows_chunk, 1:length(rows_chunk))
                write(iobuf, string(i))
                write(iobuf, delim)
                for (k, col) in enumerate(colbufs)
                    write(iobuf, col[j])
                    k < length(colbufs) && write(iobuf, delim)
                end
                write(iobuf, '\n')
            end
            seekstart(iobuf)
            write(io, iobuf)
            seekstart(iobuf)
            truncate(iobuf, length(rows_chunk))
        end
    end
    return vcat(Int, schema)
end
