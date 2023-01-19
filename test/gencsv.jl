using Random: randstring
using Dates
using TimeZones
using FixedPointDecimals

csvrand(::Type{DateTime}, quotechar, escapechar, delim) = rand(("9999-12-31T23:59:59.999", "0100-01-01T00:00:00", "9999-12-31T23:59:59.999UTC", "0100-01-01T00:00:00+0700", "0100-01-01T00:00:00Z"))
csvrand(::Type{Date}, quotechar, escapechar, delim) = rand((Date(100, 1, 1), Date(9999, 12, 31), Date(2020, 2, 29)))
csvrand(::Type{Int}, quotechar, escapechar, delim) = rand((typemin(Int64), typemax(Int64), 0, -1, 1))
csvrand(::Type{Bool}, quotechar, escapechar, delim) = rand(Bool)
csvrand(::Type{Char}, quotechar, escapechar, delim) = rand('a':'z')
csvrand(::Type{Float64}, quotechar, escapechar, delim) = rand((typemin(Float64), typemax(Float64), nextfloat(typemin(Float64)), prevfloat(typemax(Float64)), 0.0, -1.0, 1.0))
csvrand(::Type{FixedDecimal{Int64,4}}, quotechar, escapechar, delim) = rand(("99.9999", "999999e-4", "0.009999991111e4", "-99.9999", "-999999e-4", "-0.009999991111e4"))
csvrand(::Type{String}, quotechar, escapechar, delim) = rand(Bool) ?
    string(quotechar, 'S', randstring(['\n', delim, 'a', 'z'], rand(2:27)), escapechar, quotechar, escapechar, quotechar, 'E', quotechar) :
    string('S', randstring(['a', 'z'], rand(4:27)), 'E')


csvrand(::Type{String}, quotechar, escapechar, delim) = rand(Bool) ?
    string(quotechar, 'S', randstring(['\n', delim, 'a', 'z'], rand(2:1000000)), escapechar, quotechar, escapechar, quotechar, 'E', quotechar) :
    string('S', randstring(['a', 'z'], rand(4:1000000)), 'E')

csvrand(::Type{String}, quotechar, escapechar, delim) = rand(Bool) ?
    string(quotechar, 'S', randstring([escapechar], rand(2:2:1000000)), escapechar, quotechar, escapechar, quotechar, 'E', quotechar) :
    string('S', randstring(['a', 'z'], rand(4:1000000)), 'E')
# csvrand(::Type{String}, quotechar, escapechar, delim) = string('S', randstring(['a', 'z'], rand(4:27)), 'E')

function csvrand!(::Type{T}, buf::AbstractVector{String}, quotechar, escapechar, delim) where {T}
    if T === Int
        map!(x->string(csvrand(Int, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === Float64
        map!(x->string(csvrand(Float64, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === String
        map!(x->csvrand(String, quotechar, escapechar, delim), buf, 1:length(buf))
    elseif T === Date
        map!(x->string(csvrand(Date, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === FixedDecimal{Int64,4}
        map!(x->csvrand(FixedDecimal{Int64,4}, quotechar, escapechar, delim), buf, 1:length(buf))
    elseif T === DateTime
        map!(x->csvrand(DateTime, quotechar, escapechar, delim), buf, 1:length(buf))
    elseif T === Bool
        map!(x->string(csvrand(Bool, quotechar, escapechar, delim)), buf, 1:length(buf))
    elseif T === Char
        map!(x->string(csvrand(Char, quotechar, escapechar, delim)), buf, 1:length(buf))
    end
end

function generatecsv(path, schema, rows; header=string.("COL_", 1:length(schema)+1), delim=',', quotechar='"', escapechar='"')
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
