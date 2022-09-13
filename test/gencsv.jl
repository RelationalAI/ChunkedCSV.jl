using Random: randstring

csvrand(::Type{Int}, quotechar, escapechar, delim) = rand((1, 22, 333, 4444, 55555, 666666, 7777777, 88888888, 999999999, -1, -22, -333, -4444, -55555, -666666, -7777777, -88888888, -999999999))
csvrand(::Type{Float64}, quotechar, escapechar, delim) = rand((1.1, 22.2, 333.3, 4444.4, 55555.5, 666666.6, 7777777.7, 88888888.8, 999999999.9, -1.1, -22.2, -333.3, -4444.4, -55555.5, -666666.6, -7777777.7, -88888888.8, -999999999.9))
csvrand(::Type{String}, quotechar, escapechar, delim) = rand(Bool) ?
    string(quotechar, 'S', randstring(['\n', delim, 'a', 'z'], rand(4:27)), quotechar, escapechar, quotechar, escapechar, 'E', quotechar) :
    string('S', randstring(['a', 'z'], rand(4:27)), 'E')


function generatecsv(path, schema, rows, header=string.("COL_", 1:length(schema)+1), delim=',', quotechar='"', escapechar='"')
    buf = collect(csvrand.(schema, quotechar, escapechar, delim))
    open(path, "w") do io
        !isnothing(header) && join(io, header, delim)
        !isnothing(header) && write(io, '\n')
        foreach(1:rows) do i
            buf .= csvrand.(schema, quotechar, escapechar, delim)
            write(io, string(i, delim))
            join(io, buf, delim)
            write(io, '\n')
        end
    end
    return vcat(Int, schema...)
end