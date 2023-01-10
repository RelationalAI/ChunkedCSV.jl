@precompile_setup begin
    # Putting some things in `setup` can reduce the size of the
    # precompile file and potentially make loading faster.
    PRECOMPILE_DATA = """
    int,float,date,datetime,bool,null,str,int_float
    1,3.1,2019-01-01,2019-01-01T01:02:03,true,null,hey,2
    2,NaN,2019-01-02,2019-01-03T01:02:03,false,,there,3.14
    """
    @precompile_all_calls begin
        # all calls in this block will be precompiled, regardless of whether
        # they belong to your package or not (on Julia 1.8 and higher)
        ChunkedCSV.parse_file(IOBuffer(PRECOMPILE_DATA), [Int,Float64,Date,DateTime,Bool,String,String,FixedDecimal{UInt128,8}], ChunkedCSV.SkipContext())
    end
end

@assert precompile(ChunkedCSV.parse_file, (String, Vector{DataType}))
@assert precompile(ChunkedCSV.parse_file, (IOStream, Vector{DataType}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt8}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt8}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt16}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt16}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt32}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt32}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt64}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt64}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt128}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{UInt128}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{NTuple{3,UInt64}}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Vector{UInt8}))
@assert precompile(ChunkedCSV._parse_rows_forloop!, (TaskResultBuffer{NTuple{3,UInt64}}, Base.SubArray{UInt32, 1, ChunkedCSV.BufferedVector{UInt32}, Tuple{Base.UnitRange{UInt32}}, true}, Vector{UInt8}, Vector{DataType}, Parsers.Options, Nothing))
