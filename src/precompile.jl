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
        # We use small `buffersize` to make sure that tha parallel parsing codepath
        # has more than one chunk to work with.
        ChunkedCSV.parse_file(IOBuffer(PRECOMPILE_DATA), [Int,Float64,Date,DateTime,Bool,String,String,FixedDecimal{Int64,8}], ChunkedCSV.SkipContext(), buffersize=64)
        ChunkedCSV.parse_file(IOBuffer(PRECOMPILE_DATA), [Int,Float64,Date,DateTime,Bool,String,String,FixedDecimal{Int64,8}], ChunkedCSV.SkipContext(), force=:parallel, nworkers=2, buffersize=64)
        ChunkedCSV.parse_file(IOBuffer(PRECOMPILE_DATA), [Int,Float64,Date,GuessDateTime,Bool,String,String,Int], ChunkedCSV.SkipContext(), buffersize=64)
        ChunkedCSV.parse_file(IOBuffer(PRECOMPILE_DATA), [Int,Float64,Date,GuessDateTime,Bool,String,String,Int], ChunkedCSV.SkipContext(), force=:parallel, nworkers=2, buffersize=64)
    end
end

@assert precompile(ChunkedCSV.parse_file, (String, Vector{DataType}))
@assert precompile(ChunkedCSV.parse_file, (IOStream, Vector{DataType}))
