using ChunkedCSV
using Test
using Dates
using FixedPointDecimals
using Parsers

for alg in (:serial, :parallel)
    @testset "MmapStream ($alg)" begin
        function _iostream(x::String)
            (path, io) = mktemp()
            write(io, x)
            close(io)
            return path
        end
        testctx = ChunkedCSV.TestContext()
        parse_file(_iostream("""
            a,b,c
            1,2,3
            3,4,4
            """),
            [Int,Int,Int],
            testctx,
            _force=alg,
            buffersize=8,
            use_mmap=true,
        )
        @test testctx.header == [:a, :b, :c]
        @test testctx.schema == [Int, Int, Int]
        @test testctx.results[1].cols[1] == [1]
        @test testctx.results[1].cols[2] == [2]
        @test testctx.results[1].cols[3] == [3]
        @test testctx.results[2].cols[1] == [3]
        @test testctx.results[2].cols[2] == [4]
        @test testctx.results[2].cols[3] == [4]
        @test length(testctx.results[1].cols[1]) == 1
        @test length(testctx.results[1].cols[2]) == 1
        @test length(testctx.results[1].cols[3]) == 1
        @test length(testctx.results[2].cols[1]) == 1
        @test length(testctx.results[2].cols[2]) == 1
        @test length(testctx.results[2].cols[3]) == 1
    end
end

@testset "_is_supported_type" begin
    @test ChunkedCSV._is_supported_type(Bool)
    @test ChunkedCSV._is_supported_type(Int)
    @test ChunkedCSV._is_supported_type(Int8)
    @test ChunkedCSV._is_supported_type(Int16)
    @test ChunkedCSV._is_supported_type(Int32)
    @test ChunkedCSV._is_supported_type(Int64)
    @test ChunkedCSV._is_supported_type(UInt)
    @test ChunkedCSV._is_supported_type(UInt8)
    @test ChunkedCSV._is_supported_type(UInt16)
    @test ChunkedCSV._is_supported_type(UInt32)
    @test ChunkedCSV._is_supported_type(UInt64)
    @test ChunkedCSV._is_supported_type(Float16)
    @test ChunkedCSV._is_supported_type(Float32)
    @test ChunkedCSV._is_supported_type(Float64)
    @test ChunkedCSV._is_supported_type(String)
    @test ChunkedCSV._is_supported_type(ChunkedCSV.GuessDateTime)
    @test ChunkedCSV._is_supported_type(Date)
    @test ChunkedCSV._is_supported_type(DateTime)
    @test ChunkedCSV._is_supported_type(Time)

    @test ChunkedCSV._is_supported_type(FixedDecimal{Int,8})
    @test ChunkedCSV._is_supported_type(FixedDecimal{UInt128,16})
    @test ChunkedCSV._is_supported_type(FixedDecimal{UInt8,0})
    @test ChunkedCSV._is_supported_type(FixedDecimal{UInt8,1})
    @test ChunkedCSV._is_supported_type(FixedDecimal{UInt8,2})
    @test !ChunkedCSV._is_supported_type(FixedDecimal{UInt8,3})
    @test !ChunkedCSV._is_supported_type(FixedDecimal{UInt128,100})

    @test !ChunkedCSV._is_supported_type(ComplexF16)
    @test !ChunkedCSV._is_supported_type(ComplexF32)
end

@testset "Schema inputs" begin
    @testset "$schema_input" for (schema_input, typed) in (
        (nothing, false), ([Int, String], true), (Dict(:a => Int, :b => String), true),
        (Dict(:a => Int), true), (Dict(:b => String), false), (Dict(1 => Int, 2 => String), true),
        (Dict(1 => Int), true), (Dict(2 => String), false), (String, false),
        ((i, name) -> i == 1 ? Int : String, true)
    )
        for kwargs in ((header=true, skipto=0), (header=[:a, :b], skipto=2))
            ctx = ChunkedCSV.TestContext()
            parse_file(IOBuffer("a,b\n1,\"s\"\n"), schema_input, ctx; kwargs...)
            @test ctx.header == [:a, :b]
            if typed
                @test ctx.schema == [Int, Parsers.PosLen31]
                @test ctx.results[1].cols[1] == [1]
                @test ctx.strings[1][2] == ["s"]
            else
                @test ctx.schema == [Parsers.PosLen31, Parsers.PosLen31]
                @test ctx.strings[1][1] == ["1"]
                @test ctx.strings[1][2] == ["s"]
            end
        end
    end
end

@testset "deduplicate_names" begin
    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,b\n1,\"s\"\n"), nothing, ctx; deduplicate_names=true)
    @test ctx.header == [:a, :b]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,a\n1,\"s\"\n"), nothing, ctx; deduplicate_names=true)
    @test ctx.header == [:a, :a_1]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,a\n1,\"s\"\n"), nothing, ctx; deduplicate_names=false)
    @test ctx.header == [:a, :a]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("a,\n1,\"s\"\n"), nothing, ctx; deduplicate_names=true)
    @test ctx.header == [:a, :COL_2]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("COL_2,\n1,\"s\"\n"), nothing, ctx; deduplicate_names=true)
    @test ctx.header == [:COL_2, :COL_2_1]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer("COL_2,\n1,\"s\"\n"), nothing, ctx; deduplicate_names=false)
    @test ctx.header == [:COL_2, :COL_2]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer(",COL_1,COL_1_1\n1,1,1\n"), nothing, ctx; deduplicate_names=true)
    @test ctx.header == [:COL_1, :COL_1_2, :COL_1_1]

    ctx = ChunkedCSV.TestContext()
    parse_file(IOBuffer(",COL_1,COL_1_1\n1,1,1\n"), nothing, ctx; deduplicate_names=false)
    @test ctx.header == [:COL_1, :COL_1, :COL_1_1]
end

@testset "_subset_columns!" begin
    function make_ctx(header)
        schema = fill(String, length(header))
        enum_schema = map(ChunkedCSV.Enums.to_enum, schema)
        ChunkedCSV.ParsingContext(schema, enum_schema, header, UInt8('"'), Parsers.OPTIONS)
    end

    ctx = make_ctx([:a, :b, :c])
    ChunkedCSV._subset_columns!(ctx, nothing, nothing)
    @test ctx.header == [:a, :b, :c]
    @test ctx.schema == [String, String, String]
    @test ctx.enum_schema == [ChunkedCSV.Enums.STRING, ChunkedCSV.Enums.STRING, ChunkedCSV.Enums.STRING]

    for cols in ([:a, :c], ["a", "c"], [1, 3], [true, false, true], (i, name) -> i != 2)
        ctx = make_ctx([:a, :b, :c])
        ChunkedCSV._subset_columns!(ctx, nothing, cols)
        @test ctx.header == [:b]
        @test ctx.schema == [String]
        @test ctx.enum_schema == [ChunkedCSV.Enums.SKIP, ChunkedCSV.Enums.STRING, ChunkedCSV.Enums.SKIP]

        ctx = make_ctx([:a, :b, :c])
        ChunkedCSV._subset_columns!(ctx, cols, nothing)
        @test ctx.header == [:a, :c]
        @test ctx.schema == [String, String]
        @test ctx.enum_schema == [ChunkedCSV.Enums.STRING, ChunkedCSV.Enums.SKIP, ChunkedCSV.Enums.STRING]

        ctx = make_ctx([:a, :b, :c])
        @test_throws ArgumentError ChunkedCSV._subset_columns!(ctx, cols, cols)
    end

    for bad_cols in ([:d], ["d"], Symbol[], String[], [4], [0], Int[], [false, false, false, false], Bool[])
        ctx = make_ctx([:a, :b, :c])
        @test_throws ArgumentError ChunkedCSV._subset_columns!(ctx, nothing, bad_cols)
        ctx = make_ctx([:a, :b, :c])
        @test_throws ArgumentError ChunkedCSV._subset_columns!(ctx, bad_cols, nothing)
    end
end
