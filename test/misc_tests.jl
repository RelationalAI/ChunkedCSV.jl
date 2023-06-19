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
