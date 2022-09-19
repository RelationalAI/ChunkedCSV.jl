using Test
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext
import Parsers

struct TestContext <: AbstractConsumeContext
    results::Vector{TaskResultBuffer}
    header::Vector{Symbol}
    schema::Vector{DataType}
    lock::ReentrantLock
end
TestContext() = TestContext([], [], [], ReentrantLock())
function ChunkedCSV.consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, ctx::TestContext) where N
    @lock ctx.lock begin
        push!(ctx.results, deepcopy(task_buf))
        isempty(ctx.header) && append!(ctx.header, copy(parsing_ctx.header))
        isempty(ctx.schema) && append!(ctx.schema, copy(parsing_ctx.schema))
    end
end

Threads.nthreads() == 1 && @warn "Running tests with a single thread -- won't be able to spot concurency issues"

@testset "simple file parsing" begin
    @testset "simple file, single chunk" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end

            @testset "$alg float" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1.0,2.0
                    3.0,4.0
                    """),
                    [Float64, Float64],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1:2] == [1.0,3.0]
                @test testctx.results[1].cols[2].elements[1:2] == [2.0,4.0]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end

            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    "1","2"
                    "3","4"
                    """),
                    [String,String],
                    testctx,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(5, 1), Parsers.PosLen(13, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(9, 1), Parsers.PosLen(17, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "simple file, multiple chunks" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg int" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    buffersize=6,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test testctx.results[2].cols[1].elements[1] == 3
                @test testctx.results[2].cols[2].elements[1] == 4
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end

            @testset "$alg float" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    1.0,2.0
                    3.0,4.0
                    """),
                    [Float64, Float64],
                    testctx,
                    buffersize=10,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1] == 1.0
                @test testctx.results[1].cols[2].elements[1] == 2.0
                @test testctx.results[2].cols[1].elements[1] == 3.0
                @test testctx.results[2].cols[2].elements[1] == 4.0
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end

            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    a,b
                    "1","2"
                    "3","4"
                    """),
                    [String,String],
                    testctx,
                    buffersize=8,
                    _force=alg,
                )
                sort!(testctx.results, by=x->x.cols[1][1].pos)
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(1, 1)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(5, 1)
                @test testctx.results[2].cols[1].elements[1] == Parsers.PosLen(1, 1)
                @test testctx.results[2].cols[2].elements[1] == Parsers.PosLen(5, 1)
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end
        end
    end

    @testset "skiprows" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxxxxx
                    xxxxxx
                    xxxxxx
                    xxxxxx
                    a,b
                    "1","2"
                    "3","4"
                    "5","6"
                    """),
                    [String,String],
                    testctx,
                    buffersize=8,
                    _force=alg,
                    skiprows=4,
                )
                sort!(testctx.results, by=x->x.cols[1][1].pos)
                @test testctx.header == [:a, :b]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(1, 1)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(5, 1)
                @test testctx.results[2].cols[1].elements[1] == Parsers.PosLen(1, 1)
                @test testctx.results[2].cols[2].elements[1] == Parsers.PosLen(5, 1)
                @test testctx.results[3].cols[1].elements[1] == Parsers.PosLen(1, 1)
                @test testctx.results[3].cols[2].elements[1] == Parsers.PosLen(5, 1)
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    xxx
                    xxx
                    a,b
                    1,2
                    3,4
                    5,6
                    """),
                    [Int,Int],
                    testctx,
                    buffersize=4,
                    _force=alg,
                    skiprows=4,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test testctx.results[2].cols[1].elements[1] == 3
                @test testctx.results[2].cols[2].elements[1] == 4
                @test testctx.results[3].cols[1].elements[1] == 5
                @test testctx.results[3].cols[2].elements[1] == 6
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
            @testset "$alg string" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxxxxx
                    xxxxxx
                    xxxxxx
                    xxxxxx
                    a,b
                    1.0,2.0
                    3.0,4.0
                    5.0,6.0
                    """),
                    [Float64,Float64],
                    testctx,
                    buffersize=8,
                    _force=alg,
                    skiprows=4,
                )
                sort!(testctx.results, by=x->x.cols[1][1])
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Float64, Float64]
                @test testctx.results[1].cols[1].elements[1] == 1.0
                @test testctx.results[1].cols[2].elements[1] == 2.0
                @test testctx.results[2].cols[1].elements[1] == 3.0
                @test testctx.results[2].cols[2].elements[1] == 4.0
                @test testctx.results[3].cols[1].elements[1] == 5.0
                @test testctx.results[3].cols[2].elements[1] == 6.0
                @test length(testctx.results) == 3
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
                @test length(testctx.results[3].cols[1]) == 1
                @test length(testctx.results[3].cols[2]) == 1
            end
        end
    end
    @testset "No header, no schema" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    nothing,
                    testctx,
                    hasheader=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(0, 1), Parsers.PosLen(4, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(2, 1), Parsers.PosLen(6, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    nothing,
                    testctx,
                    hasheader=false,
                    skiprows=2,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [String, String]
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(8, 1), Parsers.PosLen(12, 1)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(10, 1), Parsers.PosLen(14, 1)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "No header" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    hasheader=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    hasheader=false,
                    skiprows=2,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1:2] == [1,3]
                @test testctx.results[1].cols[2].elements[1:2] == [2,4]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "limit" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    hasheader=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    xxx
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    hasheader=false,
                    skiprows=2,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end

    @testset "limit and skiprow" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg" begin
                testctx = TestContext()
                parse_file(IOBuffer("""
                    1,2
                    3,4
                    5,6
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    skiprows=1,
                    hasheader=false,
                    _force=alg,
                )
                @test testctx.header == [:COL_1, :COL_2]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 3
                @test testctx.results[1].cols[2].elements[1] == 4
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1

                testctx = TestContext()
                parse_file(IOBuffer("""
                    xxx
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    testctx,
                    limit=1,
                    hasheader=true,
                    skiprows=1,
                    _force=alg,
                )
                @test testctx.header == [:a, :b]
                @test testctx.schema == [Int, Int]
                @test testctx.results[1].cols[1].elements[1] == 1
                @test testctx.results[1].cols[2].elements[1] == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
            end
        end
    end
end