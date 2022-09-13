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

@testset "csv" begin
    @testset "simple file, single buffer" begin
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
                @test testctx.results[1].cols[1].elements[1:2] == [Parsers.PosLen(6, 0), Parsers.PosLen(14, 0)]
                @test testctx.results[1].cols[2].elements[1:2] == [Parsers.PosLen(10, 0), Parsers.PosLen(18, 0)]
                @test length(testctx.results[1].cols[1]) == 2
                @test length(testctx.results[1].cols[2]) == 2
            end
        end
    end

    @testset "simple file, multiple buffers" begin
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
                @test testctx.results[1].cols[1].elements[1] == Parsers.PosLen(2, 0)
                @test testctx.results[1].cols[2].elements[1] == Parsers.PosLen(6, 0)
                @test testctx.results[2].cols[1].elements[1] == Parsers.PosLen(2, 0)
                @test testctx.results[2].cols[2].elements[1] == Parsers.PosLen(6, 0)
                @test length(testctx.results) == 2
                @test length(testctx.results[1].cols[1]) == 1
                @test length(testctx.results[1].cols[2]) == 1
                @test length(testctx.results[2].cols[1]) == 1
                @test length(testctx.results[2].cols[2]) == 1
            end
        end
    end
end