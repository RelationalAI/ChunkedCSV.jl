using Test
using Logging
using .Threads
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext

struct TestThrowingContext <: AbstractConsumeContext end

const throw_ctx = TestThrowingContext()
function ChunkedCSV.consume!(task_buf::TaskResultBuffer{N}, parsing_ctx::ParsingContext, row_num::UInt32, ctx::TestThrowingContext) where N
    error("These contexts are for throwing, and that's all what they do")
end

@testset "exception_handling" begin
    @testset "serial" begin
        test_logger = TestLogger(catch_exceptions=true);
        with_logger(test_logger) do
            @test_throws "These contexts are for throwing, and that's all what they do" parse_file(IOBuffer("""
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                throw_ctx,
                _force=:serial,
            )
        end
    end

    @testset "singlebuffer" begin
        test_logger = TestLogger(catch_exceptions=true);
        with_logger(test_logger) do
            @test_throws "These contexts are for throwing, and that's all what they do" parse_file(IOBuffer("""
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                throw_ctx,
                nworkers=nthreads(),
                _force=:singlebuffer,
            )
        end
        @test length(test_logger.logs) == nthreads()
        @test test_logger.logs[1].message == "Task failed"
        @test test_logger.logs[1].kwargs[1][1].msg == "These contexts are for throwing, and that's all what they do"
    end

    @testset "doublebuffer" begin
        test_logger = TestLogger(catch_exceptions=true);
        with_logger(test_logger) do
            @test_throws "These contexts are for throwing, and that's all what they do" parse_file(IOBuffer("""
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                throw_ctx,
                nworkers=nthreads(),
                _force=:doublebuffer,
            )
        end
        @test length(test_logger.logs) == nthreads()
        @test test_logger.logs[1].message == "Task failed"
        @test test_logger.logs[1].kwargs[1][1].msg == "These contexts are for throwing, and that's all what they do"
    end
end