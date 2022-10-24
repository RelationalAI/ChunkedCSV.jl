using Test
using Logging
using .Threads
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext

struct TestThrowingContext <: AbstractConsumeContext end
struct ThrowingIO <: IO
    io::IOBuffer
end
ThrowingIO(s::String) = ThrowingIO(IOBuffer(s))
ChunkedCSV.readbytesall!(io::ThrowingIO, buf, n) = io.io.ptr > 6 ? error("That should be enough data for everyone") : ChunkedCSV.readbytesall!(io.io, buf, n)
Base.eof(io::ThrowingIO) = Base.eof(io.io)


const throw_ctx = TestThrowingContext()
function ChunkedCSV.consume!(ctx::TestThrowingContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer{N,M}, row_num::UInt32, eol_idx::UInt32) where {N,M}
    error("These contexts are for throwing, and that's all what they do")
end

@testset "Exception Handling" begin
    @testset "consume!" begin
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
                sleep(0.2)
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
                sleep(0.2)
            end
            @test length(test_logger.logs) == nthreads()
            @test test_logger.logs[1].message == "Task failed"
            @test test_logger.logs[1].kwargs[1][1].msg == "These contexts are for throwing, and that's all what they do"
        end
    end

    @testset "io" begin
        @testset "serial" begin
            test_logger = TestLogger(catch_exceptions=true);
            with_logger(test_logger) do
                @test_throws "That should be enough data for everyone" parse_file(ThrowingIO("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    ChunkedCSV.SkipContext(),
                    _force=:serial,
                    buffersize=4,
                )
            end
        end

        @testset "singlebuffer" begin
            test_logger = TestLogger(catch_exceptions=true);
            with_logger(test_logger) do
                @test_throws "That should be enough data for everyone" parse_file(ThrowingIO("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    ChunkedCSV.SkipContext(),
                    nworkers=nthreads(),
                    _force=:singlebuffer,
                    buffersize=4,
                )
                sleep(0.2)
            end
            @test length(test_logger.logs) == nthreads()
            @test test_logger.logs[1].message == "Task failed"
            @test test_logger.logs[1].kwargs[1][1].task.result.msg == "That should be enough data for everyone"
        end

        @testset "doublebuffer" begin
            test_logger = TestLogger(catch_exceptions=true);
            with_logger(test_logger) do
                @test_throws "That should be enough data for everyone" parse_file(ThrowingIO("""
                    a,b
                    1,2
                    3,4
                    """),
                    [Int,Int],
                    ChunkedCSV.SkipContext(),
                    nworkers=nthreads(),
                    _force=:doublebuffer,
                    buffersize=4,
                )
                sleep(0.2)
            end
            @test length(test_logger.logs) == nthreads()
            @test test_logger.logs[1].message == "Task failed"
            @test test_logger.logs[1].kwargs[1][1].task.result.msg == "That should be enough data for everyone"
        end
    end

    @testset "Empty input" begin
        for alg in [:serial, :singlebuffer, :doublebuffer]
            @testset "$alg, has file header, no provided header, no schema" begin
                @test_throws "Error parsing header for column 1 at 1:1 (row:col)." parse_file(IOBuffer(""), nothing, ChunkedCSV.SkipContext(), _force=alg, hasheader=true, header=nothing)
            end

            @testset "$alg, has file header, no provided header, has schema" begin
                @test_throws "Error parsing header for column 1 at 1:1 (row:col)." parse_file(IOBuffer(""), [Int, String], ChunkedCSV.SkipContext(), _force=alg, hasheader=true, header=nothing)
            end
        end
    end
end

@testset "Schema and header validation" begin
    @test_throws "Provided header and schema lengths don't match. Header has 3 columns, schema has 2." parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        [Int,Int],
        header=[:a, :b, :c],
    )

    @test_throws "Error parsing header, there are more columns that provided types in schema" parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        [Int,Int],
        header=nothing,
    )

    @test_throws "Provided header and schema names don't match. In schema, not in header: Set([:q])). In header, not in schema: [:a, :b, :c]" parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        Dict(:q => Int),
        header=[:a, :b, :c],
        validate_type_map=true,
    )

    @test_throws "Unknown columns from schema mapping: Set([:q]), parsed header: [:a, :b, :c]" parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        Dict(:q => Int),
        header=nothing,
        validate_type_map=true,
    )
end