using Test
using ChunkedCSV

@testset "Consume contexts" begin
    @testset "defaults" begin
        parsing_ctx = ChunkedCSV.ParsingContext(1, DataType[], ChunkedCSV.Enums.CSV_TYPE[], Symbol[], UInt8[], ChunkedCSV.BufferedVector{Int32}(), 0, 0x00, 0x00, ChunkedCSV.TaskCounter(), nothing)
        consume_ctx = ChunkedCSV.SkipContext() # uses default methods
        result_buf = ChunkedCSV.TaskResultBuffer(1, [Int])
        @assert parsing_ctx.counter.n == 0

        ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 1)
        @test parsing_ctx.counter.n == 1
        ChunkedCSV.task_done!(consume_ctx, parsing_ctx)
        @test parsing_ctx.counter.n == 0
        @test ChunkedCSV.sync_tasks(consume_ctx, parsing_ctx) === nothing # would hang if ntasks != 0
        @test_throws AssertionError ChunkedCSV.task_done!(consume_ctx, parsing_ctx)
        ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 2)
        @test parsing_ctx.counter.n == 2
        # We only ever increment when we are done wirh all tasks,
        # so we assert the counter is zero before calling `set!` on it.
        @test_throws AssertionError ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 2)
        ChunkedCSV.task_done!(consume_ctx, parsing_ctx)
        @test parsing_ctx.counter.n == 1
        ChunkedCSV.task_done!(consume_ctx, parsing_ctx)
        @test parsing_ctx.counter.n == 0

        @test_throws ArgumentError ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 0)
        @test_throws ArgumentError ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, -1)
    end
end

@testset "TaskCounter" begin
    @testset "defaults" begin
        counter = ChunkedCSV.TaskCounter()
        @assert counter.n == 0
        ChunkedCSV.set!(counter, 1)
        @test counter.n == 1
        ChunkedCSV.dec!(counter)
        @test counter.n == 0
        @test ChunkedCSV.wait(counter) === nothing # would hang if ntasks != 0
        @test_throws AssertionError ChunkedCSV.dec!(counter)
        ChunkedCSV.set!(counter, 2)
        @test counter.n == 2
        # We only ever increment when we are done wirh all tasks,
        # so we assert the counter is zero before calling `set!` on it.
        @test_throws AssertionError ChunkedCSV.set!(counter, 2)
        ChunkedCSV.dec!(counter, 2)
        @test counter.n == 0

        counter = ChunkedCSV.TaskCounter()
        @test_throws ArgumentError ChunkedCSV.set!(counter, 0)
        @test_throws ArgumentError ChunkedCSV.set!(counter, -1)
        @test_throws ArgumentError ChunkedCSV.dec!(counter, 0)
        @test_throws ArgumentError ChunkedCSV.dec!(counter, -1)
    end
end