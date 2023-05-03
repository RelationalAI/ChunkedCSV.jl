using Test
using ChunkedCSV

@testset "Consume contexts" begin
    @testset "defaults" begin
        parsing_ctx = ChunkedCSV.ParsingContext(1, DataType[], ChunkedCSV.Enums.CSV_TYPE[], Symbol[], UInt8[], ChunkedCSV.BufferedVector{Int32}(), 0, 0x00, 0x00, ChunkedCSV.TaskCondition(), nothing)
        consume_ctx = ChunkedCSV.SkipContext() # uses default methods
        result_buf = ChunkedCSV.TaskResultBuffer(1, [Int])
        @assert parsing_ctx.cond.ntasks == 0
        ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 1)
        @test parsing_ctx.cond.ntasks == 1
        ChunkedCSV.task_done!(consume_ctx, parsing_ctx, result_buf)
        @test parsing_ctx.cond.ntasks == 0
        @test ChunkedCSV.sync_tasks(consume_ctx, parsing_ctx) === nothing # would hang if ntasks != 0
        ChunkedCSV.task_done!(consume_ctx, parsing_ctx, result_buf)
        @test parsing_ctx.cond.ntasks == -1
        ChunkedCSV.setup_tasks!(consume_ctx, parsing_ctx, 2)
        @test parsing_ctx.cond.ntasks == 2
    end
end