using Test
using .Threads
using ChunkedCSV
using ChunkedCSV: TaskResultBuffer, ParsingContext
using FixedPointDecimals

struct CustomType
    x::Any
end

# Throws when a specified row is greater than the first row of a task buffer
struct TestThrowingContext <: AbstractConsumeContext
    tasks::Vector{Task}
    conds::Vector{ChunkedCSV.TaskCounter}
    throw_row::Int
end
TestThrowingContext(throw_row) = TestThrowingContext(Task[], ChunkedCSV.TaskCounter[], throw_row)

# Throws in the last quarter of the buffer
struct ThrowingIO <: IO
    io::IOBuffer
    throw_byte::Int
end
ThrowingIO(s::String) = ThrowingIO(IOBuffer(s), length(s) - cld(length(s), 4))
ChunkedCSV.readbytesall!(io::ThrowingIO, buf, n::Int) = io.io.ptr > io.throw_byte ? error("That should be enough data for everyone") : ChunkedCSV.readbytesall!(io.io, buf, n)
Base.eof(io::ThrowingIO) = Base.eof(io.io)

function ChunkedCSV.consume!(ctx::TestThrowingContext, parsing_ctx::ParsingContext, task_buf::TaskResultBuffer, row_num::Int, eol_idx::Int32)
    t = current_task()
    c = parsing_ctx.counter
    c in ctx.conds || push!(ctx.conds, c)
    t in ctx.tasks || push!(ctx.tasks, t)
    row_num >= ctx.throw_row && error("These contexts are for throwing, and that's all what they do")
    sleep(0.01) # trying to get the task off a fast path to claim everything from the parsing queue
    return nothing
end

@testset "Exception Handling" begin
    @testset "Lexing errors" begin
        @testset "NoValidRowsInBufferError" begin
            @test_throws ChunkedCSV.NoValidRowsInBufferError begin
                parse_file(IOBuffer("""
                    a,b
                    1,23
                    3,4
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    buffersize=4,
                )
            end

            @test_throws ChunkedCSV.NoValidRowsInBufferError begin
                parse_file(IOBuffer("""
                    0,"S\"\"\"
                    1,"S\"\"\"
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    header=false,
                    buffersize=7,
                )
            end

            @test_throws ChunkedCSV.NoValidRowsInBufferError begin
                parse_file(IOBuffer("""
                    0,"S\\\\"
                    1,"S\\\\"
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    header=false,
                    buffersize=7,
                    escapechar='\\'
                )
            end

            @test_throws ChunkedCSV.NoValidRowsInBufferError begin
                parse_file(IOBuffer("""
                    a,b
                    1,"2
                    3,4"
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    buffersize=5,
                )
            end

            @test_throws ChunkedCSV.NoValidRowsInBufferError begin
                ChunkedCSV.parse_file(IOBuffer("""
                    1234567
                    \"\\\"a\\\\\\\\\\"\""""),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    header=false,
                    buffersize=8,
                    escapechar='\\',
                )
            end
        end

        @testset "UnmatchedQuoteError" begin
            @test_throws ChunkedCSV.UnmatchedQuoteError begin
                parse_file(IOBuffer("""
                    a,b
                    1,2
                    3,"4
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    buffersize=5,
                )
            end
            @test_throws ChunkedCSV.UnmatchedQuoteError begin
                parse_file(IOBuffer("""
                    a,b
                    1,2
                    3,"4\\
                    """),
                    nothing,
                    ChunkedCSV.SkipContext(),
                    escapechar='\\',
                )
            end
        end
    end

    @testset "consume!" begin
        @testset "serial" begin
            throw_ctx = TestThrowingContext(2)
            @test_throws ErrorException("These contexts are for throwing, and that's all what they do") parse_file(IOBuffer("""
                a,b
                1,2
                3,4
                """),
                [Int,Int],
                throw_ctx,
                _force=:serial,
                buffersize=6
            )
            @assert !isempty(throw_ctx.tasks)
            @test throw_ctx.tasks[1] == current_task()
            @test throw_ctx.conds[1].exception isa ErrorException
        end

        @testset "parallel" begin
            # 1500 rows should be enough to get each of the 3 task at least one consume!
            throw_ctx = TestThrowingContext(1500)
            @test_throws TaskFailedException parse_file(
                IOBuffer("a,b\n" * ("1,2\n3,4\n" ^ 800)), # 1600 rows total
                [Int,Int],
                throw_ctx,
                nworkers=min(3, nthreads()),
                _force=:parallel,
                buffersize=12,
            )
            sleep(0.2)
            @test length(throw_ctx.tasks) == min(3, nthreads())
            @test all(istaskdone, throw_ctx.tasks)
            @test throw_ctx.conds[1].exception isa CapturedException
            @test throw_ctx.conds[1].exception.ex.msg == "These contexts are for throwing, and that's all what they do"
            @test throw_ctx.conds[2].exception isa CapturedException
            @test throw_ctx.conds[2].exception.ex.msg == "These contexts are for throwing, and that's all what they do"
        end
    end

    @testset "io" begin
        @testset "serial" begin
            throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
            @test_throws ErrorException("That should be enough data for everyone") parse_file(
                ThrowingIO("a,b\n" * ("1,2\n3,4\n" ^ 10)), # 20 rows total
                [Int,Int],
                throw_ctx,
                _force=:serial,
                buffersize=6,
            )
            @assert !isempty(throw_ctx.tasks)
            @test throw_ctx.tasks[1] == current_task()
            @test throw_ctx.conds[1].exception isa ErrorException
        end

        @testset "parallel" begin
            throw_ctx = TestThrowingContext(typemax(Int)) # Only capture tasks, let IO do the throwing
            @test_throws TaskFailedException parse_file(
                ThrowingIO("a,b\n" * ("1,2\n3,4\n" ^ 800)), # 1600 rows total
                [Int,Int],
                throw_ctx,
                nworkers=min(3, nthreads()),
                _force=:parallel,
                buffersize=12,
            )
            sleep(0.2)
            @test length(throw_ctx.tasks) == min(3, nthreads())
            @test all(istaskdone, throw_ctx.tasks)
            @test throw_ctx.conds[1].exception isa CapturedException
            @test throw_ctx.conds[1].exception.ex.task.result.msg == "That should be enough data for everyone"
            @test throw_ctx.conds[2].exception isa CapturedException
            @test throw_ctx.conds[2].exception.ex.task.result.msg == "That should be enough data for everyone"
        end
    end
end

@testset "Schema and header validation" begin
    @test_throws ArgumentError("Provided header and schema lengths don't match. Header has 3 columns, schema has 2.") parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        [Int,Int],
        header=[:a, :b, :c],
    )

    @test_throws ChunkedCSV.HeaderParsingError("Error parsing header, there are more columns (3) than provided types in schema (2) at 1:6 (row:pos).") parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        [Int,Int],
        header=true,
    )

    @test_throws ArgumentError("Provided header and schema names don't match. In schema, not in header: Set([:q])). In header, not in schema: [:a, :b, :c]") parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        Dict(:q => Int),
        header=[:a, :b, :c],
        validate_type_map=true,
    )

    @test_throws ArgumentError("Unknown columns from schema mapping: Set([:q]), parsed header: [:a, :b, :c], row 1") parse_file(IOBuffer("""
        a,b,c
        1,2,3
        3,4,5
        """),
        Dict(:q => Int),
        header=true,
        validate_type_map=true,
    )

    @test_throws ArgumentError("Provided schema contains unsupported types: FixedDecimal{Int64, 100}, CustomType.") parse_file(IOBuffer("""
        a,b,c,d
        1,2,3,"00000000-0000-0000-0000-000000000000"
        3,4,5,"00000000-0000-0000-0000-000000000000"
        """),
        [Int,FixedDecimal{Int64,9},FixedDecimal{Int64,100},CustomType],
    )
end