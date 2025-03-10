using ChunkedCSV
using ChunkedCSV: Enums
using Test
import Parsers
using Dates

@testset "TaskResultBuffers" begin

@testset "_translate_to_buffer_type" begin
    @test ChunkedCSV._translate_to_buffer_type(Int) == Int
    @test ChunkedCSV._translate_to_buffer_type(Float64) == Float64
    @test ChunkedCSV._translate_to_buffer_type(String) == Parsers.PosLen31
    @test ChunkedCSV._translate_to_buffer_type(ChunkedCSV.GuessDateTime) == DateTime
end

@testset "TaskResultBuffer constructors" begin
    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, Parsers.PosLen31], 10)
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.missing_values isa ChunkedCSV.BitSetMatrix
    @test buf.errored_values isa ChunkedCSV.BitSetMatrix
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test size(buf.missing_values) == (0, 3)
    @test size(buf.errored_values) == (0, 3)

    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, Parsers.PosLen31])
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.missing_values isa ChunkedCSV.BitSetMatrix
    @test buf.errored_values isa ChunkedCSV.BitSetMatrix
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 0
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 0
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 0
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 0
    @test size(buf.missing_values) == (0, 3)
    @test size(buf.errored_values) == (0, 3)

    bufs = ChunkedCSV._make_result_buffers(2, [Int, Float64, Parsers.PosLen31], 3)
    @test length(bufs) == 2
    @test bufs[1].id == 1
    @test bufs[1].cols isa Vector{ChunkedCSV.BufferedVector}
    @test bufs[1].cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test bufs[1].cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test bufs[1].cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test bufs[1].row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test bufs[1].missing_values isa ChunkedCSV.BitSetMatrix
    @test bufs[1].errored_values isa ChunkedCSV.BitSetMatrix
    @test length(bufs[1].cols) == 3
    @test length(bufs[1].row_statuses) == 0
    @test length(bufs[1].row_statuses.elements) == 3
    @test length(bufs[1].cols[1]) == 0
    @test length(bufs[1].cols[1].elements) == 3
    @test length(bufs[1].cols[2]) == 0
    @test length(bufs[1].cols[2].elements) == 3
    @test length(bufs[1].cols[3]) == 0
    @test length(bufs[1].cols[3].elements) == 3
    @test size(bufs[1].missing_values) == (0, 3)
    @test size(bufs[1].errored_values) == (0, 3)
    @test bufs[2].id == 2
    @test bufs[2].cols isa Vector{ChunkedCSV.BufferedVector}
    @test bufs[2].cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test bufs[2].cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test bufs[2].cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test bufs[2].row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test bufs[2].missing_values isa ChunkedCSV.BitSetMatrix
    @test bufs[2].errored_values isa ChunkedCSV.BitSetMatrix
    @test length(bufs[2].cols) == 3
    @test length(bufs[2].row_statuses) == 0
    @test length(bufs[2].row_statuses.elements) == 3
    @test length(bufs[2].cols[1]) == 0
    @test length(bufs[2].cols[1].elements) == 3
    @test length(bufs[2].cols[2]) == 0
    @test length(bufs[2].cols[2].elements) == 3
    @test length(bufs[2].cols[3]) == 0
    @test length(bufs[2].cols[3].elements) == 3
    @test size(bufs[2].missing_values) == (0, 3)
    @test size(bufs[2].errored_values) == (0, 3)
end

@testset "TaskResultBuffer empty! and ensureroom" begin
    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, Parsers.PosLen31], 10)
    push!(buf.cols[1], 1)
    push!(buf.cols[2], 1.0)
    push!(buf.cols[3], Parsers.PosLen31(1, 1))
    push!(buf.row_statuses, ChunkedCSV.RowStatus.Ok)
    ChunkedCSV.addrows!(buf.missing_values, 1)
    ChunkedCSV.addrows!(buf.errored_values, 1)

    @test length(buf.cols[1]) == 1
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 1
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 1
    @test length(buf.cols[3].elements) == 10
    @test length(buf) == 1
    @test length(buf.row_statuses) == 1
    @test length(buf.row_statuses.elements) == 10
    @test size(buf.missing_values) == (1, 3)
    @test size(buf.errored_values) == (1, 3)
    ChunkedCSV.empty!(buf)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test length(buf) == 0
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test size(buf.missing_values) == (0, 3)
    @test size(buf.errored_values) == (0, 3)
    Base.ensureroom(buf, 20)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 20
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 20
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 20
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 20
    @test size(buf.missing_values) == (0, 3)
    @test size(buf.errored_values) == (0, 3)
end


@testset "ColumnIterator" begin
    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64], 10)

    # +------------------------------------------------------------------+
    # |                        TASK_RESULT_BUFFER                        |
    # +--------------------------+---------+---------+---------+---------+
    # |       row_statuses       | missing | errored | cols[1] | cols[2] |
    # +--------------------------+---------+---------+---------+---------+
    # | Ok                       |   ---   |   ---   |    1    |   1.0   |
    # | Miss                     |   1 0   |   ---   |  undef  |   2.0   |
    # | TooManyColumns           |   ---   |   ---   |    3    |   3.0   |
    # | Miss | ValueParsingError |   0 1   |   1 0   |  undef  |  undef  |
    # | Miss | TooFewColumns     |   1 0   |   0 1   |  undef  |  undef  |
    # | Miss | SkippedRow        |   1 1   |   ---   |  undef  |  undef  |
    # +--------------------------+---------+---------+---------+---------+
    for i in 1:6; push!(buf.cols[1], i); end
    for i in 1:6; push!(buf.cols[2], Float64(i)); end
    push!(buf.row_statuses, ChunkedCSV.RowStatus.Ok)
    push!(buf.row_statuses, ChunkedCSV.RowStatus.MissingValues)
    push!(buf.row_statuses, ChunkedCSV.RowStatus.TooManyColumns)
    push!(buf.row_statuses, ChunkedCSV.RowStatus.MissingValues | ChunkedCSV.RowStatus.ValueParsingError)
    push!(buf.row_statuses, ChunkedCSV.RowStatus.MissingValues | ChunkedCSV.RowStatus.TooFewColumns)
    push!(buf.row_statuses, ChunkedCSV.RowStatus.MissingValues | ChunkedCSV.RowStatus.SkippedRow)
    ChunkedCSV.addrows!(buf.missing_values, 4)
    ChunkedCSV.addrows!(buf.errored_values, 2)

    buf.missing_values[1, 1] = true # ~ data row 2, col 1, "Miss"
    buf.missing_values[2, 2] = true # ~ data row 4, col 2, "Miss | ValueParsingError"
    buf.missing_values[3, 1] = true # ~ data row 5, col 1, "Miss | TooFewColumns"
    buf.missing_values[4, 1] = true # ~ data row 6, col 1, "Miss | SkippedRow"
    buf.missing_values[4, 2] = true # ~ data row 6, col 2, "Miss | SkippedRow"

    buf.errored_values[1, 1] = true # ~ data row 4, col 1, "Miss | ValueParsingError"
    buf.errored_values[2, 2] = true # ~ data row 5, col 2, "Miss | TooFewColumns"

    iter_data = collect(ChunkedCSV.ColumnIterator{Int}(buf, 1))
    #                                          val, errrow, errval, missval
    @test iter_data[1] == ChunkedCSV.ParsedField(1,  false,  false,  false)
    @test iter_data[2] == ChunkedCSV.ParsedField(2,  false,  false,   true)
    @test iter_data[3] == ChunkedCSV.ParsedField(3,   true,  false,  false)
    @test iter_data[4] == ChunkedCSV.ParsedField(4,   true,   true,  false)
    @test iter_data[5] == ChunkedCSV.ParsedField(5,   true,  false,   true)
    @test iter_data[6] == ChunkedCSV.ParsedField(6,  false,  false,   true)

    iter_data = collect(ChunkedCSV.ColumnIterator{Float64}(buf, 2))
    #                                            val, errrow, errval, missval
    @test iter_data[1] == ChunkedCSV.ParsedField(1.0,  false,  false,  false)
    @test iter_data[2] == ChunkedCSV.ParsedField(2.0,  false,  false,  false)
    @test iter_data[3] == ChunkedCSV.ParsedField(3.0,   true,  false,  false)
    @test iter_data[4] == ChunkedCSV.ParsedField(4.0,   true,  false,   true)
    @test iter_data[5] == ChunkedCSV.ParsedField(5.0,   true,   true,  false)
    @test iter_data[6] == ChunkedCSV.ParsedField(6.0,  false,  false,   true)
end


@testset "BitSetMatrix" begin
    bs = ChunkedCSV.BitSetMatrix(3, 4)
    @test bs.nrows == 3
    @test bs.ncolumns == 4
    @test size(bs) == (3, 4)
    @test all(iszero, bs)

    bs[3,2] = true
    @test bs[3, 2]
    @test !bs[3, 3]
    @test bs[3, :] == [false, true, false, false]

    @test ChunkedCSV.addrows!(bs) == 4
    @test bs.nrows == 4
    @test all(iszero, bs[4, :])

    @test ChunkedCSV.addrows!(bs, 1, true) == 5
    @test bs.nrows == 5
    @test all(bs[5, :])

    @test ChunkedCSV.addrows!(bs, 2) == 7
    @test bs.nrows == 7
    @test all(iszero, bs[6, :])
    @test all(iszero, bs[7, :])

    @test ChunkedCSV.addcols!(bs) == 5
    @test bs.ncolumns == 5
    @test sum(bs) == 5
    @test bs[3, 2]
    @test bs[5,:] == [true, true, true, true, false]

    @test ChunkedCSV.addcols!(bs, 2) == 7
    @test bs.ncolumns == 7
    @test sum(bs) == 5
    @test bs[3, 2]
    @test bs[5,:] == [true, true, true, true, false, false, false]

    empty!(bs)
    @test bs.nrows == 0
    @test length(bs.data) == 0
    @test bs.ncolumns == 7
end

end # @testset "TaskResultBuffers"
