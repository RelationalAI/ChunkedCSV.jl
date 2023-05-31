using ChunkedCSV
using ChunkedCSV: Enums
using Test
import Parsers
using Dates

@testset "TaskResultBuffers" begin

@testset "_translate_to_buffer_type" begin
    @test ChunkedCSV._translate_to_buffer_type(Int) == Int
    @test ChunkedCSV._translate_to_buffer_type(Float64) == Float64
    @test ChunkedCSV._translate_to_buffer_type(Float64, false) == Float64
    @test ChunkedCSV._translate_to_buffer_type(String) == Parsers.PosLen31
    @test ChunkedCSV._translate_to_buffer_type(String, false) == String
    @test ChunkedCSV._translate_to_buffer_type(ChunkedCSV.GuessDateTime) == DateTime
end

@testset "TaskResultBuffer constructors" begin
    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, String], 10)
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.column_indicators isa ChunkedCSV.BitSetMatrix
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test size(buf.column_indicators) == (0, 3)

    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, String])
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.column_indicators isa ChunkedCSV.BitSetMatrix
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 0
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 0
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 0
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 0
    @test size(buf.column_indicators) == (0, 3)
end

@testset "TaskResultBuffer empty! and ensureroom" begin
    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, String], 10)
    push!(buf.cols[1], 1)
    push!(buf.cols[2], 1.0)
    push!(buf.cols[3], Parsers.PosLen31(1, 1))
    push!(buf.row_statuses, ChunkedCSV.RowStatus.Ok)
    ChunkedCSV.addrows!(buf.column_indicators, 1)

    @test length(buf.cols[1]) == 1
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 1
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 1
    @test length(buf.cols[3].elements) == 10
    @test length(buf.row_statuses) == 1
    @test length(buf.row_statuses.elements) == 10
    @test size(buf.column_indicators) == (1, 3)
    ChunkedCSV.empty!(buf)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test size(buf.column_indicators) == (0, 3)
    Base.ensureroom(buf, 20)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 20
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 20
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 20
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 20
    @test size(buf.column_indicators) == (0, 3)
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
