using ChunkedCSV
using ChunkedCSV: Enums
using Test
import Parsers
using Dates

@testset "TaskResultBuffers" begin

@testset "_bounding_flag_type" begin
    @test ChunkedCSV._bounding_flag_type(0) == UInt8
    @test ChunkedCSV._bounding_flag_type(1) == UInt8
    @test ChunkedCSV._bounding_flag_type(8) == UInt8
    @test ChunkedCSV._bounding_flag_type(9) == UInt16
    @test ChunkedCSV._bounding_flag_type(16) == UInt16
    @test ChunkedCSV._bounding_flag_type(17) == UInt32
    @test ChunkedCSV._bounding_flag_type(32) == UInt32
    @test ChunkedCSV._bounding_flag_type(33) == UInt64
    @test ChunkedCSV._bounding_flag_type(64) == UInt64
    @test ChunkedCSV._bounding_flag_type(65) == UInt128
    @test ChunkedCSV._bounding_flag_type(127) == UInt128
    @test ChunkedCSV._bounding_flag_type(128) == UInt128
    @test ChunkedCSV._bounding_flag_type(129) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(130) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(192) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(193) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(256) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(257) == NTuple{5,UInt64}
    @test ChunkedCSV._bounding_flag_type(258) == NTuple{5,UInt64}

    @test ChunkedCSV._bounding_flag_type(fill(Int, 0)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Int, 1)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Int, 8)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Int, 9)) == UInt16
    @test ChunkedCSV._bounding_flag_type(fill(Int, 16)) == UInt16
    @test ChunkedCSV._bounding_flag_type(fill(Int, 17)) == UInt32
    @test ChunkedCSV._bounding_flag_type(fill(Int, 32)) == UInt32
    @test ChunkedCSV._bounding_flag_type(fill(Int, 33)) == UInt64
    @test ChunkedCSV._bounding_flag_type(fill(Int, 64)) == UInt64
    @test ChunkedCSV._bounding_flag_type(fill(Int, 65)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Int, 127)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Int, 128)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Int, 129)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 130)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 192)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 193)) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 256)) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 257)) == NTuple{5,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Int, 258)) == NTuple{5,UInt64}

    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 0))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 1))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 8))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 9))) == UInt16
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 16))) == UInt16
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 17))) == UInt32
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 32))) == UInt32
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 33))) == UInt64
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 64))) == UInt64
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 65))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 127))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 128))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 129))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 130))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 192))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 193))) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 256))) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 257))) == NTuple{5,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Nothing, 100), fill(Int, 258))) == NTuple{5,UInt64}

    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 0)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 1)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 8)) == UInt8
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 9)) == UInt16
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 16)) == UInt16
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 17)) == UInt32
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 32)) == UInt32
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 33)) == UInt64
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 64)) == UInt64
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 65)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 127)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 128)) == UInt128
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 129)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 130)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 192)) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 193)) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 256)) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 257)) == NTuple{5,UInt64}
    @test ChunkedCSV._bounding_flag_type(fill(Enums.INT, 258)) == NTuple{5,UInt64}

    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 0))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 1))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 8))) == UInt8
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 9))) == UInt16
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 16))) == UInt16
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 17))) == UInt32
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 32))) == UInt32
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 33))) == UInt64
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 64))) == UInt64
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 65))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 127))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 128))) == UInt128
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 129))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 130))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 192))) == NTuple{3,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 193))) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 256))) == NTuple{4,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 257))) == NTuple{5,UInt64}
    @test ChunkedCSV._bounding_flag_type(vcat(fill(Enums.SKIP, 100), fill(Enums.INT, 258))) == NTuple{5,UInt64}
end

@testset "_translate_to_buffer_type" begin 
    @test ChunkedCSV._translate_to_buffer_type(Int) == Int
    @test ChunkedCSV._translate_to_buffer_type(Float64) == Float64
    @test ChunkedCSV._translate_to_buffer_type(Float64, false) == Float64
    @test ChunkedCSV._translate_to_buffer_type(String) == Parsers.PosLen31
    @test ChunkedCSV._translate_to_buffer_type(String, false) == String
    @test ChunkedCSV._translate_to_buffer_type(ChunkedCSV.GuessDateTime) == DateTime
end

@testset "TaskResultBuffer constructors" begin 
    buf = ChunkedCSV.TaskResultBuffer{UInt8}(1, [Int, Float64, String], 10)
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.column_indicators isa ChunkedCSV.BufferedVector{UInt8}
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test length(buf.column_indicators) == 0
    @test length(buf.column_indicators.elements) == 0

    buf = ChunkedCSV.TaskResultBuffer(1, [Int, Float64, String])
    @test buf.id == 1
    @test buf.cols isa Vector{ChunkedCSV.BufferedVector}
    @test buf.cols[1] isa ChunkedCSV.BufferedVector{Int}
    @test buf.cols[2] isa ChunkedCSV.BufferedVector{Float64}
    @test buf.cols[3] isa ChunkedCSV.BufferedVector{Parsers.PosLen31}
    @test buf.row_statuses isa ChunkedCSV.BufferedVector{ChunkedCSV.RowStatus.T}
    @test buf.column_indicators isa ChunkedCSV.BufferedVector{UInt8}
    @test length(buf.cols) == 3
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 0
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 0
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 0
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 0
    @test length(buf.column_indicators) == 0
    @test length(buf.column_indicators.elements) == 0
end

@testset "TaskResultBuffer empty! and ensureroom" begin 
    buf = ChunkedCSV.TaskResultBuffer{UInt8}(1, [Int, Float64, String], 10)
    push!(buf.cols[1], 1)
    push!(buf.cols[2], 1.0)
    push!(buf.cols[3], Parsers.PosLen31(1, 1))
    push!(buf.row_statuses, ChunkedCSV.RowStatus.Ok)
    push!(buf.column_indicators, UInt8(1))

    @test length(buf.cols[1]) == 1
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 1
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 1
    @test length(buf.cols[3].elements) == 10
    @test length(buf.row_statuses) == 1
    @test length(buf.row_statuses.elements) == 10
    @test length(buf.column_indicators) == 1
    @test length(buf.column_indicators.elements) == ChunkedCSV.BufferedVectors._grow_by(UInt8)
    ChunkedCSV.empty!(buf)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 10
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 10
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 10
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 10
    @test length(buf.column_indicators) == 0
    @test length(buf.column_indicators.elements) == ChunkedCSV.BufferedVectors._grow_by(UInt8)
    Base.ensureroom(buf, 20)
    @test length(buf.cols[1]) == 0
    @test length(buf.cols[1].elements) == 20
    @test length(buf.cols[2]) == 0
    @test length(buf.cols[2].elements) == 20
    @test length(buf.cols[3]) == 0
    @test length(buf.cols[3].elements) == 20
    @test length(buf.row_statuses) == 0
    @test length(buf.row_statuses.elements) == 20
    @test length(buf.column_indicators) == 0
    @test length(buf.column_indicators.elements) == ChunkedCSV.BufferedVectors._grow_by(UInt8)
end

@testset "initflag" begin
    @test ChunkedCSV.initflag(UInt8) == 0x00
    @test ChunkedCSV.initflag(UInt16) == 0x0000
    @test ChunkedCSV.initflag(UInt32) == 0x00000000
    @test ChunkedCSV.initflag(UInt64) == 0x0000000000000000
    @test ChunkedCSV.initflag(UInt128) == 0x00000000000000000000000000000000
    @test ChunkedCSV.initflag(NTuple{3,UInt64}) == (0x0000000000000000, 0x0000000000000000, 0x0000000000000000)
    @test ChunkedCSV.initflag(NTuple{4,UInt64}) == (0x0000000000000000, 0x0000000000000000, 0x0000000000000000, 0x0000000000000000)
end

@testset "initflagset" begin
    @test ChunkedCSV.initflagset(UInt8) == 0xFF
    @test ChunkedCSV.initflagset(UInt16) == 0xFFFF
    @test ChunkedCSV.initflagset(UInt32) == 0xFFFFFFFF
    @test ChunkedCSV.initflagset(UInt64) == 0xFFFFFFFFFFFFFFFF
    @test ChunkedCSV.initflagset(UInt128) == 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    @test ChunkedCSV.initflagset(NTuple{3,UInt64}) == (0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
    @test ChunkedCSV.initflagset(NTuple{4,UInt64}) == (0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
end

@testset "isflagset" begin
    # bitstring(0xAA) == "10101010"
    for i in 1:sizeof(UInt8)*8
        @test ChunkedCSV.isflagset(0xAA, i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(UInt8)*8):(2*sizeof(UInt8)*8)
        @test !ChunkedCSV.isflagset(0xAA, i)
    end
    @test !ChunkedCSV.isflagset(0xAA, 0)

    for i in 1:sizeof(UInt16)*8
        @test ChunkedCSV.isflagset(0xAAAA, i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(UInt16)*8):(2*sizeof(UInt16)*8)
        @test !ChunkedCSV.isflagset(0xAAAA, i)
    end
    @test !ChunkedCSV.isflagset(0xAAAA, 0)

    for i in 1:sizeof(UInt32)*8
        @test ChunkedCSV.isflagset(0xAAAAAAAA, i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(UInt32)*8):(2*sizeof(UInt32)*8)
        @test !ChunkedCSV.isflagset(0xAAAAAAAA, i)
    end
    @test !ChunkedCSV.isflagset(0xAAAAAAAA, 0)

    for i in 1:sizeof(UInt64)*8
        @test ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAA, i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(UInt64)*8):(2*sizeof(UInt64)*8)
        @test !ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAA, i)
    end
    @test !ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAA, 0)

    for i in 1:sizeof(UInt128)*8
        @test ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA, i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(UInt128)*8):(2*sizeof(UInt128)*8)
        @test !ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA, i)
    end
    @test !ChunkedCSV.isflagset(0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA, 0)

    for i in 1:sizeof(NTuple{3,UInt64})*8
        @test ChunkedCSV.isflagset((0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA), i) == (i % 2 == 0)
    end
    for i in (1 + sizeof(NTuple{3,UInt64})*8):(2*sizeof(NTuple{3,UInt64})*8)
        @test !ChunkedCSV.isflagset((0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA), i)
    end
    @test !ChunkedCSV.isflagset((0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA, 0xAAAAAAAAAAAAAAAA), 0)
end

@testset "setflag" begin
    for T in (UInt8, UInt16, UInt32, UInt64, UInt128)
        for i in 1:sizeof(T)*8
            @test ChunkedCSV.setflag(typemin(T), i) == one(T) << (i-1)
        end
    end
    
    nt = (0x0000000000000000, 0x0000000000000000, 0x0000000000000000)
    for i in 1:sizeof(NTuple{3,UInt64})*8
        @test ChunkedCSV.setflag(nt, i) == (one(UInt64) << (i-129), one(UInt64) << (i-65), one(UInt64) << (i-1))
    end

    nt = (0x0000000000000000, 0x0000000000000000, 0x0000000000000000, 0x0000000000000000)
    for i in 1:sizeof(NTuple{4,UInt64})*8
        @test ChunkedCSV.setflag(nt, i) == (one(UInt64) << (i-193), one(UInt64) << (i-129), one(UInt64) << (i-65), one(UInt64) << (i-1))
    end
end

@testset "anyflagset" begin
    @test ChunkedCSV.anyflagset(0x00) == false
    @test ChunkedCSV.anyflagset(0x01) == true

    @test ChunkedCSV.anyflagset(0x0000) == false
    @test ChunkedCSV.anyflagset(0x0001) == true

    @test ChunkedCSV.anyflagset(0x00000000) == false
    @test ChunkedCSV.anyflagset(0x00000001) == true

    @test ChunkedCSV.anyflagset(0x0000000000000000) == false
    @test ChunkedCSV.anyflagset(0x0000000000000001) == true

    @test ChunkedCSV.anyflagset(0x00000000000000000000000000000000) == false
    @test ChunkedCSV.anyflagset(0x00000000000000000000000000000001) == true

    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000000)) == false
    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000001)) == true
    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000001, 0x0000000000000000)) == true
    @test ChunkedCSV.anyflagset((0x0000000000000001, 0x0000000000000000, 0x0000000000000000)) == true

    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000000, 0x0000000000000000)) == false
    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000000, 0x0000000000000001)) == true
    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000001, 0x0000000000000000)) == true
    @test ChunkedCSV.anyflagset((0x0000000000000000, 0x0000000000000001, 0x0000000000000000, 0x0000000000000000)) == true
    @test ChunkedCSV.anyflagset((0x0000000000000001, 0x0000000000000000, 0x0000000000000000, 0x0000000000000000)) == true
end

@testset "flagpadding" begin
    @test ChunkedCSV.flagpadding(1) == 7
    @test ChunkedCSV.flagpadding(2) == 6
    @test ChunkedCSV.flagpadding(3) == 5
    @test ChunkedCSV.flagpadding(4) == 4
    @test ChunkedCSV.flagpadding(5) == 3
    @test ChunkedCSV.flagpadding(6) == 2
    @test ChunkedCSV.flagpadding(7) == 1
    @test ChunkedCSV.flagpadding(8) == 0
    @test ChunkedCSV.flagpadding(9) == 7
    @test ChunkedCSV.flagpadding(10) == 6
    @test ChunkedCSV.flagpadding(11) == 5
    @test ChunkedCSV.flagpadding(12) == 4
    @test ChunkedCSV.flagpadding(13) == 3
    @test ChunkedCSV.flagpadding(14) == 2
    @test ChunkedCSV.flagpadding(15) == 1
    @test ChunkedCSV.flagpadding(16) == 0
    @test ChunkedCSV.flagpadding(17) == 15
    @test ChunkedCSV.flagpadding(18) == 14
    @test ChunkedCSV.flagpadding(19) == 13
    @test ChunkedCSV.flagpadding(32) == 0
    @test ChunkedCSV.flagpadding(33) == 31
    @test ChunkedCSV.flagpadding(64) == 0
    @test ChunkedCSV.flagpadding(65) == 63
    @test ChunkedCSV.flagpadding(128) == 0
    @test ChunkedCSV.flagpadding(129) == 63
    @test ChunkedCSV.flagpadding(256) == 0
    @test ChunkedCSV.flagpadding(257) == 63
    @test ChunkedCSV.flagpadding(512) == 0
    @test ChunkedCSV.flagpadding(513) == 63
end

@testset "lastflagset" begin
    @test ChunkedCSV.lastflagset(0x00) == 9
    @test ChunkedCSV.lastflagset(0x01) == 1
    @test ChunkedCSV.lastflagset(0x02) == 2
    @test ChunkedCSV.lastflagset(0x04) == 3
    @test ChunkedCSV.lastflagset(0x05) == 1
    @test ChunkedCSV.lastflagset(0x06) == 2
    @test ChunkedCSV.lastflagset(0xFF) == 1
    @test ChunkedCSV.lastflagset(0x80) == 8 

    @test ChunkedCSV.lastflagset(0x0000) == 17
    @test ChunkedCSV.lastflagset(0x0001) == 1
    @test ChunkedCSV.lastflagset(0x0002) == 2
    @test ChunkedCSV.lastflagset(0x0004) == 3
    @test ChunkedCSV.lastflagset(0x0005) == 1
    @test ChunkedCSV.lastflagset(0x0006) == 2
    @test ChunkedCSV.lastflagset(0xFFFF) == 1
    @test ChunkedCSV.lastflagset(0x8000) == 16

    @test ChunkedCSV.lastflagset(0x00000000) == 33
    @test ChunkedCSV.lastflagset(0x00000001) == 1
    @test ChunkedCSV.lastflagset(0x00000002) == 2
    @test ChunkedCSV.lastflagset(0x00000004) == 3
    @test ChunkedCSV.lastflagset(0x00000005) == 1
    @test ChunkedCSV.lastflagset(0x00000006) == 2
    @test ChunkedCSV.lastflagset(0xFFFFFFFF) == 1
    @test ChunkedCSV.lastflagset(0x80000000) == 32

    @test ChunkedCSV.lastflagset(0x0000000000000000) == 65
    @test ChunkedCSV.lastflagset(0x0000000000000001) == 1
    @test ChunkedCSV.lastflagset(0x0000000000000002) == 2
    @test ChunkedCSV.lastflagset(0x0000000000000004) == 3
    @test ChunkedCSV.lastflagset(0x0000000000000005) == 1
    @test ChunkedCSV.lastflagset(0x0000000000000006) == 2
    @test ChunkedCSV.lastflagset(0xFFFFFFFFFFFFFFFF) == 1
    @test ChunkedCSV.lastflagset(0x8000000000000000) == 64

    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000000) == 129
    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000001) == 1
    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000002) == 2
    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000004) == 3
    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000005) == 1
    @test ChunkedCSV.lastflagset(0x00000000000000000000000000000006) == 2
    @test ChunkedCSV.lastflagset(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) == 1
    @test ChunkedCSV.lastflagset(0x80000000000000000000000000000000) == 128

    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000000)) == 193
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000001)) == 1
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000002)) == 2
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000004)) == 3
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000005)) == 1
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000006)) == 2
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0xFFFFFFFFFFFFFFFF)) == 1
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000000, 0x8000000000000000)) == 64
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000001, 0x0000000000000000)) == 65
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000002, 0x0000000000000000)) == 66
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000004, 0x0000000000000000)) == 67
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000005, 0x0000000000000000)) == 65
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x0000000000000006, 0x0000000000000000)) == 66
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0xFFFFFFFFFFFFFFFF, 0x0000000000000000)) == 65
    @test ChunkedCSV.lastflagset((0x0000000000000000, 0x8000000000000000, 0x0000000000000000)) == 128
    @test ChunkedCSV.lastflagset((0x0000000000000001, 0x0000000000000000, 0x0000000000000000)) == 129
    @test ChunkedCSV.lastflagset((0x0000000000000002, 0x0000000000000000, 0x0000000000000000)) == 130
    @test ChunkedCSV.lastflagset((0x0000000000000004, 0x0000000000000000, 0x0000000000000000)) == 131
    @test ChunkedCSV.lastflagset((0x0000000000000005, 0x0000000000000000, 0x0000000000000000)) == 129
    @test ChunkedCSV.lastflagset((0x0000000000000006, 0x0000000000000000, 0x0000000000000000)) == 130
    @test ChunkedCSV.lastflagset((0xFFFFFFFFFFFFFFFF, 0x0000000000000000, 0x0000000000000000)) == 129
    @test ChunkedCSV.lastflagset((0x8000000000000000, 0x0000000000000000, 0x0000000000000000)) == 192
    @test ChunkedCSV.lastflagset((0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)) == 1
end


@testset "firstflagset" begin
    @test ChunkedCSV.firstflagset(0x00) == 0
    @test ChunkedCSV.firstflagset(0x01) == 1
    @test ChunkedCSV.firstflagset(0x02) == 2
    @test ChunkedCSV.firstflagset(0x04) == 3
    @test ChunkedCSV.firstflagset(0x05) == 3
    @test ChunkedCSV.firstflagset(0x06) == 3
    @test ChunkedCSV.firstflagset(0xFF) == 8
    @test ChunkedCSV.firstflagset(0x80) == 8 

    @test ChunkedCSV.firstflagset(0x0000) == 0
    @test ChunkedCSV.firstflagset(0x0001) == 1
    @test ChunkedCSV.firstflagset(0x0002) == 2
    @test ChunkedCSV.firstflagset(0x0004) == 3
    @test ChunkedCSV.firstflagset(0x0005) == 3
    @test ChunkedCSV.firstflagset(0x0006) == 3
    @test ChunkedCSV.firstflagset(0xFFFF) == 16
    @test ChunkedCSV.firstflagset(0x8000) == 16

    @test ChunkedCSV.firstflagset(0x00000000) == 0
    @test ChunkedCSV.firstflagset(0x00000001) == 1
    @test ChunkedCSV.firstflagset(0x00000002) == 2
    @test ChunkedCSV.firstflagset(0x00000004) == 3
    @test ChunkedCSV.firstflagset(0x00000005) == 3
    @test ChunkedCSV.firstflagset(0x00000006) == 3
    @test ChunkedCSV.firstflagset(0xFFFFFFFF) == 32
    @test ChunkedCSV.firstflagset(0x80000000) == 32

    @test ChunkedCSV.firstflagset(0x0000000000000000) == 0
    @test ChunkedCSV.firstflagset(0x0000000000000001) == 1
    @test ChunkedCSV.firstflagset(0x0000000000000002) == 2
    @test ChunkedCSV.firstflagset(0x0000000000000004) == 3
    @test ChunkedCSV.firstflagset(0x0000000000000005) == 3
    @test ChunkedCSV.firstflagset(0x0000000000000006) == 3
    @test ChunkedCSV.firstflagset(0xFFFFFFFFFFFFFFFF) == 64
    @test ChunkedCSV.firstflagset(0x8000000000000000) == 64

    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000000) == 0
    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000001) == 1
    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000002) == 2
    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000004) == 3
    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000005) == 3
    @test ChunkedCSV.firstflagset(0x00000000000000000000000000000006) == 3
    @test ChunkedCSV.firstflagset(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) == 128
    @test ChunkedCSV.firstflagset(0x80000000000000000000000000000000) == 128

    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000000)) == 0
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000001)) == 1
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000002)) == 2
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000004)) == 3
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000005)) == 3
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x0000000000000006)) == 3
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0xFFFFFFFFFFFFFFFF)) == 64
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000000, 0x8000000000000000)) == 64
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000001, 0x0000000000000000)) == 65
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000002, 0x0000000000000000)) == 66
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000004, 0x0000000000000000)) == 67
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000005, 0x0000000000000000)) == 67
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x0000000000000006, 0x0000000000000000)) == 67
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0xFFFFFFFFFFFFFFFF, 0x0000000000000000)) == 128
    @test ChunkedCSV.firstflagset((0x0000000000000000, 0x8000000000000000, 0x0000000000000000)) == 128
    @test ChunkedCSV.firstflagset((0x0000000000000001, 0x0000000000000000, 0x0000000000000000)) == 129
    @test ChunkedCSV.firstflagset((0x0000000000000002, 0x0000000000000000, 0x0000000000000000)) == 130
    @test ChunkedCSV.firstflagset((0x0000000000000004, 0x0000000000000000, 0x0000000000000000)) == 131
    @test ChunkedCSV.firstflagset((0x0000000000000005, 0x0000000000000000, 0x0000000000000000)) == 131
    @test ChunkedCSV.firstflagset((0x0000000000000006, 0x0000000000000000, 0x0000000000000000)) == 131
    @test ChunkedCSV.firstflagset((0xFFFFFFFFFFFFFFFF, 0x0000000000000000, 0x0000000000000000)) == 192
    @test ChunkedCSV.firstflagset((0x8000000000000000, 0x0000000000000000, 0x0000000000000000)) == 192
    @test ChunkedCSV.firstflagset((0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)) == 192
end

end # @testset "TaskResultBuffers"