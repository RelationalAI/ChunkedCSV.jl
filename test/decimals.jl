using Parsers
using Test
using ChunkedCSV: _typeparse

const DEFAULT_OPTIONS = Parsers.Options()
@testset "decimals" begin
    function exhaustive_tests()
        @testset "$T" for T in (Int8, Int16)
            @testset "$f" for f in (0, 1, 2)
                for i in typemin(T):typemax(T)
                    expected = i * 10^f
                    bs = UInt8.(collect(string(i)))
                    (expected > typemax(T) || expected < typemin(T)) && continue
                    @testset "i" begin @test _typeparse(T, f, bs, DEFAULT_OPTIONS).val == expected end
                    @testset "i.00" begin @test _typeparse(T, f, vcat(bs, UInt8['.', '0', '0']), DEFAULT_OPTIONS).val == expected end
                    @testset "ie0"  begin @test _typeparse(T, f, vcat(bs, UInt8['e', '0']), DEFAULT_OPTIONS).val == expected end
                    if f == 1 && length(bs) > f + 1 + (i < 0)
                        @testset "i.f" begin
                            bss = copy(bs)
                            n = length(bss)
                            splice!(bss, n:n-1, UInt8('.'))
                            @test _typeparse(T, f, bss, DEFAULT_OPTIONS).val == i
                        end
                    end
                end
            end
        end
    end
    exhaustive_tests()

    @test _typeparse(Int32, Int8(4), UInt8.(collect("0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.0e0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.0e+0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.0e-0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.0e0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.0e+0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.0e-0")), DEFAULT_OPTIONS).val == 0
    @test _typeparse(Int32, Int8(4), UInt8.(collect("1")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("1.0")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("1.0e+0")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("1.0e-0")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.1e+1")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.01e+2")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("0.00000000001e+11")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("10.0e-1")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("100.0e-2")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("100000000000.0e-11")), DEFAULT_OPTIONS).val == 10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-1")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-1.0")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-1.0e+0")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-1.0e-0")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.1e+1")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.01e+2")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-0.00000000001e+11")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-10.0e-1")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-100.0e-2")), DEFAULT_OPTIONS).val == -10000
    @test _typeparse(Int32, Int8(4), UInt8.(collect("-100000000000.0e-11")), DEFAULT_OPTIONS).val == -10000


    @testset "decimal position" begin
        @test _typeparse(Int64, 2, UInt8.(collect("123")), DEFAULT_OPTIONS).val   == 123_00
        @test _typeparse(Int64, 2, UInt8.(collect("0.123")), DEFAULT_OPTIONS).val == 0_12
        @test _typeparse(Int64, 2, UInt8.(collect(".123")), DEFAULT_OPTIONS).val  == 0_12
        @test _typeparse(Int64, 2, UInt8.(collect("1.23")), DEFAULT_OPTIONS).val  == 1_23
        @test _typeparse(Int64, 2, UInt8.(collect("12.3")), DEFAULT_OPTIONS).val  == 12_30
        @test _typeparse(Int64, 2, UInt8.(collect("123.")), DEFAULT_OPTIONS).val  == 123_00
        @test _typeparse(Int64, 2, UInt8.(collect("123.0")), DEFAULT_OPTIONS).val == 123_00

        @test _typeparse(Int64, 2, UInt8.(collect("-123")), DEFAULT_OPTIONS).val   == -123_00
        @test _typeparse(Int64, 2, UInt8.(collect("-0.123")), DEFAULT_OPTIONS).val == -0_12
        @test _typeparse(Int64, 2, UInt8.(collect("-.123")), DEFAULT_OPTIONS).val  == -0_12
        @test _typeparse(Int64, 2, UInt8.(collect("-1.23")), DEFAULT_OPTIONS).val  == -1_23
        @test _typeparse(Int64, 2, UInt8.(collect("-12.3")), DEFAULT_OPTIONS).val  == -12_30
        @test _typeparse(Int64, 2, UInt8.(collect("-123.")), DEFAULT_OPTIONS).val  == -123_00
        @test _typeparse(Int64, 2, UInt8.(collect("-123.0")), DEFAULT_OPTIONS).val == -123_00
    end

    @testset "scientific notation" begin
        @test _typeparse(Int64, 4, UInt8.(collect("12e0")), DEFAULT_OPTIONS).val   == 00012_0000
        @test _typeparse(Int64, 4, UInt8.(collect("12e3")), DEFAULT_OPTIONS).val   == 12000_0000
        @test _typeparse(Int64, 4, UInt8.(collect("12e-3")), DEFAULT_OPTIONS).val  == 00000_0120
        @test _typeparse(Int64, 4, UInt8.(collect("1.2e0")), DEFAULT_OPTIONS).val  == 00001_2000
        @test _typeparse(Int64, 4, UInt8.(collect("1.2e3")), DEFAULT_OPTIONS).val  == 01200_0000
        @test _typeparse(Int64, 4, UInt8.(collect("1.2e-3")), DEFAULT_OPTIONS).val == 00000_0012
        @test _typeparse(Int64, 4, UInt8.(collect("1.2e-4")), DEFAULT_OPTIONS).val == 00000_0001

        @test _typeparse(Int64, 4, UInt8.(collect("-12e0")), DEFAULT_OPTIONS).val   == -00012_0000
        @test _typeparse(Int64, 4, UInt8.(collect("-12e3")), DEFAULT_OPTIONS).val   == -12000_0000
        @test _typeparse(Int64, 4, UInt8.(collect("-12e-3")), DEFAULT_OPTIONS).val  == -00000_0120
        @test _typeparse(Int64, 4, UInt8.(collect("-1.2e0")), DEFAULT_OPTIONS).val  == -00001_2000
        @test _typeparse(Int64, 4, UInt8.(collect("-1.2e3")), DEFAULT_OPTIONS).val  == -01200_0000
        @test _typeparse(Int64, 4, UInt8.(collect("-1.2e-3")), DEFAULT_OPTIONS).val == -00000_0012

        @test _typeparse(Int64, 2, UInt8.(collect("999e-1")), DEFAULT_OPTIONS).val == 99_90
        @test _typeparse(Int64, 2, UInt8.(collect("999e-2")), DEFAULT_OPTIONS).val == 09_99
        @test _typeparse(Int64, 2, UInt8.(collect("999e-3")), DEFAULT_OPTIONS).val == 01_00
        @test _typeparse(Int64, 2, UInt8.(collect("999e-4")), DEFAULT_OPTIONS).val == 00_10
        @test _typeparse(Int64, 2, UInt8.(collect("999e-5")), DEFAULT_OPTIONS).val == 00_01
        @test _typeparse(Int64, 2, UInt8.(collect("999e-6")), DEFAULT_OPTIONS).val == 00_00

        @test _typeparse(Int64, 2, UInt8.(collect("-999e-1")), DEFAULT_OPTIONS).val == -99_90
        @test _typeparse(Int64, 2, UInt8.(collect("-999e-2")), DEFAULT_OPTIONS).val == -09_99
        @test _typeparse(Int64, 2, UInt8.(collect("-999e-3")), DEFAULT_OPTIONS).val == -01_00
        @test _typeparse(Int64, 2, UInt8.(collect("-999e-4")), DEFAULT_OPTIONS).val == -00_10
        @test _typeparse(Int64, 2, UInt8.(collect("-999e-5")), DEFAULT_OPTIONS).val == -00_01
        @test _typeparse(Int64, 2, UInt8.(collect("-999e-6")), DEFAULT_OPTIONS).val == -00_00

        @test _typeparse(Int64, 4, UInt8.(collect("9"^96 * "e-100")), DEFAULT_OPTIONS).val == 0_001
    end

    @testset "round to nearest" begin
        @test _typeparse(Int64, 2, UInt8.(collect("0.444")), DEFAULT_OPTIONS).val == 0_44
        @test _typeparse(Int64, 2, UInt8.(collect("0.445")), DEFAULT_OPTIONS).val == 0_44
        @test _typeparse(Int64, 2, UInt8.(collect("0.446")), DEFAULT_OPTIONS).val == 0_45
        @test _typeparse(Int64, 2, UInt8.(collect("0.454")), DEFAULT_OPTIONS).val == 0_45
        @test _typeparse(Int64, 2, UInt8.(collect("0.455")), DEFAULT_OPTIONS).val == 0_46
        @test _typeparse(Int64, 2, UInt8.(collect("0.456")), DEFAULT_OPTIONS).val == 0_46

        @test _typeparse(Int64, 2, UInt8.(collect("-0.444")), DEFAULT_OPTIONS).val == -0_44
        @test _typeparse(Int64, 2, UInt8.(collect("-0.445")), DEFAULT_OPTIONS).val == -0_44
        @test _typeparse(Int64, 2, UInt8.(collect("-0.446")), DEFAULT_OPTIONS).val == -0_45
        @test _typeparse(Int64, 2, UInt8.(collect("-0.454")), DEFAULT_OPTIONS).val == -0_45
        @test _typeparse(Int64, 2, UInt8.(collect("-0.455")), DEFAULT_OPTIONS).val == -0_46
        @test _typeparse(Int64, 2, UInt8.(collect("-0.456")), DEFAULT_OPTIONS).val == -0_46

        @test _typeparse(Int64, 2, UInt8.(collect("0.009")), DEFAULT_OPTIONS).val  ==  0_01
        @test _typeparse(Int64, 2, UInt8.(collect("-0.009")), DEFAULT_OPTIONS).val == -0_01

        @test _typeparse(Int64, 4, UInt8.(collect("1.5e-4")), DEFAULT_OPTIONS).val == 0_0002
    end

    @testset "round to zero" begin
        @test _typeparse(Int64, 2, UInt8.(collect("0.444")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int64, 2, UInt8.(collect("0.445")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int64, 2, UInt8.(collect("0.446")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int64, 2, UInt8.(collect("0.454")), DEFAULT_OPTIONS, RoundToZero).val == 0_45
        @test _typeparse(Int64, 2, UInt8.(collect("0.455")), DEFAULT_OPTIONS, RoundToZero).val == 0_45
        @test _typeparse(Int64, 2, UInt8.(collect("0.456")), DEFAULT_OPTIONS, RoundToZero).val == 0_45

        @test _typeparse(Int64, 2, UInt8.(collect("-0.444")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int64, 2, UInt8.(collect("-0.445")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int64, 2, UInt8.(collect("-0.446")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int64, 2, UInt8.(collect("-0.454")), DEFAULT_OPTIONS, RoundToZero).val == -0_45
        @test _typeparse(Int64, 2, UInt8.(collect("-0.455")), DEFAULT_OPTIONS, RoundToZero).val == -0_45
        @test _typeparse(Int64, 2, UInt8.(collect("-0.456")), DEFAULT_OPTIONS, RoundToZero).val == -0_45

        @test _typeparse(Int64, 2, UInt8.(collect("0.009")), DEFAULT_OPTIONS, RoundToZero).val  == 0_00
        @test _typeparse(Int64, 2, UInt8.(collect("-0.009")), DEFAULT_OPTIONS, RoundToZero).val == 0_00

        @test _typeparse(Int64, 4, UInt8.(collect("1.5e-4")), DEFAULT_OPTIONS, RoundToZero).val == 0_0001
    end


    @testset "decimal position" begin
        @test _typeparse(Int32, 2, UInt8.(collect("123")), DEFAULT_OPTIONS).val   == 123_00
        @test _typeparse(Int32, 2, UInt8.(collect("0.123")), DEFAULT_OPTIONS).val == 0_12
        @test _typeparse(Int32, 2, UInt8.(collect(".123")), DEFAULT_OPTIONS).val  == 0_12
        @test _typeparse(Int32, 2, UInt8.(collect("1.23")), DEFAULT_OPTIONS).val  == 1_23
        @test _typeparse(Int32, 2, UInt8.(collect("12.3")), DEFAULT_OPTIONS).val  == 12_30
        @test _typeparse(Int32, 2, UInt8.(collect("123.")), DEFAULT_OPTIONS).val  == 123_00
        @test _typeparse(Int32, 2, UInt8.(collect("123.0")), DEFAULT_OPTIONS).val == 123_00

        @test _typeparse(Int32, 2, UInt8.(collect("-123")), DEFAULT_OPTIONS).val   == -123_00
        @test _typeparse(Int32, 2, UInt8.(collect("-0.123")), DEFAULT_OPTIONS).val == -0_12
        @test _typeparse(Int32, 2, UInt8.(collect("-.123")), DEFAULT_OPTIONS).val  == -0_12
        @test _typeparse(Int32, 2, UInt8.(collect("-1.23")), DEFAULT_OPTIONS).val  == -1_23
        @test _typeparse(Int32, 2, UInt8.(collect("-12.3")), DEFAULT_OPTIONS).val  == -12_30
        @test _typeparse(Int32, 2, UInt8.(collect("-123.")), DEFAULT_OPTIONS).val  == -123_00
        @test _typeparse(Int32, 2, UInt8.(collect("-123.0")), DEFAULT_OPTIONS).val == -123_00
    end

    @testset "scientific notation" begin
        @test _typeparse(Int32, 4, UInt8.(collect("12e0")), DEFAULT_OPTIONS).val   == 00012_0000
        @test _typeparse(Int32, 4, UInt8.(collect("12e3")), DEFAULT_OPTIONS).val   == 12000_0000
        @test _typeparse(Int32, 4, UInt8.(collect("12e-3")), DEFAULT_OPTIONS).val  == 00000_0120
        @test _typeparse(Int32, 4, UInt8.(collect("1.2e0")), DEFAULT_OPTIONS).val  == 00001_2000
        @test _typeparse(Int32, 4, UInt8.(collect("1.2e3")), DEFAULT_OPTIONS).val  == 01200_0000
        @test _typeparse(Int32, 4, UInt8.(collect("1.2e-3")), DEFAULT_OPTIONS).val == 00000_0012
        @test _typeparse(Int32, 4, UInt8.(collect("1.2e-4")), DEFAULT_OPTIONS).val == 00000_0001

        @test _typeparse(Int32, 4, UInt8.(collect("-12e0")), DEFAULT_OPTIONS).val   == -00012_0000
        @test _typeparse(Int32, 4, UInt8.(collect("-12e3")), DEFAULT_OPTIONS).val   == -12000_0000
        @test _typeparse(Int32, 4, UInt8.(collect("-12e-3")), DEFAULT_OPTIONS).val  == -00000_0120
        @test _typeparse(Int32, 4, UInt8.(collect("-1.2e0")), DEFAULT_OPTIONS).val  == -00001_2000
        @test _typeparse(Int32, 4, UInt8.(collect("-1.2e3")), DEFAULT_OPTIONS).val  == -01200_0000
        @test _typeparse(Int32, 4, UInt8.(collect("-1.2e-3")), DEFAULT_OPTIONS).val == -00000_0012

        @test _typeparse(Int32, 2, UInt8.(collect("999e-1")), DEFAULT_OPTIONS).val == 99_90
        @test _typeparse(Int32, 2, UInt8.(collect("999e-2")), DEFAULT_OPTIONS).val == 09_99
        @test _typeparse(Int32, 2, UInt8.(collect("999e-3")), DEFAULT_OPTIONS).val == 01_00
        @test _typeparse(Int32, 2, UInt8.(collect("999e-4")), DEFAULT_OPTIONS).val == 00_10
        @test _typeparse(Int32, 2, UInt8.(collect("999e-5")), DEFAULT_OPTIONS).val == 00_01
        @test _typeparse(Int32, 2, UInt8.(collect("999e-6")), DEFAULT_OPTIONS).val == 00_00

        @test _typeparse(Int32, 2, UInt8.(collect("-999e-1")), DEFAULT_OPTIONS).val == -99_90
        @test _typeparse(Int32, 2, UInt8.(collect("-999e-2")), DEFAULT_OPTIONS).val == -09_99
        @test _typeparse(Int32, 2, UInt8.(collect("-999e-3")), DEFAULT_OPTIONS).val == -01_00
        @test _typeparse(Int32, 2, UInt8.(collect("-999e-4")), DEFAULT_OPTIONS).val == -00_10
        @test _typeparse(Int32, 2, UInt8.(collect("-999e-5")), DEFAULT_OPTIONS).val == -00_01
        @test _typeparse(Int32, 2, UInt8.(collect("-999e-6")), DEFAULT_OPTIONS).val == -00_00

        @test _typeparse(Int32, 4, UInt8.(collect("9"^96 * "e-100")), DEFAULT_OPTIONS).val == 0_001
    end

    @testset "round to nearest" begin
        @test _typeparse(Int32, 2, UInt8.(collect("0.444")), DEFAULT_OPTIONS).val == 0_44
        @test _typeparse(Int32, 2, UInt8.(collect("0.445")), DEFAULT_OPTIONS).val == 0_44
        @test _typeparse(Int32, 2, UInt8.(collect("0.446")), DEFAULT_OPTIONS).val == 0_45
        @test _typeparse(Int32, 2, UInt8.(collect("0.454")), DEFAULT_OPTIONS).val == 0_45
        @test _typeparse(Int32, 2, UInt8.(collect("0.455")), DEFAULT_OPTIONS).val == 0_46
        @test _typeparse(Int32, 2, UInt8.(collect("0.456")), DEFAULT_OPTIONS).val == 0_46

        @test _typeparse(Int32, 2, UInt8.(collect("-0.444")), DEFAULT_OPTIONS).val == -0_44
        @test _typeparse(Int32, 2, UInt8.(collect("-0.445")), DEFAULT_OPTIONS).val == -0_44
        @test _typeparse(Int32, 2, UInt8.(collect("-0.446")), DEFAULT_OPTIONS).val == -0_45
        @test _typeparse(Int32, 2, UInt8.(collect("-0.454")), DEFAULT_OPTIONS).val == -0_45
        @test _typeparse(Int32, 2, UInt8.(collect("-0.455")), DEFAULT_OPTIONS).val == -0_46
        @test _typeparse(Int32, 2, UInt8.(collect("-0.456")), DEFAULT_OPTIONS).val == -0_46

        @test _typeparse(Int32, 2, UInt8.(collect("0.009")), DEFAULT_OPTIONS).val  ==  0_01
        @test _typeparse(Int32, 2, UInt8.(collect("-0.009")), DEFAULT_OPTIONS).val == -0_01

        @test _typeparse(Int32, 4, UInt8.(collect("1.5e-4")), DEFAULT_OPTIONS).val == 0_0002
    end

    @testset "round to zero" begin
        @test _typeparse(Int32, 2, UInt8.(collect("0.444")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int32, 2, UInt8.(collect("0.445")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int32, 2, UInt8.(collect("0.446")), DEFAULT_OPTIONS, RoundToZero).val == 0_44
        @test _typeparse(Int32, 2, UInt8.(collect("0.454")), DEFAULT_OPTIONS, RoundToZero).val == 0_45
        @test _typeparse(Int32, 2, UInt8.(collect("0.455")), DEFAULT_OPTIONS, RoundToZero).val == 0_45
        @test _typeparse(Int32, 2, UInt8.(collect("0.456")), DEFAULT_OPTIONS, RoundToZero).val == 0_45

        @test _typeparse(Int32, 2, UInt8.(collect("-0.444")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int32, 2, UInt8.(collect("-0.445")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int32, 2, UInt8.(collect("-0.446")), DEFAULT_OPTIONS, RoundToZero).val == -0_44
        @test _typeparse(Int32, 2, UInt8.(collect("-0.454")), DEFAULT_OPTIONS, RoundToZero).val == -0_45
        @test _typeparse(Int32, 2, UInt8.(collect("-0.455")), DEFAULT_OPTIONS, RoundToZero).val == -0_45
        @test _typeparse(Int32, 2, UInt8.(collect("-0.456")), DEFAULT_OPTIONS, RoundToZero).val == -0_45

        @test _typeparse(Int32, 2, UInt8.(collect("0.009")), DEFAULT_OPTIONS, RoundToZero).val  == 0_00
        @test _typeparse(Int32, 2, UInt8.(collect("-0.009")), DEFAULT_OPTIONS, RoundToZero).val == 0_00

        @test _typeparse(Int32, 4, UInt8.(collect("1.5e-4")), DEFAULT_OPTIONS, RoundToZero).val == 0_0001
    end
end