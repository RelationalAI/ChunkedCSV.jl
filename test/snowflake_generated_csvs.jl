using ChunkedCSV: ChunkedCSV, TestContext, parse_file, GuessDateTime
using Parsers: PosLen31
using Test
using Dates
using FixedPointDecimals

# The following test cases were produced with the following Snowflake file format:

# CREATE OR REPLACE FILE FORMAT rai_export_format
#     TYPE = CSV
#     FIELD_OPTIONALLY_ENCLOSED_BY = '"'
#     NULL_IF = ()
#     TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM'
#     DATE_FORMAT = 'YYYY-MM-DD'
#     TIME_FORMAT = 'HH24:MI:SS'
#     BINARY_FORMAT = HEX
#     ENCODING = UTF8
# ;

@testset "Snowflake-generated CSVs" begin
    # CREATE OR REPLACE TABLE binary_test_cases (binary BINARY);
    # INSERT INTO binary_test_cases VALUES
    #        (                                ''::BINARY),
    #        (          '1234567890abcdefABCDEF'::BINARY),
    #        ('00000000000000000000000000000000'::BINARY),
    #        ('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'::BINARY);
    @testset "binary" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "BINARY"
            ""
            "1234567890ABCDEFABCDEF"
            "00000000000000000000000000000000"
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
            """),
            [String],
            testctx,
        )
        @test testctx.header == [:BINARY]
        @test testctx.schema == [PosLen31]
        @test testctx.results[1].cols[1] == [PosLen31(11, 0), PosLen31(14, 22), PosLen31(39, 32), PosLen31(74, 32)]
        @test testctx.strings[1][1] == ["", "1234567890ABCDEFABCDEF", "00000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"]
    end

    # CREATE OR REPLACE TABLE boolean_test_cases (bool BOOLEAN);
    # INSERT INTO boolean_test_cases VALUES (True::BOOLEAN), (False::BOOLEAN);
    @testset "boolean" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "BOOL"
            true
            false
            """),
            [Bool],
            testctx,
        )
        @test testctx.header == [:BOOL]
        @test testctx.schema == [Bool]
        @test testctx.results[1].cols[1] == [true, false]
    end

    # CREATE OR REPLACE TABLE char_test_cases (char CHAR);
    # INSERT INTO char_test_cases VALUES
    #        (''::CHAR),  // 0 codeunits
    #        ('a'::CHAR), // 1 codeunit
    #        ('£'::CHAR), // 2 codeunits
    #        ('€'::CHAR), // 3 codeunits
    #        ('𐍈');       // 4 codeunits
    @testset "char" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "CHAR"
            "a"
            "£"
            "€"
            "𐍈"
            """),
            [Char],
            testctx,
        )
        @test testctx.header == [:CHAR]
        @test testctx.schema == [Char]
        @test testctx.results[1].cols[1] == ['a', '£', '€', '𐍈']
    end

    # CREATE OR REPLACE TABLE date_test_cases (date DATE);
    # INSERT INTO date_test_cases VALUES
    #        ('2023-12-31'::DATE),
    #        ('1970-01-01'::DATE),
    #        ('1582-01-01'::DATE), // 1592 is the min recommended year in Snowflake
    #        ('9999-12-31'::DATE); // 9999 is the max recommended year in Snowflake
    @testset "date" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "DATE"
            "2023-12-31"
            "1970-01-01"
            "1582-01-01"
            "9999-12-31"
            """),
            [Date],
            testctx,
        )
        @test testctx.header == [:DATE]
        @test testctx.schema == [Date]
        @test testctx.results[1].cols[1] == [Date(2023, 12, 31), Date(1970, 01, 01), Date(1582, 01, 01), Date(9999, 12, 31)]
    end

    # NOTE: Snowflake loses precision when converting floating point to decimal strings
    # https://docs.snowflake.com/en/user-guide/data-unload-considerations#floating-point-numbers-truncated
    #
    # CREATE OR REPLACE TABLE float_test_cases (float64 FLOAT);
    # INSERT INTO float_test_cases VALUES
    #        ( 0::FLOAT),
    #        (-0::FLOAT),
    #        (1.0000000000000002::FLOAT),
    #        ('-inf'::FLOAT),
    #        ('inf'::FLOAT),
    #        ('NaN'::FLOAT),
    #        (-1.7976931348623157e308::FLOAT), // -> -1.79769313486232e+308 (this truncated number overflows to -Inf)
    #        ( 1.7976931348623157e308::FLOAT), // ->  1.79769313486232e+308 (this truncated number overflows to Inf)
    #        (4.9406564584124654e-324::FLOAT),
    #        (2.2250738585072009e-308::FLOAT),
    #        (2.2250738585072014e-308::FLOAT),
    #        // maxintfloat(Float64) in Julia is 9007199254740992, but this is what their docs are saying...
    #        ( 9007199254740991::FLOAT),
    #        (-9007199254740991::FLOAT),
    #        (NULL::FLOAT);
    @testset "float snowflake output" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "FLOAT64"
            0
            -0
            1
            -inf
            inf
            NaN
            -1.79769313486232e+308
            1.79769313486232e+308
            4.940656458e-324
            2.225073859e-308
            2.225073859e-308
            9.00719925474099e+15
            -9.00719925474099e+15
            """),
            [Float64],
            testctx,
        )
        @test testctx.header == [:FLOAT64]
        @test testctx.schema == [Float64]
        @test testctx.results[1].cols[1][1:5] == [0.0, -0.0, 1.0, -Inf, Inf]
        @test isnan(testctx.results[1].cols[1][6])
        @test testctx.results[1].cols[1][7:13] == [-Inf, Inf, 4.940656458e-324, 2.225073859e-308, 2.225073859e-308, 9.00719925474099e+15, -9.00719925474099e+15]
    end
    # Lets make sure we do handle to original input correctly
    @testset "float original input" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "FLOAT64"
            0
            -0
            1.0000000000000002
            -inf
            inf
            NaN
            -1.7976931348623157e308
            1.7976931348623157e308
            4.9406564584124654e-324
            2.2250738585072009e-308
            2.2250738585072014e-308
            9007199254740992
            -9007199254740992
            """),
            [Float64],
            testctx,
        )
        @test testctx.header == [:FLOAT64]
        @test testctx.schema == [Float64]
        @test testctx.results[1].cols[1][1:5] == [0.0, -0.0, 1.0000000000000002, -Inf, Inf]
        @test isnan(testctx.results[1].cols[1][6])
        @test testctx.results[1].cols[1][7:13] == [-1.7976931348623157e308, 1.7976931348623157e308, 4.940656458e-324, 2.225073858507201e-308, 2.2250738585072014e-308, 9.007199254740992e15, -9.007199254740992e15]
    end

    # CREATE OR REPLACE TABLE numeric_test_cases1 (number_38_37 NUMBER(38,37), number_38_0 NUMBER(38,0), number_19_18 NUMBER(19,18), number_19_0 NUMBER(19,0));
    # INSERT INTO numeric_test_cases1 VALUES
    #        (                                       0::NUMBER(38,37),                                       0::NUMBER(38,0),                     0::NUMBER(19,18),                    0::NUMBER(19,0)),
    #        ( 1.2345678901234567890123456789012345678::NUMBER(38,37),  12345678901234567890123456789012345678::NUMBER(38,0),  1.234567890123456789::NUMBER(19,18),  1234567890123456789::NUMBER(19,0)),
    #        ( 9.9999999999999999999999999999999999999::NUMBER(38,37),  99999999999999999999999999999999999999::NUMBER(38,0),  9.999999999999999999::NUMBER(19,18),  9999999999999999999::NUMBER(19,0)),
    #        (-9.9999999999999999999999999999999999999::NUMBER(38,37), -99999999999999999999999999999999999999::NUMBER(38,0), -9.999999999999999999::NUMBER(19,18), -9999999999999999999::NUMBER(19,0));
    @testset "number pt 1" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "NUMBER_38_37","NUMBER_38_0","NUMBER_19_18","NUMBER_19_0"
            0.0000000000000000000000000000000000000,0,0.000000000000000000,0
            1.2345678901234567890123456789012345678,12345678901234567890123456789012345678,1.234567890123456789,1234567890123456789
            9.9999999999999999999999999999999999999,99999999999999999999999999999999999999,9.999999999999999999,9999999999999999999
            -9.9999999999999999999999999999999999999,-99999999999999999999999999999999999999,-9.999999999999999999,-9999999999999999999
            """),
            [FixedDecimal{Int128,37},Int128,FixedDecimal{Int128,18},Int128],
            testctx,
        )
        @test testctx.header == [:NUMBER_38_37,:NUMBER_38_0,:NUMBER_19_18,:NUMBER_19_0]
        @test testctx.schema == [FixedDecimal{Int128,37},Int128,FixedDecimal{Int128,18},Int128]
        @test testctx.results[1].cols[1] == [
            reinterpret(FixedDecimal{Int128,37},                                       0),
            reinterpret(FixedDecimal{Int128,37},  12345678901234567890123456789012345678),
            reinterpret(FixedDecimal{Int128,37},  99999999999999999999999999999999999999),
            reinterpret(FixedDecimal{Int128,37}, -99999999999999999999999999999999999999),
        ]
        @test testctx.results[1].cols[2] == [
            0,
            12345678901234567890123456789012345678,
            99999999999999999999999999999999999999,
           -99999999999999999999999999999999999999,
        ]
        @test testctx.results[1].cols[3] == [
            reinterpret(FixedDecimal{Int128,18},                    0),
            reinterpret(FixedDecimal{Int128,18},  1234567890123456789),
            reinterpret(FixedDecimal{Int128,18},  9999999999999999999),
            reinterpret(FixedDecimal{Int128,18}, -9999999999999999999),
        ]
        @test testctx.results[1].cols[4] == [
            0,
            1234567890123456789,
            9999999999999999999,
           -9999999999999999999,
        ]
    end

    # CREATE OR REPLACE TABLE numeric_test_cases2 (number_18_17 NUMBER(18,17), number_18_0 NUMBER(18,0));
    # INSERT INTO numeric_test_cases2 VALUES
    #        (                   0::NUMBER(18,17),                   0::NUMBER(18,0)),
    #        ( 1.23456789012345678::NUMBER(18,17),  123456789012345678::NUMBER(18,0)),
    #        ( 9.99999999999999999::NUMBER(18,17),  999999999999999999::NUMBER(18,0)),
    #        (-9.99999999999999999::NUMBER(18,17), -999999999999999999::NUMBER(18,0));
    @testset "number pt 2" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "NUMBER_18_17","NUMBER_18_0"
            0.00000000000000000,0
            1.23456789012345678,123456789012345678
            9.99999999999999999,999999999999999999
            -9.99999999999999999,-999999999999999999
            """),
            [FixedDecimal{Int64,17},Int64],
            testctx,
        )
        @test testctx.header == [:NUMBER_18_17,:NUMBER_18_0]
        @test testctx.schema == [FixedDecimal{Int64,17},Int64]
        @test testctx.results[1].cols[1] == [
            reinterpret(FixedDecimal{Int64,17},                   0),
            reinterpret(FixedDecimal{Int64,17},  123456789012345678),
            reinterpret(FixedDecimal{Int64,17},  999999999999999999),
            reinterpret(FixedDecimal{Int64,17}, -999999999999999999),
        ]
        @test testctx.results[1].cols[2] == [
            0,
            123456789012345678,
            999999999999999999,
           -999999999999999999,
        ]
    end

    # CREATE OR REPLACE TABLE numeric_test_cases3 (number_9_8 NUMBER(9,8), number_4_3 NUMBER(4,3), number_2_1 NUMBER(2,1));
    # INSERT INTO numeric_test_cases3 VALUES
    #        (          0::NUMBER(9,8),      0::NUMBER(4,3),    0::NUMBER(2,1)),
    #        ( 1.23456789::NUMBER(9,8),  1.234::NUMBER(4,3),  1.2::NUMBER(2,1)),
    #        ( 9.99999999::NUMBER(9,8),  9.999::NUMBER(4,3),  9.9::NUMBER(2,1)),
    #        (-9.99999999::NUMBER(9,8), -9.999::NUMBER(4,3), -9.9::NUMBER(2,1)),
    #        (       NULL::NUMBER(9,8),   NULL::NUMBER(4,3), NULL::NUMBER(2,1));
    @testset "number pt 3" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "NUMBER_9_8","NUMBER_4_3","NUMBER_2_1"
            0.00000000,0.000,0.0
            1.23456789,1.234,1.2
            9.99999999,9.999,9.9
            -9.99999999,-9.999,-9.9
            """),
            [FixedDecimal{Int32,8},FixedDecimal{Int16,3},FixedDecimal{Int8,1}],
            testctx,
        )
        @test testctx.header == [:NUMBER_9_8,:NUMBER_4_3,:NUMBER_2_1]
        @test testctx.schema == [FixedDecimal{Int32,8},FixedDecimal{Int16,3},FixedDecimal{Int8,1}]
    end

    # CREATE OR REPLACE TABLE string_test_cases (string STRING);
    # INSERT INTO string_test_cases VALUES
    #        (''::STRING),
    #        ('abc'::STRING),
    #        ('abc"efg'::STRING),
    #        ('quote in " the middle'::STRING),
    #        ('" quote at the beginning'::STRING),
    #        ('quote at the end "'::STRING),
    #        ('many quotes " "" """ """" """"" """"""'::STRING),
    #        ('abc'::STRING),
    #        ('abc\\efg'::STRING),
    #        ('backslash in \\ the middle'::STRING),
    #        ('\\ backslash at the beginning'::STRING),
    #        ('backslash at the end \\'::STRING),
    #        ('many backslashes \\ \\\\ \\\\\\ \\\\\\\\ \\\\\\\\\\ \\\\\\\\\\\\'::STRING),
    #        ('many backslashes and quotes \\ " \\" \\" \\"\\ "\\" ""\\\\ \\\\"" "\\"\\" \\"\\"\\ """\\\\\\'::STRING),
    #        ('escapes \b\f\n\r\t\0'),
    #        ('dollar and single quote $\''),
    #        ('comma, newline \n, backslash \\, double-quote "'),
    #        ('1 codeunit a, 2 codeunits £, 3 codeunits €, 4 codeunits 𐍈');
    @testset "string" begin
        testctx = TestContext()
        parse_file(
            joinpath(pkgdir(ChunkedCSV), "test", "test_files", "string_test_cases.csv"),
            [String],
            testctx,
        )
        @test testctx.header == [:STRING]
        @test testctx.schema == [PosLen31]
        @test testctx.strings[1][1][1] == ""
        @test testctx.strings[1][1][2] == "abc"
        @test testctx.strings[1][1][3] == "abc\"efg"
        @test testctx.strings[1][1][4] == "quote in \" the middle"
        @test testctx.strings[1][1][5] == "\" quote at the beginning"
        @test testctx.strings[1][1][6] == "quote at the end \""
        @test testctx.strings[1][1][7] == "many quotes \" \"\" \"\"\" \"\"\"\" \"\"\"\"\" \"\"\"\"\"\""
        @test testctx.strings[1][1][8] == "abc"
        @test testctx.strings[1][1][9] == "abc\\efg"
        @test testctx.strings[1][1][10] == "backslash in \\ the middle"
        @test testctx.strings[1][1][11] == "\\ backslash at the beginning"
        @test testctx.strings[1][1][12] == "backslash at the end \\"
        @test testctx.strings[1][1][13] == "many backslashes \\ \\\\ \\\\\\ \\\\\\\\ \\\\\\\\\\ \\\\\\\\\\\\"
        @test testctx.strings[1][1][14] == "many backslashes and quotes \\ \" \\\" \\\" \\\"\\ \"\\\" \"\"\\\\ \\\\\"\" \"\\\"\\\" \\\"\\\"\\ \"\"\"\\\\\\"
        @test testctx.strings[1][1][15] == "escapes \b\f\n\r\t\0"
        @test testctx.strings[1][1][16] == "dollar and single quote \$'"
        @test testctx.strings[1][1][17] == "comma, newline \n, backslash \\, double-quote \""
        @test testctx.strings[1][1][18] == "1 codeunit a, 2 codeunits £, 3 codeunits €, 4 codeunits 𐍈"
    end

    # CREATE OR REPLACE TABLE time_test_cases (time TIME);
    # INSERT INTO time_test_cases VALUES
    #        (      '12:34:56.500'::TIME),
    #        ('12:34:56.123456789'::TIME),
    #        ('00:00:00.000000000'::TIME),
    #        ('23:59:59.999999999'::TIME),
    #        (                NULL::TIME);
    @testset "time pt 1" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "TIME"
            "12:34:56"
            "12:34:56"
            "00:00:00"
            "23:59:59"
            """),
            [Time],
            testctx,
        )
        @test testctx.header == [:TIME]
        @test testctx.schema == [Time]
        @test testctx.results[1].cols[1] == [Time(12, 34, 56), Time(12, 34, 56), Time(0, 0, 0), Time(23, 59, 59)]
    end

    # Lets make sure we do handle to full nanosecond precision
    @testset "time pt 2" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "TIME"
            "12:34:56.500000000"
            "12:34:56.123456789"
            "00:00:00.000000000"
            "23:59:59.999999999"
            """),
            [Time],
            testctx,
        )
        @test testctx.header == [:TIME]
        @test testctx.schema == [Time]
        @test testctx.results[1].cols[1][1] == Time(12, 34, 56, 500)
        @test_broken testctx.results[1].cols[1][2] == Time(12, 34, 56, 123, 456, 789)
        @test testctx.results[1].cols[1][3] == Time(0, 0, 0, 0)
        @test_broken testctx.results[1].cols[1][4] == Time(23, 59, 59, 999,999,999)
    end


    # ALTER SESSION SET TIMEZONE = 'America/Los_Angeles'; // -0800
    # CREATE OR REPLACE TABLE timestamp_milli_los_angeles_test_cases (datetime TIMESTAMP_LTZ);
    # INSERT INTO timestamp_milli_los_angeles_test_cases VALUES
    #        (      '2023-12-31 12:34:56.500'::TIMESTAMP_LTZ),
    #        ('1970-01-01 12:34:56.123456789'::TIMESTAMP_LTZ),
    #        ('1582-01-01 00:00:00.000000000'::TIMESTAMP_LTZ),
    #        ('9999-12-31 23:59:59.999999999'::TIMESTAMP_LTZ);
    @testset "timestamps pt 1 America/Los_Angeles" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "DATETIME"
            "2023-12-31 12:34:56.500 -0800"
            "1970-01-01 12:34:56.123 -0800"
            "1582-01-01 00:00:00.000 -0752"
            "9999-12-31 23:59:59.999 -0800"
            """),
            [GuessDateTime],
            testctx,
        )
        @test testctx.header == [:DATETIME]
        @test testctx.schema == [DateTime]
        @test testctx.results[1].cols[1] == [
            DateTime(2023, 12, 31, 20, 34, 56, 500),
            DateTime(1970, 01, 01, 20, 34, 56, 123),
            DateTime(1582, 01, 01, 7, 52, 0, 0),
            DateTime(10000, 1, 1, 7, 59, 59, 999),
        ]
    end

    # ALTER SESSION SET TIMEZONE = 'Australia/Perth'; // +0800
    # CREATE OR REPLACE TABLE timestamp_milli_perth_test_cases (datetime TIMESTAMP_LTZ);
    # INSERT INTO timestamp_milli_perth_test_cases VALUES
    #        (      '2023-12-31 12:34:56.500'::TIMESTAMP_LTZ),
    #        ('1970-01-01 12:34:56.123456789'::TIMESTAMP_LTZ),
    #        ('1582-01-01 00:00:00.000000000'::TIMESTAMP_LTZ),
    #        ('9999-12-31 23:59:59.999999999'::TIMESTAMP_LTZ);
    @testset "timestamps pt 2 Australia/Perth" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "DATETIME"
            "2023-12-31 12:34:56.500 +0800"
            "1970-01-01 12:34:56.123 +0800"
            "1582-01-01 00:00:00.000 +0743"
            "9999-12-31 23:59:59.999 +0800"
            """),
            [GuessDateTime],
            testctx,
        )
        @test testctx.header == [:DATETIME]
        @test testctx.schema == [DateTime]
        @test testctx.results[1].cols[1] == [
            DateTime(2023, 12, 31, 4, 34, 56, 500),
            DateTime(1970, 01, 01, 4, 34, 56, 123),
            DateTime(1581, 12, 31, 16, 17, 0, 0),
            DateTime(9999, 12, 31, 15, 59, 59, 999),
        ]
    end

    # ALTER SESSION UNSET TIMEZONE;
    # CREATE OR REPLACE TABLE timestamp_milli_utc_test_cases (datetime TIMESTAMP_LTZ);
    # INSERT INTO timestamp_milli_utc_test_cases VALUES
    #        (      '2023-12-31 12:34:56.500'::TIMESTAMP_LTZ),
    #        ('1970-01-01 12:34:56.123456789'::TIMESTAMP_LTZ),
    #        ('1582-01-01 00:00:00.000000000'::TIMESTAMP_LTZ),
    #        ('9999-12-31 23:59:59.999999999'::TIMESTAMP_LTZ);
    @testset "timestamps pt 3 UTC" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "DATETIME"
            "2023-12-31 12:34:56.500 Z"
            "1970-01-01 12:34:56.123 Z"
            "1582-01-01 00:00:00.000 Z"
            "9999-12-31 23:59:59.999 Z"
            """),
            [GuessDateTime],
            testctx,
        )
        @test testctx.header == [:DATETIME]
        @test testctx.schema == [DateTime]
        @test testctx.results[1].cols[1] == [
            DateTime(2023, 12, 31, 12, 34, 56, 500),
            DateTime(1970, 01, 01, 12, 34, 56, 123),
            DateTime(1582, 01, 01, 0, 0, 0, 0),
            DateTime(9999, 12, 31, 23, 59, 59, 999),
        ]
    end

    # CREATE OR REPLACE TABLE tricky_headers_test_cases (
    #   myidentifier INT,
    #   MyIdentifier1 INT,
    #   My$identifier INT,
    #   _my_identifier INT,
    #   "MyIdentifier" INT,
    #   "my.identifier" INT,
    #   "my identifier" INT,
    #   "My 'Identifier'" INT,
    #   "3rd_identifier" INT,
    #   "$Identifier" INT,
    #   "идентификатор" INT
    # );
    # INSERT INTO tricky_headers_test_cases VALUES
    #        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    @testset "tricky headers" begin
        testctx = TestContext()
        parse_file(IOBuffer("""
            "MYIDENTIFIER","MYIDENTIFIER1","MY\$IDENTIFIER","_MY_IDENTIFIER","MyIdentifier","my.identifier","my identifier","My 'Identifier'","3rd_identifier","\$Identifier","идентификатор"
            1,2,3,4,5,6,7,8,9,10,11
            """),
            nothing,
            testctx,
        )
        @test testctx.header == [
            :MYIDENTIFIER,
            :MYIDENTIFIER1,
            Symbol("MY\$IDENTIFIER"),
            :_MY_IDENTIFIER,
            :MyIdentifier,
            Symbol("my.identifier"),
            Symbol("my identifier"),
            Symbol("My 'Identifier'"),
            Symbol("3rd_identifier"),
            Symbol("\$Identifier"),
            :идентификатор,
        ]
        @test testctx.schema == fill(PosLen31, 11)
        @test testctx.strings[1][1] == ["1"]
        @test testctx.strings[1][2] == ["2"]
        @test testctx.strings[1][3] == ["3"]
        @test testctx.strings[1][4] == ["4"]
        @test testctx.strings[1][5] == ["5"]
        @test testctx.strings[1][6] == ["6"]
        @test testctx.strings[1][7] == ["7"]
        @test testctx.strings[1][8] == ["8"]
        @test testctx.strings[1][9] == ["9"]
        @test testctx.strings[1][10] == ["10"]
        @test testctx.strings[1][11] == ["11"]
    end
end
