include("ChunkedCSV.jl")

# All files have 4 columns, 1M rows, no missing values or tricky things to handle, strings are short (18 chars)
# the parsed data is not returned anywhere, the default consume!() method does nothing with it.
# Currently, we're using a single global buffer, so it is not reflected in the allocations.
parse_file("tst_4xint.csv", [Int,Int,Int,Int])
parse_file("tst_4xdouble.csv", [Float64,Float64,Float64,Float64])
parse_file("tst_4xstr.csv", [String,String,String,String])

@time parse_file("tst_4xint.csv", [Int,Int,Int,Int])
# 0.059793 seconds (464 allocations: 18.239 MiB)
@time parse_file("tst_4xdouble.csv", [Float64,Float64,Float64,Float64])
# 0.062673 seconds (469 allocations: 18.239 MiB)
@time parse_file("tst_4xstr.csv", [String,String,String,String]) # only producing `PosLen`s into the current buffer
# 0.117370 seconds (599 allocations: 8.586 MiB)


import CSV

collect(CSV.read(NoopStream(open("tst_4xint.csv")), NamedTuple, types=[Int,Int,Int,Int], ntasks=Threads.nthreads()));
foreach(identity, CSV.Chunks(NoopStream(open("tst_4xint.csv")), types=[Int,Int,Int,Int], ntasks=Threads.nthreads()));
foreach(identity, CSV.Rows(NoopStream(open("tst_4xint.csv")), types=[Int,Int,Int,Int], reusebuffer=true));

collect(CSV.read(NoopStream(open("tst_4xdouble.csv")), NamedTuple, types=[Float64, Float64, Float64, Float64]));
foreach(identity, CSV.Chunks(NoopStream(open("tst_4xdouble.csv")), types=[Float64, Float64, Float64, Float64], ntasks=Threads.nthreads()));
foreach(identity, CSV.Rows(NoopStream(open("tst_4xdouble.csv")), types=[Float64, Float64, Float64, Float64], reusebuffer=true));

collect(CSV.read(NoopStream(open("tst_4xstr.csv")), NamedTuple, types=[String,String,String,String], ntasks=Threads.nthreads()));
foreach(identity, CSV.Chunks(NoopStream(open("tst_4xstr.csv")), types=[String,String,String,String], ntasks=Threads.nthreads()));
foreach(identity, CSV.Rows(NoopStream(open("tst_4xstr.csv")), types=[String,String,String,String], reusebuffer=true));

@time collect(CSV.read(NoopStream(open("tst_4xint.csv")), NamedTuple, types=[Int,Int,Int,Int], ntasks=Threads.nthreads()));
#  0.060106 seconds (6.25 k allocations: 69.340 MiB)
@time foreach(identity, CSV.Chunks(NoopStream(open("tst_4xint.csv")), types=[Int,Int,Int,Int], ntasks=Threads.nthreads()));
#  0.147805 seconds (6.03 k allocations: 69.325 MiB, 1.67% gc time)
@time foreach(identity, CSV.Rows(NoopStream(open("tst_4xint.csv")), types=[Int,Int,Int,Int], reusebuffer=true));
#  0.219820 seconds (4.01 M allocations: 99.076 MiB, 4.07% gc time)

@time collect(CSV.read(NoopStream(open("tst_4xdouble.csv")), NamedTuple, types=[Float64, Float64, Float64, Float64], ntasks=Threads.nthreads()));
#  0.062281 seconds (6.08 k allocations: 68.631 MiB)
@time foreach(identity, CSV.Chunks(NoopStream(open("tst_4xdouble.csv")), types=[Float64, Float64, Float64, Float64], ntasks=Threads.nthreads()));
#  0.151726 seconds (6.04 k allocations: 68.627 MiB, 1.47% gc time)
@time foreach(identity, CSV.Rows(NoopStream(open("tst_4xdouble.csv")), types=[Float64, Float64, Float64, Float64], reusebuffer=true));
#  0.196413 seconds (4.01 M allocations: 98.691 MiB, 2.56% gc time)

@time collect(CSV.read(NoopStream(open("tst_4xstr.csv")), NamedTuple, types=[String,String,String,String], ntasks=Threads.nthreads()));
#  0.149315 seconds (4.01 M allocations: 268.399 MiB, 18.07% gc time)
@time foreach(identity, CSV.Chunks(NoopStream(open("tst_4xstr.csv")), types=[String,String,String,String], ntasks=Threads.nthreads()));
#  0.322632 seconds (4.01 M allocations: 268.610 MiB, 7.57% gc time)
@time foreach(identity, CSV.Rows(NoopStream(open("tst_4xstr.csv")), types=[String,String,String,String], reusebuffer=true));
#  0.381761 seconds (4.01 M allocations: 221.786 MiB, 2.00% gc time)

# julia> versioninfo()
# Julia Version 1.8.0
# Commit 5544a0fab76 (2022-08-17 13:38 UTC)
# Platform Info:
#   OS: macOS (arm64-apple-darwin21.3.0)
#   CPU: 10 × Apple M1 Max
#   WORD_SIZE: 64
#   LIBM: libopenlibm
#   LLVM: libLLVM-13.0.1 (ORCJIT, apple-m1)
#   Threads: 4 on 8 virtual cores
# Environment:
#   JULIA_EDITOR = code
#   JULIA_NUM_THREADS =