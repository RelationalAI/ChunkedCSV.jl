using Test
using ChunkedCSV
using Aqua

Threads.nthreads() == 1 && @warn "Running tests with a single thread -- won't be able to spot concurrency issues"

@testset "ChunkedCSV.jl" begin
    # For persistent_tasks, we are getting:
    # ┌ Error: Unexpected error: /var/folders/jq/n4hkdx3968z1qw60dlgzgmnr0000gn/T/jl_YzeXgbOreL/done.log was not created, but precompilation exited
    # └ @ Aqua ~/.julia/packages/Aqua/rTj6Y/src/persistent_tasks.jl:159
    # The error only seem to replicate when running tests
    Aqua.test_all(ChunkedCSV, ambiguities=false, deps_compat=false, persistent_tasks=false)
    Aqua.test_ambiguities(ChunkedCSV) # Our dependencies are not passing this test
    Aqua.test_deps_compat(ChunkedCSV, check_extras=false) # No bounds on extras
    include("guess_datetime.jl")
    include("simple_file_parsing.jl")
    include("exception_handling.jl")
    include("task_result_buffer_tests.jl")
    include("misc_tests.jl")
    include("detect_tests.jl")
end

#=
using Coverage
using ChunkedCSV
pkg_path = pkgdir(ChunkedCSV);
coverage = process_folder(joinpath(pkg_path, "src"));
open(joinpath(pkg_path, "lcov.info"), "w") do io
    LCOV.write(io, coverage)
end;
covered_lines, total_lines = get_summary(coverage);
println("Coverage: $(round(100 * covered_lines / total_lines, digits=2))%");
run(`find $pkg_path -name "*.cov" -type f -delete`);
=#
