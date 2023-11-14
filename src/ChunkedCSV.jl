module ChunkedCSV

export setup_parser, parse_file, ColumnIterator, DebugContext, GuessDateTime
export AbstractConsumeContext, ParsedPayload
export consume!, setup_tasks!, task_done!

import Parsers
using Dates
using FixedPointDecimals
using TimeZones
using SnoopPrecompile
using ChunkedBase
using SentinelArrays.BufferedVectors

# `GuessDateTime` type is used with Parsers.jl to produce a `DateTime`
# from various ISO8601-like formats.
include("type_parsers/datetime_parser.jl")

# Enums used to represent known types that we manually unroll on in populate_result_buffer.jl
# Unrolling, i.e. manually dispatching on specific methods a chain of if-else statements,
# is much easier for the compiler than unrolling on types.
include("Enums.jl")

# Utilities for validating user provided arguments and constructing
# Parsers.Options
include("input_arguments_handling.jl")

#
# Integration with ChunkedBase.jl
#

# How we store the parsed results
include("result_buffer.jl")
# How we parse the results into the result buffer
include("populate_result_buffer.jl")
# How we consume the results from the result buffer
include("consume_contexts.jl")

#
# Input sniffing and setting up the parser
#

include("detect_delim.jl")
include("init_parsing_utils.jl")
include("init_parsing.jl")

#
# Main entrypoints
#

include("parse_file.jl")

# Temporary hack to register new DateTime
function __init__()
    Dates.CONVERSION_TRANSLATIONS[GuessDateTime] = Dates.CONVERSION_TRANSLATIONS[Dates.DateTime]
    return nothing
end

include("precompile.jl")

end # module
