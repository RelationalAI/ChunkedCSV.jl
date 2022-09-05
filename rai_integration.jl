using RAI.UpdateAPI: unsafe_append!
using RAI_VariableSizeStrings: VariableSizeString

include("ChunkedCSV.jl")

struct BeTreeUpsertContext <: AbstractParsingContext
    consumer::RelationConsumer
end

# This is where the parsed results get consumed.
# Users could dispatch on AbstractContext. Currently WIP sketch of what will be needed for RAI.
function consume!(taks_buf::TaskResultBuffer{N}, parsing_ctxs::ParsingContext, row_num::UInt32, context::BeTreeUpsertContext) where {N}
    errsink = context.consumer.errsink
    sinks = context.consumer.sinks
    partition = context.consumer.partition
    schema = parsing_ctxs.schema
    eols = parsing_ctxs.eols.elements
    bytes = parsing_ctxs.bytes
    column_indicators = taks_buf.column_indicators
    cols = taks_buf.cols
    row_statuses = taks_buf.row_statuses
    @inbounds for c in 1:N
        sink = sinks[c]
        type = schema[c]
        if type === Int
            col = getfield(cols[c], :elements)::Vector{Int}
        elseif type === Float64
            col = getfield(cols[c], :elements)::Vector{Float64}
        elseif type === String
            col = getfield(cols[c], :elements)::Vector{Parsers.PosLen}
        else
            @assert false "unreachable"
        end
        row = row_num
        colflag_num = 0
        for r in 1:length(row_statuses)
            row_status = row_statuses.elements[r]
            val = col[r]
            if row_status === NoMissing
                unsafe_append!(sink, (partition, row), (val,))
            elseif row_status === HasMissing
                colflag_num += 1
                flagset(column_indicators[colflag_num], c) && continue
                unsafe_append!(sink, (partition, row), (val,))
            else # error
                c > 1 && continue
                row_str = VariableSizeString(String(bytes[eols[r]:eols[r+1]])) # makes a copy
                # TODO: use column_indicators to indicate the column where we errored on?
                unsafe_append!(errsink, (partition, row, UInt32(N)), (row_str,))
            end
            row += 1
        end
    end
    return nothing
end