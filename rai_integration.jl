using RAI.UpdateAPI: unsafe_append!
using RAI_VariableSizeStrings: VariableSizeString
using Dates
import Parsers

using ChunkedCSV

struct BeTreeUpsertContext <: AbstractConsumeContext
    consumer::RelationConsumer
    lock::ReentrantLock
    BeTreeUpsertContext(consumer::RelationConsumer) = new(consumer, ReentrantLock())
end

# This is where the parsed results get consumed.
# Users could dispatch on AbstractContext. Currently WIP sketch of what will be needed for RAI.
function consume!(taks_buf::ChunkedCSV.TaskResultBuffer{N}, parsing_ctx::ChunkedCSV.ParsingContext, row_num::UInt32, consume_ctx::BeTreeUpsertContext) where {N}
    @lock consume_ctx.lock begin
        errsink = consume_ctx.consumer.errsink
        sinks = consume_ctx.consumer.sinks
        partition = consume_ctx.consumer.partition
        schema = parsing_ctx.schema
        eols = parsing_ctx.eols.elements
        bytes = parsing_ctx.bytes
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
            elseif type === Date
                col = getfield(cols[c], :elements)::Vector{Date}
            elseif type === DateTime
                col = getfield(cols[c], :elements)::Vector{DateTime}
            elseif type === String
                # TODO: Strings need a bit more care (handling escaped chars and converting PosLen to String)
                col = getfield(cols[c], :elements)::Vector{Parsers.PosLen}
            else
                @assert false "unreachable"
            end
            row = row_num
            colflag_num = 0
            for r in 1:length(row_statuses)
                row_status = row_statuses.elements[r]
                val = col[r]
                if row_status === ChunkedCSV.NoMissing
                    unsafe_append!(sink, (partition, row), (val,))
                elseif row_status === ChunkedCSV.HasMissing
                    colflag_num += 1
                    ChunkedCSV.isflagset(column_indicators[colflag_num], c) && continue
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
end