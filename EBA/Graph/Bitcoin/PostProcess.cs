using EBA.Graph.Bitcoin.Factories;

namespace EBA.Graph.Bitcoin;

public class PostProcess
{
    private readonly Options _options;
    private readonly IGraphDb<BitcoinGraph> _graphDb;
    private readonly ILogger<BitcoinGraphAgent> _logger;

    public PostProcess(Options options, IGraphDb<BitcoinGraph> graphDb, ILogger<BitcoinGraphAgent> logger)
    {
        _options = options;
        _graphDb = graphDb;
        _logger = logger;
    }

    public async Task Run(CancellationToken ct)
    {   
        await SetUtxoSpendingStats(ct);
        await SetTotalSupply();
    }

    private async Task SetUtxoSpendingStats(CancellationToken ct)
    {
        _logger.LogInformation("Setting SpentHeight on Credits edges and computing per-block UTXO stats.");


        var spentPerBlock = await _graphDb.SetSpentHeightOnCreditsAsync(ct);

        // Step 2 – fetch per-block created counts (Credits grouped by CreationHeight).
        var nodeVar = "n";
        var records = await _graphDb.GetNodesAsync(NodeKind.Block, CancellationToken.None, nodeVariable: nodeVar);

        // Step 3 – compute running UTXO set size per block.
        var blocks = new SortedList<long, long>(records.Count);   // height → utxoSetSize
        foreach (var record in records)
        {
            NodeFactory.TryCreate(record[nodeVar].As<Neo4j.Driver.INode>(), out var blockNode);
            var block = (BlockNode)blockNode;
            blocks.Add(block.BlockMetadata.Height, 0);
        }

        long runningUtxoSetSize = 0;
        var updates = new List<Dictionary<string, object?>>(blocks.Count);
        foreach (var height in blocks.Keys)
        {
            // Every Credits edge whose CreationHeight == height is a new UTXO.
            // That count equals the block's total output count minus provably-unspendable ones,
            // but a simple approach: query it or derive from existing metadata.
            // For now, use spentPerBlock to at least track the spent side.
            spentPerBlock.TryGetValue(height, out var spent);

            // TODO: replace 0 with actual created-UTXO count for this block
            //       (e.g., from block metadata OutputCountsStats or a separate query).
            long created = 0;

            runningUtxoSetSize += created - spent;
            blocks[height] = runningUtxoSetSize;

            updates.Add(new Dictionary<string, object?>
            {
                [nameof(BlockMetadata.Height)] = height,
                ["UtxoSetSize"] = runningUtxoSetSize,
                ["UtxoSpent"] = spent
            });
        }

        _logger.LogInformation("Pushing {count:n0} UTXO-per-block updates.", updates.Count);
        await _graphDb.BulkUpdateNodePropertiesAsync(
            NodeKind.Block,
            nameof(BlockMetadata.Height),
            updates,
            CancellationToken.None);
        _logger.LogInformation("Completed UTXO-per-block updates.");
    }

    private async Task SetTotalSupply()
    {
        var blocks = new SortedList<long, BlockNode>();
        var nodeVar = "n";

        _logger.LogInformation("Fetching blocks from graph database.");
        var records = await _graphDb.GetNodesAsync(NodeKind.Block, CancellationToken.None, nodeVariable: nodeVar);
        _logger.LogInformation("Retrieved {count:n0} records from graph database. Creating block nodes.", records.Count);

        foreach (var record in records)
        {
            NodeFactory.TryCreate(record[nodeVar].As<Neo4j.Driver.INode>(), out var blockNode);
            var block = (BlockNode)blockNode;
            blocks.Add(block.BlockMetadata.Height, block);
        }

        if (blocks.First().Value.BlockMetadata.Height != 0)
        {
            throw new InvalidOperationException(
                $"The first block in the graph has height " +
                $"{blocks.First().Value.BlockMetadata.Height:,}, " +
                $"expected 0.");
        }

        if (blocks.Last().Value.BlockMetadata.Height != blocks.Count - 1)
        {
            throw new InvalidOperationException(
                $"This operation requires a continues set of blocks, there are missing blocks");
        }

        blocks[0].BlockMetadata.TotalSupply = blocks[0].BlockMetadata.MintedBitcoins;
        blocks[0].BlockMetadata.TotalSupplyNominal = blocks[0].BlockMetadata.TotalSupply;
        for (var i = 1; i < blocks.Count; i++)
        {
            blocks[i].BlockMetadata.TotalSupply =
                blocks[i - 1].BlockMetadata.TotalSupply +
                blocks[i].TripletTypeValueSum[C2TEdge.Kind] -
                blocks[i].BlockMetadata.ProvablyUnspendableBitcoins;

            blocks[i].BlockMetadata.TotalSupplyNominal =
                blocks[i - 1].BlockMetadata.TotalSupplyNominal + blocks[i].BlockMetadata.MintedBitcoins;
        }

        var updates = blocks.Values.Select(b => new Dictionary<string, object?>
        {
            [nameof(BlockMetadata.Height)] = b.BlockMetadata.Height,
            [nameof(BlockMetadata.TotalSupply)] = b.BlockMetadata.TotalSupply,
            [nameof(BlockMetadata.TotalSupplyNominal)] = b.BlockMetadata.TotalSupplyNominal,
        }).ToList();

        _logger.LogInformation("Pushing {count:n0} block updates to graph database.", updates.Count);
        await _graphDb.BulkUpdateNodePropertiesAsync(
            NodeKind.Block,
            nameof(BlockMetadata.Height),
            updates,
            CancellationToken.None);
        _logger.LogInformation("Completed pushing block updates.");
    }
}
