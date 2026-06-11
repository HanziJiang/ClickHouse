#include <Coordination/Storage/FileState.h>

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/Memtable.h>
#include <Coordination/Storage/Node.h>
#include <Common/Exception.h>
#include <base/defines.h>

#include <algorithm>

#include <atomic>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Coordination::Storage
{

BlockPtr FileState::getOrLoadBlock(uint32_t block_idx, BlockCache * block_cache) const
{
    chassert(block_idx < blocks.size());
    const BlockInfo & info = blocks[block_idx];

    if (BlockPtr cached = info.data.load())
        return cached;

    chassert(block_cache); // in memory-only mode load() above succeeds because all blocks are pinned

    /// getOrSet deduplicates concurrent loads of the same block.
    BlockPtr block = block_cache->getOrSet(
        BlockCacheKey{.file_id = file_id, .block_idx = block_idx},
        [&] { return loadBlock(block_idx); });

    info.data.store(block);
    return block;
}

void FileState::listChildren(const NodePath & path, ChildrenSet2 & out, DB::Arena & out_arena, BlockCache * block_cache) const
{
    /// Children of `path` are contiguous in the (depth, path string) sort order: they have
    /// depth == path.depth + 1, and their paths start with `path` followed by '/' (just "/" if
    /// `path` is the root, which is the only path that already ends with '/').
    const uint32_t child_depth = path.depth + 1;
    std::string prefix(path.str, path.len);
    if (!prefix.ends_with('/'))
        prefix += '/';

    /// Lower bound of the children range. (No real path is equal to it: paths don't end with '/'.)
    const NodePath range_start{.depth = child_depth, .len = static_cast<uint32_t>(prefix.size()), .str = prefix.data()};

    const auto is_child = [&](const NodePath & p)
    {
        return p.depth == child_depth && p.len > prefix.size() && memcmp(p.str, prefix.data(), prefix.size()) == 0;
    };

    /// The first block that may contain children: the last block with min_path <= range_start
    /// (the range may start in the middle of it), if any.
    auto block_it = std::partition_point(
        blocks.begin(), blocks.end(),
        [&](const BlockInfo & block) { return block.min_path.compare(range_start) <= 0; });
    if (block_it != blocks.begin())
        --block_it;

    std::string path_buf;
    for (; block_it != blocks.end(); ++block_it)
    {
        /// Stop if the block starts past the end of the children range.
        if (block_it->min_path.compare(range_start) > 0 && !is_child(block_it->min_path))
            break;
        /// Skip a block that ends before the start of the range (possible only for the first
        /// considered block).
        if (block_it->max_path.compare(range_start) < 0)
            continue;

        const uint32_t block_idx = static_cast<uint32_t>(block_it - blocks.begin());
        BlockPtr block = getOrLoadBlock(block_idx, block_cache);

        NodeRef ref{.action = NodeAction::Create, .offset = 0, .block = block};
        for (uint32_t offset = block->entries_start; offset < block->size;)
        {
            ref.offset = offset;
            NodeBasicInfo basic;
            NodePath node_path;
            ref.read(basic, &node_path, &path_buf, nullptr, nullptr);
            offset += basic.serialized_size;

            if (node_path.compare(range_start) < 0)
                continue;
            if (!is_child(node_path))
                return; /// past the end of the children range

            out.insertCombine(
                std::string_view(node_path.str + prefix.size(), node_path.len - prefix.size()),
                basic.action, out_arena, /*strict=*/ true);
        }
    }
}

uint32_t FileState::generateFileId()
{
    static std::atomic<uint32_t> next_file_id{1};
    return next_file_id.fetch_add(1, std::memory_order_relaxed);
}

BlockPtr FileState::loadBlock(uint32_t) const
{
    /// TODO: Come up with file format and implement. Remember to assign block's compatible_digest = (digest_version == KEEPER_CURRENT_DIGEST_VERSION).
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Reading blocks from Keeper storage files is not implemented yet");
}

}
