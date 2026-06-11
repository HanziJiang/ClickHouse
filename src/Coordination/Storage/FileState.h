#pragma once

#include <Coordination/Storage/Common.h>
#include <Coordination/KeeperCommon.h>
#include <Common/Arena.h>

#include <vector>

namespace Coordination::Storage
{

/// Immutable sorted file.
struct FileState
{
    struct BlockInfo
    {
        uint64_t offset = 0;
        NodePath min_path;
        /// (Should we store max_path like this, or should we use the next block's min_path as upper
        ///  bound for this block's paths? Unclear. Omitting max_path saves memory, but including
        ///  it speeds up lookup if the searched path falls in the gap between blocks (which might
        ///  matter if we choose block boundaries carefully to maximize such gaps; e.g. put the
        ///  boundary where consecutive nodes have the shortest common prefix, within some range of
        ///  allowed block sizes).)
        NodePath max_path;

        /// If in block cache or in pinned_blocks.
        mutable BlockAtomicWeakPtr data;
    };

    String file_path;
    int64_t min_zxid = 0;
    int64_t max_zxid = 0;

    uint32_t serialization_version = 0;
    /// Forward compatibility: the file can be read by readers this old and newer.
    /// E.g. we can add optional fields under Node's varints_size without breaking old readers.
    uint32_t min_compatible_version = 0;
    DB::KeeperDigestVersion digest_version = DB::KeeperDigestVersion::NO_DIGEST;

    /// Unique only within a process, changes on restart.
    uint32_t file_id = generateFileId();

    DB::Arena arena; // for path strings used in `blocks`

    /// BlockInfo::data is the only mutable part of this struct after construction (everything
    /// else must not be mutated after the FileState is published to readers).
    std::vector<BlockInfo> blocks;

    /// If we're in memory-only mode, files are not written to disk. Blocks don't go to BlockCache
    /// and are instead owned by this array to always stay in memory.
    std::vector<BlockPtr> pinned_blocks;

    /// TODO: Consider storing children index.

    /// Gets from cache or reads from file. Thread safe.
    BlockPtr getOrLoadBlock(uint32_t block_idx, BlockCache * block_cache) const;

    /// insertCombine children of this node into `out` (with strict actions, like everything in
    /// committed state), copying the names into `out_arena`.
    void listChildren(const NodePath & path, ChildrenSet2 & out, DB::Arena & out_arena, BlockCache * block_cache) const;

private:
    static uint32_t generateFileId();

    BlockPtr loadBlock(uint32_t block_idx) const;
};
using FileStatePtr = std::shared_ptr<FileState>;

}
