#include <Coordination/Storage/StorageState.h>

#include <Coordination/Storage/Node.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>

#include <algorithm>
#include <mutex>
#include <shared_mutex>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 committed_memtable_size;
    extern const CoordinationSettingsUInt64 memtable_block_size;
    extern const CoordinationSettingsUInt64 uncommitted_memtable_size;
}

namespace Coordination::Storage
{

StorageState::StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_)
    : keeper_context(std::move(keeper_context_)), storage_mutex(storage_mutex_)
{
}

NodeRef StorageState::getOrLoadCommittedNode(const NodePathWithHash & path) const
{
    chassert(!storage_mutex->try_lock());

    const auto * lookup = committed_nodes.find(path.hash);
    if (!lookup)
        return NodeRef{.action = NodeAction::Remove, .offset = 0, .block = nullptr};
    const CommittedNodeInfo & info = lookup->getMapped();

    {
        std::lock_guard guard(info.lock);

        /// `committed_nodes` doesn't contain removed nodes.
        chassert(info.action != NodeAction::Remove);

        if (BlockPtr block = info.block.lock())
            /// Normal fast path: the node is already in memory.
            return NodeRef{.action = info.action, .offset = info.node_offset, .block = std::move(block)};
    }

    /// The block was evicted from the block cache. Memtables keep their blocks alive, so the
    /// node's latest update must be in a file. Find the file by zxid.
    const int64_t zxid = info.last_container_zxid;
    auto file_it = std::partition_point(
        files.begin(), files.end(),
        [zxid](const FileStatePtr & file) { return file->max_zxid < zxid; });
    if (file_it == files.end() || (*file_it)->min_zxid > zxid)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node's block expired, but no file covers zxid {}", zxid);
    const FileState & file = **file_it;

    /// Find the block that covers the path.
    auto block_it = std::partition_point(
        file.blocks.begin(), file.blocks.end(),
        [&](const FileState::BlockInfo & block) { return block.min_path.compare(path.path) <= 0; });
    if (block_it == file.blocks.begin())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node is expected in file {}, but precedes its first block", file.file_path);
    --block_it;
    if (path.path.compare(block_it->max_path) > 0)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Node is expected in file {}, but no block covers its path", file.file_path);
    const uint32_t block_idx = static_cast<uint32_t>(block_it - file.blocks.begin());

    BlockPtr block = file.getOrLoadBlock(block_idx, block_cache ? &*block_cache : nullptr);

    /// Re-point `committed_nodes` entries at the freshly loaded copy of the block, for all nodes
    /// in it. We may be holding storage_mutex in shared mode, so concurrent readers may be doing
    /// the same; that's fine: we only update existing entries (no map rehash), one entry at a time
    /// under its spinlock.
    NodeRef ref{.action = NodeAction::Create, .offset = 0, .block = block};
    std::string path_buf;
    for (uint32_t offset = block->entries_start; offset < block->size;)
    {
        ref.offset = offset;
        NodeBasicInfo basic;
        NodePath node_path;
        ref.read(basic, &node_path, &path_buf, nullptr, nullptr);

        if (const auto * node_lookup = committed_nodes.find(node_path.calculateHash().hash))
        {
            const CommittedNodeInfo & node_info = node_lookup->getMapped();
            /// Don't touch entries whose latest update is in a newer file or memtable.
            if (file.max_zxid >= node_info.last_container_zxid)
            {
                chassert(file.min_zxid <= node_info.last_container_zxid);
                std::lock_guard guard(node_info.lock);
                node_info.block = block;
                node_info.node_offset = offset;
                node_info.action = basic.action;
            }
        }

        offset += basic.serialized_size;
    }

    /// `info` is still valid: the loop above only updated existing `committed_nodes` entries.
    std::lock_guard guard(info.lock);
    BlockPtr loaded = info.block.lock();
    if (!loaded || info.action == NodeAction::Remove)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Node (zxid {}) not found in the expected block of file covering {}-{}", zxid, file.min_zxid, file.max_zxid);
    return NodeRef{.action = info.action, .offset = info.node_offset, .block = std::move(loaded)};
}

NodeRef StorageState::getOrLoadUncommittedNode(const NodePathWithHash & path)
{
    chassert(!uncommitted_mutex.try_lock());

    /// Search uncommitted memtables, newest first. The found NodeRef may be a tombstone
    /// (action == Remove) with a non-null block.
    for (auto it = uncommitted.rbegin(); it != uncommitted.rend(); ++it)
        if (const auto * lookup = it->nodes.find(path.hash))
            return lookup->getMapped();

    std::shared_lock lock(*storage_mutex);
    return getOrLoadCommittedNode(path);
}

void StorageState::appendCommittedNode(FullNode & node, int64_t zxid)
{
    chassert(!storage_mutex->try_lock_shared());

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (!mutable_memtable
        || (zxid > mutable_memtable->max_zxid
            /// (Quirk: this condition will usually pass just after allocating a new block in the memtable.
            ///  So we'll usually finalize the memtable with a nearly empty last block, wasting its capacity.
            ///  That's fine, memtable usually has lots of blocks, this is a tiny waste of memory.)
            && mutable_memtable->total_bytes > settings[DB::CoordinationSetting::committed_memtable_size]))
    {
        if (mutable_memtable)
            immutable_memtables.push_back(std::move(mutable_memtable));

        mutable_memtable = std::make_shared<Memtable>();
        mutable_memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        mutable_memtable->min_zxid = zxid;
        mutable_memtable->max_zxid = zxid;

        /// TODO: Create block_cache (if not memory-only mode) or update its setting if changed.
        /// TODO: Schedule a background flush for the new immutable memtable.
    }

    NodeRef ref = mutable_memtable->appendNode(node, zxid, /*strict=*/ true);

    /// Update `committed_nodes`. (We hold storage_mutex exclusively, so no concurrent readers;
    /// no need for the per-entry spinlocks.)
    const NodePathHash hash = node.path.calculateHash().hash;
    if (auto * lookup = committed_nodes.find(hash))
    {
        CommittedNodeInfo & info = lookup->getMapped();
        std::optional<NodeAction> combined = combineActions(info.action, node.basic.action, /*strict=*/ true);
        if (!combined || *combined == NodeAction::Remove)
        {
            /// `committed_nodes` doesn't keep removed nodes.
            committed_nodes.erase(hash);
        }
        else
        {
            info.last_container_zxid = zxid;
            info.block = ref.block;
            info.node_offset = ref.offset;
            info.action = *combined;
        }
    }
    else
    {
        if (node.basic.action != NodeAction::Create)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Unexpected NodeAction {} for a node that doesn't exist",
                uint32_t(node.basic.action));

        CommittedNodeInfo & info = committed_nodes[hash];
        info.last_container_zxid = zxid;
        info.block = ref.block;
        info.node_offset = ref.offset;
        info.action = NodeAction::Create;
    }
}

ChildrenSet2 StorageState::listCommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena) const
{
    chassert(!storage_mutex->try_lock());

    ChildrenSet2 set;

    /// Oldest to newest, so that strict insertCombine sees each child's history in order.
    for (const FileStatePtr & file : files)
        file->listChildren(path.path, set, out_arena, block_cache ? &*block_cache : nullptr);
    for (const MemtablePtr & memtable : immutable_memtables)
        memtable->listChildren(path, /*strict=*/ true, set, out_arena);
    if (mutable_memtable)
        mutable_memtable->listChildren(path, /*strict=*/ true, set, out_arena);

    return set;
}

void StorageState::appendUncommittedNode(FullNode & node, int64_t zxid)
{
    chassert(!uncommitted_mutex.try_lock());

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (uncommitted.empty()
        || uncommitted.back().memtable->total_bytes > settings[DB::CoordinationSetting::uncommitted_memtable_size])
    {
        UncommittedMemtable u;
        u.memtable = std::make_shared<Memtable>();
        u.memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        u.memtable->min_zxid = zxid;
        u.memtable->max_zxid = zxid;
        uncommitted.push_back(std::move(u));
    }

    UncommittedMemtable & u = uncommitted.back();
    /// strict=false: see the comment at Memtable::appendNode.
    NodeRef ref = u.memtable->appendNode(node, zxid, /*strict=*/ false);
    /// Loose model: the last record for a path wins, including Remove tombstones.
    u.nodes[node.path.calculateHash().hash] = ref;
}

void StorageState::cleanupUncommittedState(int64_t committed_zxid)
{
    chassert(!uncommitted_mutex.try_lock());

    while (!uncommitted.empty() && uncommitted.front().memtable->max_zxid <= committed_zxid)
        uncommitted.erase(uncommitted.begin());
}

ChildrenSet2 StorageState::listUncommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena)
{
    chassert(!uncommitted_mutex.try_lock());

    ChildrenSet2 set;
    {
        std::shared_lock lock(*storage_mutex);
        set = listCommittedChildren(path, out_arena);
    }

    for (const UncommittedMemtable & u : uncommitted)
        u.memtable->listChildren(path, /*strict=*/ false, set, out_arena);

    return set;
}

}
