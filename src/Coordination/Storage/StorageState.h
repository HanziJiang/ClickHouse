#pragma once

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/FileState.h>
#include <Coordination/Storage/Memtable.h>
#include <Common/SharedMutex.h>

namespace Coordination::Storage
{

struct StorageState
{
    struct CommittedNodeInfo
    {
        /// Zxid that can be used to locate the file or memtable containing this node's last update.
        /// Not necessarily the exact zxid of that update, but some zxid within the zxid range of
        /// the correct file or memtable.
        /// (We store zxid instead of file/memtable id because zxid survives flushes and merges.)
        int64_t last_container_zxid = 0;

        /// Cached pointer to the node in memtable or block cache.
        /// If the node is in memtable, this weak ptr is always alive (block held by Memtable), so
        /// we never have to do node lookup in Memtable.
        mutable BlockWeakPtr block;
        mutable uint32_t node_offset = 0; // within `block`, if it's still valid
        mutable NodeAction action = NodeAction::Create;

        /// Protects `block`, `node_offset` and `action`: getOrLoadCommittedNode reassigns them
        /// when reloading an evicted block, under just a shared storage_mutex lock.
        /// (`last_container_zxid` is not protected by this.)
        mutable Spinlock lock;
    };

    struct UncommittedMemtable
    {
        MemtablePtr memtable;
        NodeHashMap<NodeRef> nodes;
    };

    DB::KeeperContextPtr keeper_context;

    /// Can be initialized or resized when switching memtables, with storage_mutex held exclusively.
    /// In memory-only mode stays nullopt (blocks are owned by FileState-s instead).
    /// mutable because BlockCache is internally thread safe, and const readers load blocks into it.
    mutable std::optional<BlockCache> block_cache;

    /// Protects committed state (`files`, `{mutable,immutable}_memtables`, `committed_nodes`, etc).
    DB::SharedMutex * storage_mutex = nullptr;

    /// Protects uncommitted state (`uncommitted`).
    /// Lock order: storage_mutex can be locked while holding uncommitted_mutex, not the other way around.
    std::mutex uncommitted_mutex;

    /// Files and memtables containing committed nodes.
    /// Each one covers a range of zxid-s. The ranges don't overlap. Order is important:
    /// these arrays are ordered by zxid, and all memtables come after all files.
    ///
    /// E.g. to find a node (without using the `committed_nodes` hash map), you'd need to search
    /// mutable_memtable, then immutable_memtables in reverse, then files in reverse, stopping when
    /// the node (or its NodeAction::Remove tombstone) is found.
    std::vector<FileStatePtr> files;
    std::vector<MemtablePtr> immutable_memtables;
    MemtablePtr mutable_memtable; // may be nullptr

    /// Latest occurrence of each node in files and memtables. Doesn't contain removed nodes.
    NodeHashMap<CommittedNodeInfo> committed_nodes;

    /// Uncommitted state, as an overlay on top of committed state.
    /// Contains all uncommitted changes and some recently committed changes (i.e. overlaps committed state).
    /// To find a node, search in these UncommittedMemtable-s in reverse, then in committed state
    /// if not found.
    /// Similarly to regular memtables, we create a new one when the latest one gets big enough.
    /// But these memtables are never flushed to files; instead, a memtable is simply deleted when
    /// its max_zxid gets committed. This vector usually has two elements.
    ///
    /// Uncommitted zxids are mostly unreliable, avoid using them for anything except cleanup.
    /// A request with some zxid may be rolled back, then a different request may be applied with
    /// the same zxid. We don't un-append rolled back entries, we just append the inverse
    /// operation (e.g. Remove -> Create). At memtable boundaries this gets a little awkward to
    /// think about: a change and its rollback change may end up in different memtables; and we
    /// don't roll back memtable's max_zxid, so it may end overestimated and referring to a request
    /// that no longer exists; and memtables' zxid ranges may end up overlapping.
    /// Despite all of that it's safe to delete a memtable when commit point reaches its max_zxid.
    std::vector<UncommittedMemtable> uncommitted;

    explicit StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_);

    /// === Operations on committed state. ===
    /// Caller must hold storage_mutex in shared mode (for const methods) or exclusive mode (for non-const).

    /// Node lookup in committed state.
    ///
    /// Returns NodeRef with action == Remove and block == nullptr if the node doesn't exist.
    /// If the node's block was evicted from the block cache, reloads it (through `block_cache`)
    /// and re-points the affected `committed_nodes` entries at the newly loaded block.
    NodeRef getOrLoadCommittedNode(const NodePathWithHash & path) const;

    void appendCommittedNode(FullNode & node, int64_t zxid);

    /// Merged children set of the node, from all files and memtables. Children names are copied
    /// into `out_arena`. The caller should ignore entries with action == Remove (they can appear
    /// only in the uncommitted variant of this method).
    ChildrenSet2 listCommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena) const;

    /// === Operations on uncommitted state. ===
    /// Caller must hold uncommitted_mutex and must *not* hold storage_mutex (these methods may lock+unlock it).

    /// Node lookup in committed+uncommitted state.
    /// Locks storage_mutex (shared) for committed state lookup if the node is not found in
    /// uncommitted state.
    NodeRef getOrLoadUncommittedNode(const NodePathWithHash & path);

    void appendUncommittedNode(FullNode & node, int64_t zxid);

    /// Call periodically to remove obsolete UncommittedMemtable-s.
    void cleanupUncommittedState(int64_t committed_zxid);

    ChildrenSet2 listUncommittedChildren(const NodePathWithHash & path, DB::Arena & out_arena);
};

}
