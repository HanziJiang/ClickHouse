#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace Coordination::Storage
{

struct BlockData;
struct NodeBasicInfo;
struct NodeStats2;
struct FullNode;
struct ChildrenSet2;
class BlockCache;

/// SipHash of path string. We rely on not encountering collisions.
using NodePathHash = UInt128;

/// TODO: Make a custom shared_ptr replacement that stores the pointer in control block, so that
///       BlockWeakPtr is 8 bytes, since we store lots of BlockWeakPtr-s in the node map.
using BlockPtr = std::shared_ptr<BlockData>;
using BlockWeakPtr = std::weak_ptr<BlockData>;

/// Minimal 1-byte spinlock, usable with std::lock_guard. Use only for rare and short critical
/// sections (a few instructions).
class Spinlock
{
public:
    Spinlock() = default;

    /// Copying doesn't copy the lock state, the copy starts unlocked. Only copy/assign when no
    /// thread can be holding the lock (e.g. inside a structure that's mutated only exclusively).
    Spinlock(const Spinlock & rhs) { chassert(!rhs.flag.test()); }
    Spinlock & operator=(const Spinlock & rhs) { chassert(!rhs.flag.test()); return *this; } /// NOLINT(cert-oop54-cpp)

    void lock()
    {
        while (flag.test_and_set(std::memory_order_acquire))
            while (flag.test(std::memory_order_relaxed))
                ;
    }

    void unlock() { flag.clear(std::memory_order_release); }

private:
    std::atomic_flag flag;
};

struct BlockAtomicWeakPtr
{
    /// TODO: When custom BlockPtr is implemented, try to shrink this from 24 bytes to 8 bytes by
    ///       packing the spinlock into the upper bit of atomic<BlockRefcounts *>.
    ///       Maybe go overboard and make it an 7-bit rwlock (with 5 bits for reader count; block if
    ///       there are more than 31 concurrent readers), if that sounds fun to you.
    ///       (A lock is still needed to avoid use-after-free if one thread loads the pointer, then
    ///        another thread updates it and deletes the old object, then the first thread
    ///        dereferences the pointer.)
    BlockWeakPtr ptr;
    mutable Spinlock lock;

    BlockAtomicWeakPtr() = default;

    /// Moving is not atomic; only use when no other threads are accessing either side
    /// (e.g. growing a std::vector during FileState construction).
    BlockAtomicWeakPtr(BlockAtomicWeakPtr && other) noexcept : ptr(std::move(other.ptr)) {}
    BlockAtomicWeakPtr & operator=(BlockAtomicWeakPtr && other) noexcept
    {
        ptr = std::move(other.ptr);
        return *this;
    }

    BlockAtomicWeakPtr(const BlockAtomicWeakPtr &) = delete;
    BlockAtomicWeakPtr & operator=(const BlockAtomicWeakPtr &) = delete;

    BlockPtr load() const
    {
        std::lock_guard guard(lock);
        return ptr.lock();
    }

    void store(BlockWeakPtr p)
    {
        std::lock_guard guard(lock);
        ptr = std::move(p);
    }
};

/// What happened to the node in a given file or memtable.
/// Inside one file/memtable, multiple actions are collapsed into one using combineActions.
///
/// There are two possible valid mental models:
///   Loose:  The last (by time of write) node+NodeAction determines the current state of the node.
///           All previous records for the same path are obsolete and ignored.
///   Strict: The sequence of all NodeAction-s that happened at a given path must be well-formed, e.g.
///           [Create, Update, Update, Remove, Create, Remove],
///           not [Remove, ...], not [Create, Create, ...] etc.
///           Consecutive nodes+actions for the same path can be merged into one using combineActions.
///           The merging is associative. The combination of all actions in the node's history
///           determines the current state of the node.
///
/// Strict model exists mostly just for consistency checks, e.g. that we didn't somehow end up
/// creating a node twice in a row. It would be ok to always use loose model and get rid of combineActions.
/// The committed state uses strict model. E.g. when applying deltas and when merging files we use
/// strict combineActions to assert that e.g. we don't try to Update a node that was Remove-d.
/// Inside uncommitted state we mostly use strict model (except that the first action may be Update
/// or Remove). When merging committed and uncommitted lookup results we have to use loose model
/// because there can be overlap between (the zxid ranges of) the two.
enum class NodeAction : uint8_t
{
    Create = 0,
    Update = 1,
    Remove = 2,
    /// TODO: Amend
};

/// What's the combined action of doing `first`, then `second`.
///
/// If `strict` is false, just returns `second` (loose model: the last record wins).
///
/// If `strict` is true:
///   Create + Update = Create
///   Create + Remove = nullopt
///   Update + Update = Update
///   Update + Remove = Remove
///   Remove + Create = Update
///   anything else = throw LOGICAL_ERROR.
///
/// In other words, composition of partial functions on node existence: Create: absent -> present,
/// Update: present -> present, Remove: present -> absent. nullopt is Create + Remove cancelling
/// out, as if the node never existed. Composition of partial functions is associative.
/// (Note that the nullopt annihilation is only valid if `first` really is the first action in the
///  node's entire history, i.e. the node didn't exist before; that's part of what strict means.)
std::optional<NodeAction> combineActions(NodeAction first, NodeAction second, bool strict);

/// Map NodePathHash -> V.
/// Takes advantage of the key already being a hash, so it doesn't need to be hashed again
/// (and no hash is stored in the slot, so a slot is just sizeof(NodePathHash) + sizeof(V)).
template <typename V>
using NodeHashMap = HashMap<NodePathHash, V, UInt128TrivialHash>;

struct NodePathWithHash;

/// Znode path and precalculated number of components in that path.
/// Doesn't own memory, similar to std::string_view.
struct NodePath
{
    uint32_t depth = 0; // number of components in the path: "/" - 0, "/foo" - 1, etc
    uint32_t len = 0;
    const char * str = "";

    NodePathWithHash calculateHash() const;

    /// Path of the parent znode: "/" for "/foo", "/a" for "/a/b".
    /// Must not be called on the root path (depth == 0).
    NodePath parentPath() const
    {
        chassert(depth != 0);
        const size_t last_slash = std::string_view(str, len).rfind('/');
        chassert(last_slash != std::string_view::npos && last_slash + 1 < len);
        return NodePath{.depth = depth - 1, .len = static_cast<uint32_t>(last_slash == 0 ? 1 : last_slash), .str = str};
    }

    /// The last component of the path: "b" for "/a/b". Must not be called on the root path.
    std::string_view baseName() const
    {
        chassert(depth != 0);
        const size_t last_slash = std::string_view(str, len).rfind('/');
        chassert(last_slash != std::string_view::npos && last_slash + 1 < len);
        return std::string_view(str + last_slash + 1, len - last_slash - 1);
    }

    /// Order in which znodes are sorted in files: by (depth, path string).
    /// All children of a node are consecutive in this order.
    int compare(const NodePath & rhs) const
    {
        if (depth != rhs.depth)
            return depth < rhs.depth ? -1 : 1;
        if (int c = memcmp(str, rhs.str, std::min(len, rhs.len)))
            return c;
        return len == rhs.len ? 0 : (len < rhs.len ? -1 : 1);
    }
};

/// NodePath with its hash always calculated. Produced by NodePath::calculateHash; can also be
/// formed directly when the hash is already known: {.path = p, .hash = h}.
struct NodePathWithHash
{
    NodePath path;
    NodePathHash hash {};
};

/// Shared pointer referring to an immutable serialized node (or tombstone) in memory.
/// Can be fully or partially deserialized on demand, which is always thread safe.
struct NodeRef
{
    NodeAction action = NodeAction::Create;
    uint32_t offset = 0; // from start of the block
    BlockPtr block; // may be nullptr if action == Remove

    /// Deserialize the requested paths of the znode, corresponding to non-null output arguments.
    ///
    /// If `out_path` is given, `out_path_buf` must be given too; the full path will be written to
    /// `out_path_buf` (overwriting anything that was already in it), and the resulting NodePath
    /// will point into that string.
    ///
    /// If action == Remove:
    ///  * the caller must ensure that `block` is not nullptr,
    ///  * out_stats is not populated as stats are not stored for node tombstones.
    ///
    /// Inline (defined in Node.h) so that compiler can eliminate branches when called with some
    /// args being const null or always non-null. (Alternatively this could be a template on
    /// bitmask of requested parts, but that's too annoying.)
    void read(NodeBasicInfo & out_basic, NodePath * out_path, std::string * out_path_buf, NodeStats2 * out_stats, std::string_view * out_data) const;

    void readFull(FullNode & out_node, std::string & out_path_buf) const;
};

}
