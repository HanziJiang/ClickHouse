#include <gtest/gtest.h>

#include <Coordination/Storage/Node.h>

#include <string>
#include <string_view>
#include <vector>

using namespace Coordination::Storage;

namespace
{

FullNode makeNode(
    NodeAction action,
    uint32_t depth,
    std::string_view path,
    std::string_view data,
    uint32_t acl_id,
    uint32_t version,
    uint32_t num_children,
    bool is_ephemeral,
    NodeStats2 stats)
{
    FullNode node;
    node.basic.action = action;
    node.basic.acl_id = acl_id;
    node.basic.version = version;
    node.basic.setNumChildrenAndIsEphemeral(num_children, is_ephemeral);
    node.stats = stats;
    node.path = NodePath{.depth = depth, .len = static_cast<uint32_t>(path.size()), .str = path.data()};
    node.data = data;
    return node;
}

/// Ephemeral node with data.
FullNode makeNodeA(std::string_view path)
{
    return makeNode(
        NodeAction::Create, 2, path, "hello world", /*acl_id*/ 1, /*version*/ 3, /*num_children*/ 0, /*is_ephemeral*/ true,
        NodeStats2{
            .czxid = 101,
            .mzxid = 205,
            .pzxid = 210,
            .ctime = 1700000000123,
            .mtime = 1700000111456,
            .cversion = 7,
            .aversion = 2,
            .ephemeral_owner_or_seq_num = 777});
}

/// Non-ephemeral node with a seq num and children, empty data.
FullNode makeNodeB(std::string_view path)
{
    return makeNode(
        NodeAction::Update, 2, path, "", /*acl_id*/ 0, /*version*/ 0, /*num_children*/ 5, /*is_ephemeral*/ false,
        NodeStats2{
            .czxid = 300,
            .mzxid = 300,
            .pzxid = 305,
            .ctime = 1,
            .mtime = 2,
            .cversion = 1,
            .aversion = 0,
            .ephemeral_owner_or_seq_num = 4});
}

/// Root node with all-default stats and 1-byte data.
FullNode makeNodeC()
{
    return makeNode(
        NodeAction::Create, 0, "/", "x", /*acl_id*/ 0, /*version*/ 0, /*num_children*/ 2, /*is_ephemeral*/ false, NodeStats2{});
}

void expectNodesEqual(const FullNode & expected, const FullNode & actual)
{
    EXPECT_EQ(expected.basic.action, actual.basic.action);
    /// (Not comparing to expected.basic.data_size - makeNode assigns only `data`, the serializer
    ///  takes the size from there.)
    EXPECT_EQ(expected.data.size(), actual.basic.data_size);
    EXPECT_EQ(expected.basic.acl_id, actual.basic.acl_id);
    EXPECT_EQ(expected.basic.version, actual.basic.version);
    EXPECT_EQ(expected.basic.getNumChildren(), actual.basic.getNumChildren());
    EXPECT_EQ(expected.basic.isEphemeral(), actual.basic.isEphemeral());

    EXPECT_EQ(expected.stats.czxid, actual.stats.czxid);
    EXPECT_EQ(expected.stats.mzxid, actual.stats.mzxid);
    EXPECT_EQ(expected.stats.pzxid, actual.stats.pzxid);
    EXPECT_EQ(expected.stats.ctime, actual.stats.ctime);
    EXPECT_EQ(expected.stats.mtime, actual.stats.mtime);
    EXPECT_EQ(expected.stats.cversion, actual.stats.cversion);
    EXPECT_EQ(expected.stats.aversion, actual.stats.aversion);
    EXPECT_EQ(expected.stats.ephemeral_owner_or_seq_num, actual.stats.ephemeral_owner_or_seq_num);

    EXPECT_EQ(expected.path.depth, actual.path.depth);
    EXPECT_EQ(std::string_view(expected.path.str, expected.path.len), std::string_view(actual.path.str, actual.path.len));
    EXPECT_EQ(expected.data, actual.data);
}

}

TEST(KeeperStorage, NodeSerializationRoundTrip)
{
    const std::string path_a = "/test/digest_a";
    const std::string path_b = "/test/digest_b";
    const std::string path_tomb = "/test/removed";

    FullNode node_a = makeNodeA(path_a);
    FullNode node_b = makeNodeB(path_b);
    FullNode node_c = makeNodeC();
    FullNode tomb;
    tomb.basic.action = NodeAction::Remove;
    tomb.path = NodePath{.depth = 2, .len = static_cast<uint32_t>(path_tomb.size()), .str = path_tomb.data()};

    /// Tiny initial capacity, so that appends exercise block reallocation.
    BlockPtr block = BlockData::create(16);
    block->compatible_digest = true;

    std::vector<NodeRef> refs;
    refs.push_back(BlockData::appendNode(block, node_a));
    refs.push_back(BlockData::appendNode(block, node_b));
    refs.push_back(BlockData::appendNode(block, tomb));
    refs.push_back(BlockData::appendNode(block, node_c));

    /// appendNode assigned digests to the (non-tombstone) input nodes.
    EXPECT_NE(node_a.basic.digest, 0);
    EXPECT_NE(node_b.basic.digest, 0);
    EXPECT_NE(node_c.basic.digest, 0);

    FullNode out;
    std::string path_buf;

    refs[0].readFull(out, path_buf);
    expectNodesEqual(node_a, out);
    EXPECT_EQ(out.basic.digest, node_a.basic.digest);
    EXPECT_EQ(refs[0].offset + out.basic.serialized_size, refs[1].offset);

    refs[1].readFull(out, path_buf);
    expectNodesEqual(node_b, out);
    EXPECT_EQ(out.basic.digest, node_b.basic.digest);
    EXPECT_EQ(refs[1].offset + out.basic.serialized_size, refs[2].offset);

    /// Tombstones store only the path; stats must not be requested.
    NodeBasicInfo tomb_basic;
    NodePath tomb_path;
    refs[2].read(tomb_basic, &tomb_path, &path_buf, nullptr, nullptr);
    EXPECT_EQ(tomb_basic.action, NodeAction::Remove);
    EXPECT_EQ(std::string_view(tomb_path.str, tomb_path.len), path_tomb);
    EXPECT_EQ(tomb_path.depth, 2);
    EXPECT_EQ(tomb_basic.data_size, 0);
    EXPECT_EQ(refs[2].offset + tomb_basic.serialized_size, refs[3].offset);

    refs[3].readFull(out, path_buf);
    expectNodesEqual(node_c, out);
    EXPECT_EQ(refs[3].offset + out.basic.serialized_size, block->size);

    /// Partial read: only basic info and data, no path and stats.
    NodeBasicInfo basic;
    std::string_view data;
    refs[0].read(basic, nullptr, nullptr, nullptr, &data);
    EXPECT_EQ(basic.version, node_a.basic.version);
    EXPECT_EQ(data, node_a.data);

    /// Digests of nodes read from a block with !compatible_digest are discarded.
    /// (refs[0].block is not necessarily `block`: appends may have reallocated into a bigger
    ///  block, while refs keep the blocks they were written to alive.)
    refs[0].block->compatible_digest = false;
    refs[0].readFull(out, path_buf);
    EXPECT_EQ(out.basic.digest, 0);
}

TEST(KeeperStorage, NodeDigestCompatibility)
{
    /// Reference values calculated by KeeperMemNode::getDigest (i.e. calculateDigest in
    /// KeeperStorage.cpp, KeeperDigestVersion::V4) for nodes with the same fields.
    const std::string path_a = "/test/digest_a";
    FullNode node_a = makeNodeA(path_a);
    EXPECT_EQ(node_a.getOrCalculateDigest(), 13507803326533446230ULL);
    EXPECT_EQ(node_a.basic.digest, 13507803326533446230ULL);

    const std::string path_b = "/test/digest_b";
    FullNode node_b = makeNodeB(path_b);
    EXPECT_EQ(node_b.getOrCalculateDigest(), 13631188841398170870ULL);

    FullNode node_c = makeNodeC();
    EXPECT_EQ(node_c.getOrCalculateDigest(), 16444598805986783812ULL);

    /// getOrCalculateDigest doesn't overwrite an already assigned digest.
    node_c.basic.digest = 42;
    EXPECT_EQ(node_c.getOrCalculateDigest(), 42);
    node_c.basic.invalidateDigest();
    EXPECT_EQ(node_c.getOrCalculateDigest(), 16444598805986783812ULL);
}
