#include <Coordination/Storage/BlockCache.h>

#include <Coordination/Storage/Node.h>

namespace CurrentMetrics
{
    extern const Metric KeeperBlockCacheBytes;
    extern const Metric KeeperBlockCacheBlocks;
}

namespace Coordination::Storage
{

size_t BlockCacheWeightFunction::operator()(const BlockData & block) const
{
    return sizeof(BlockData) + block.capacity;
}

BlockCache::BlockCache(size_t max_size_in_bytes)
    : cache(CurrentMetrics::KeeperBlockCacheBytes, CurrentMetrics::KeeperBlockCacheBlocks, max_size_in_bytes)
{
}

BlockPtr BlockCache::getOrSet(BlockCacheKey key, std::function<BlockPtr()> load_func)
{
    return cache.getOrSet(key.pack(), load_func).first;
}

void BlockCache::insertProbationary(BlockCacheKey key, BlockPtr block)
{
    cache.set(key.pack(), block);
}

}
