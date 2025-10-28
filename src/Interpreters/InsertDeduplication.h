#pragma once

#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Core/Block_fwd.h>

#include <Common/Logger.h>

namespace DB
{

class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

class DeduplicationInfo : public ChunkInfo, public std::enable_shared_from_this<DeduplicationInfo>
{
protected:
    explicit DeduplicationInfo(bool async_insert_);
    DeduplicationInfo(const DeduplicationInfo & other) = default;

public:
    using Ptr = std::shared_ptr<DeduplicationInfo>;

    static Ptr create(bool async_insert_);
    ChunkInfo::Ptr clone() const override;
    Ptr cloneSelf() const;

    bool empty() const;

    struct FilterResult
    {
        Block block;
        Ptr tokens;
        size_t removed_count = 0;
    };
    FilterResult filterSelfDuplicate();

    std::vector<std::string> getBlockIds(const std::string & partition_id) const;
    std::string getBlockId(size_t offset, const std::string & partition_id) const;

    size_t getCount() const;
    size_t getRows() const;

    std::pair<std::string, size_t> debug(size_t offset) const;
    std::string debug() const;

    // for sync insert: if user token is empty then by_part_writer token would be calculated later by part writer
    // for async insert: if user token is empty then by_data_hash token would be calculated later
    void setUserToken(const String & token, size_t count);
    void setSourceBlockNumber(size_t block_number);

    /// use for provide deduplication hash for the part from one partition
    void setPartWriterHashForPartition(const std::string & hash, size_t count) const;
    /// use for provide deduplication hash for the chunk with maybe multiple partitions in it
    void setPartWriterHashes(const std::vector<std::string> & partitions_hashes, size_t count) const;
    /// hash from part writer would be used as user token for dependent views if no user token has been set before
    void redefinePartWriterHashes();

    void setRootViewID(const StorageIDMaybeEmpty & id);
    void setViewID(const StorageID & id);
    void setViewBlockNumber(size_t block_number);
    void rememberPartitionChoise(const std::string & partition_id);

    void setOriginalBlock(const Block & block, InsertDependenciesBuilderConstPtr insert_dependencies_);

    Block deduplicateBlock(const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context);

    const std::string & getLastPartitionChoice() const;
    const std::vector<StorageIDMaybeEmpty> & getVisitedViews() const;

private:
    FilterResult filterOriginalBlock(const std::vector<std::string> & existing_block_ids, const String & partition_id);
    FilterResult filterImpl(const std::set<size_t> & eliminate_offsets, const Block & block);

    Block goRetry(SharedHeader && header, Chunk && data, ContextPtr context);

    std::vector<std::string> getHashesForBlocks(Block & block, String partition_id);

    size_t getTokenBegin(size_t pos) const;

    size_t getTokenEnd(size_t pos) const;

    size_t getTokenRows(size_t pos) const;

    void addExtraPart(const String & part);

    enum BuildingStage
    {
        DEFINE_SOURCE,
        DEFINE_SOURCE_WITH_BLOCK_NUMBER,
        DEFINE_VIEW,
        DEFINE_VIEW_WITH_BLOCK_NUMBER,
    };

    LoggerPtr logger;
    BuildingStage stage = DEFINE_SOURCE;
    bool is_async_insert = false;

    std::vector<size_t> offsets; // points to the last row for each offset

    Block original_block;

    struct TokenDefinition
    {
        std::string by_user;
        std::string by_part_writer;

        static TokenDefinition asUserToken(std::string token);
        void setPartToken(std::string token);

        bool empty() const;
    };
    mutable std::vector<TokenDefinition> tokens;

    std::vector<String> parts;

    std::vector<StorageIDMaybeEmpty> visited_views;
    std::string last_partition_choice;

    InsertDependenciesBuilderConstPtr insert_dependencies;

    void updateAnnotation(const std::string & partition_id) const;
    struct Annotation
    {
        size_t rows = 0;
        std::vector<std::string> block_ids;
        std::unordered_map<std::string, std::vector<size_t>> block_id_to_offsets;

        void clear();
    };
    mutable Annotation annotation;
};

}
