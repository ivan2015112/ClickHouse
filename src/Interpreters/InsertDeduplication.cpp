#include <functional>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <Common/PODArray.h>
#include <Common/ErrorCodes.h>
#include <Common/SipHash.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <fmt/ranges.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_async_executor_for_materialized_views;
}


DeduplicationInfo::FilterResult DeduplicationInfo::filterSelfDuplicate()
{
    LOG_DEBUG(logger, "filterSelfDeduplicate, debug: {}", debug());
    // do not run deduplication for sync insert without async deduplication
    // for sync insert deduplication we need a has from part writer which is set later
    // for async insert we either use usert token or calculate data here
    // updateAnnotation will calculate missing tokens
    if (!is_async_insert)
        return {original_block, shared_from_this(), 0};

    updateAnnotation("no_matter_what_partition_id");
    SCOPE_EXIT({ annotation.clear(); });

    std::set<size_t> fitered_offsets;
    for (auto & [_, block_offsets] : annotation.block_id_to_offsets)
    {
        if (block_offsets.size() > 1)
            fitered_offsets.insert(block_offsets.begin() + 1, block_offsets.end());
    }

    LOG_ERROR(
        logger,
        "Detected duplicate tokens {}, fitered tokens: [{}], debug: {}",
        fitered_offsets.size(),
        fmt::join(fitered_offsets.begin(), fitered_offsets.end(), ","),
        debug());

    return filterImpl(fitered_offsets, original_block);
}


DeduplicationInfo::FilterResult DeduplicationInfo::filterOriginalBlock(const std::vector<std::string> & collisions, const String & partition_id)
{
    chassert(!original_block.empty());

    updateAnnotation(partition_id);

    if (collisions.empty())
        return {original_block, shared_from_this(), 0};

    std::set<size_t> fitered_offsets;

    for (auto & collision :collisions)
    {
        auto it = annotation.block_id_to_offsets.find(collision);
        if (it == annotation.block_id_to_offsets.end())
            continue;
        fitered_offsets.insert(it->second.begin(), it->second.end());
    }

    return filterImpl(fitered_offsets, original_block);
}


DeduplicationInfo::FilterResult DeduplicationInfo::filterImpl(const std::set<size_t> & fitered_offsets, const Block & block)
{
    if (fitered_offsets.empty())
        return {block, shared_from_this(), 0};

    Ptr new_tokens = DeduplicationInfo::create(is_async_insert);
    new_tokens->stage = stage;
    new_tokens->parts = parts;

    size_t remove_count = 0;
    PaddedPODArray<UInt8> filer_column;
    filer_column.resize_fill(block.rows(), 1);

    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (fitered_offsets.find(i) != fitered_offsets.end())
        {
            remove_count += getTokenEnd(i) - getTokenBegin(i);
            for (auto row_id = getTokenBegin(i); row_id < getTokenEnd(i); ++row_id)
                filer_column[row_id] = 0;
        }
        else
        {
            new_tokens->tokens.push_back(tokens[i]);
            new_tokens->offsets.push_back(new_tokens->getRows() + getTokenEnd(i) - getTokenBegin(i));
        }
    }

    new_tokens->setOriginalBlock(block, insert_dependencies);

    if (remove_count == 0)
        return {block, shared_from_this(), 0};

    if (remove_count == block.rows())
    {
        LOG_ERROR(
            logger,
            "All {} rows are removed due to duplicate, debug: {}",
            block.rows(),
            debug());
        return {.block = block.cloneEmpty(), .tokens = new_tokens, .removed_count = remove_count};
    }

    auto cols = block.getColumns();
    for (auto & col : cols)
        col = col->filter(filer_column, block.rows() - remove_count);

    Block filtered_block = block.cloneWithoutColumns();
    filtered_block.setColumns(cols);

    LOG_ERROR(
        logger,
        "Removed {}/ rows with {} tokens due to duplicate in {} tokens, remaining rows={}, debug: {}, new tokens debug: {}",
        remove_count,
        block.rows(),
        fitered_offsets.size(),
        filtered_block.rows(),
        debug(),
        new_tokens->debug());

    chassert(filtered_block.rows() == new_tokens->getRows());

    return {.block = filtered_block, .tokens = new_tokens, .removed_count = remove_count};
}


std::string DeduplicationInfo::getBlockId(size_t offset, const std::string & partition_id) const
{
    chassert(offset < offsets.size());

    // we use by_part_writer as is, partition is already included
    if (!tokens[offset].by_part_writer.empty())
        return tokens[offset].by_part_writer;

    // if user token is empty we calculate by_data_hash
    if (tokens[offset].empty())
    {
        chassert(original_block.rows() == getRows());

        auto cols = original_block.getColumns();

        SipHash hash;
        for (size_t j = getTokenBegin(offset); j < getTokenEnd(offset); ++j)
        {
            for (const auto & col : cols)
                col->updateHashWithValue(j, hash);
        }

        const auto hash_value = hash.get128();
        tokens[offset].by_user = DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]);
    }

    // only one value is set here
    auto token = tokens[offset].by_user;

    // for other token sources addition parts are appended
    for (const auto & part : parts)
    {
        token.append(":");
        token.append(part);
    }

    SipHash hash;
    hash.update(token.data(), token.size());

    const auto hash_value = hash.get128();
    return partition_id + "_" + DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]);
}


void DeduplicationInfo::updateAnnotation(const std::string & partition_id) const
{
    if (annotation.rows == getRows())
        return;

    annotation.clear();
    annotation.rows = getRows();
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        auto block_id = getBlockId(i, partition_id);
        annotation.block_ids.push_back(block_id);
        annotation.block_id_to_offsets[block_id].push_back(i);
    }
}


std::vector<std::string> DeduplicationInfo::getBlockIds(const std::string & partition_id) const
{
    updateAnnotation(partition_id);
    return annotation.block_ids;
}


size_t DeduplicationInfo::getCount() const
{
    return offsets.size();
}


size_t DeduplicationInfo::getRows() const
{
    if (offsets.empty())
        return 0;
    return offsets.back();
}


std::pair<std::string, size_t> DeduplicationInfo::debug(size_t offset) const
{
    chassert(offset < offsets.size());
    if (tokens[offset].empty())
        return {"-", getTokenEnd(offset)};
    else
        return {tokens[offset].by_user + tokens[offset].by_part_writer, getTokenEnd(offset)};
}


std::string DeduplicationInfo::debug() const
{
    std::vector<std::string> token_strs;
    for (const auto & token : tokens)
    {
        if (!token.by_user.empty())
            token_strs.push_back("user<" + token.by_user + ">");
        else if (!token.by_part_writer.empty())
            token_strs.push_back("part-hash<" + token.by_part_writer + ">");
        else
            token_strs.push_back("-");
    }

    return fmt::format(
        "inst: {}, rows count: {}, rows in block: {}, parts: [{}], tokens: {}:[{}], visited views: {}:[{}], chiosen partitions: {}",
        size_t(this), getRows(), original_block.rows(), fmt::join(parts, ","), getCount(), fmt::join(token_strs, ","), visited_views.size(), fmt::join(visited_views, ","), last_partition_choice.size());
}


DeduplicationInfo::Ptr DeduplicationInfo::create(bool async_insert_)
{
    struct make_shared_enabler : public DeduplicationInfo
    {
        explicit make_shared_enabler(bool async_insert)
            : DeduplicationInfo(async_insert)
        {
        }
    };
    return std::make_shared<make_shared_enabler>(async_insert_);
}


DeduplicationInfo::Ptr DeduplicationInfo::cloneSelf() const
{
    struct make_shared_enabler : public DeduplicationInfo
    {
        explicit make_shared_enabler(const DeduplicationInfo & inst)
            : DeduplicationInfo(inst)
        {
        }
    };
    return std::make_shared<make_shared_enabler>(*this);
}


ChunkInfo::Ptr DeduplicationInfo::clone() const
{
    return std::static_pointer_cast<ChunkInfo>(cloneSelf());
}


void DeduplicationInfo::setPartWriterHashForPartition(const std::string & hash, size_t) const
{
    if (is_async_insert)
        return;

    chassert(getCount() == 1);

    if (!tokens[0].empty())
        return;

    tokens[0].setPartToken(std::move(hash));

    LOG_TRACE(
        logger,
        "setPartWriterHashForPartition: hash='{}', debug: {}",
        hash,
        debug());
}


void DeduplicationInfo::setPartWriterHashes(const std::vector<std::string> & partitions_hashes, size_t count) const
{
    if (is_async_insert)
        return;

    chassert(getCount() == 1);

    if (!tokens[0].empty())
        return;

    tokens[0].setPartToken(fmt::format("all-partitions-{}", fmt::join(partitions_hashes, "-")));

    LOG_TRACE(
        logger,
        "setPartWriterHashes: token='{}', debug: {}",
        tokens[0].by_part_writer,
        debug());

    chassert(getRows() == count);
}


void DeduplicationInfo::redefinePartWriterHashes()
{
    if (is_async_insert)
        return;

    chassert(getCount() == 1);
    chassert(!tokens[0].empty());

    if (!tokens[0].by_part_writer.empty())
    {
        tokens[0].by_user = std::move(tokens[0].by_part_writer);
        tokens[0].by_part_writer.clear();
    }
}


DeduplicationInfo::DeduplicationInfo(bool async_insert_)
    : logger(getLogger("DedupInfo"))
    , is_async_insert(async_insert_)
{
    LOG_TRACE(logger, "Create DeduplicationInfo, debug: {}", debug());
}


void DeduplicationInfo::setUserToken(const String & token, size_t count)
{
    tokens.push_back(TokenDefinition::asUserToken(token));
    offsets.push_back(getRows() + count);

    LOG_TRACE(logger, "setUserToken: token='{}', {}", token, debug());
}


DeduplicationInfo::TokenDefinition DeduplicationInfo::TokenDefinition::asUserToken(std::string token)
{
    TokenDefinition t;
    t.by_user = std::move(token);
    return t;
}


void DeduplicationInfo::TokenDefinition::setPartToken(std::string token)
{
    if (!empty())
        return;
    by_part_writer = std::move(token);
}


namespace
{
using FilterPredicate = std::function<bool (const Chunk &)>;

template <typename Executor>
Chunk exec(QueryPipeline & pipeline, FilterPredicate predicate)
{
    auto executor = Executor(pipeline);
    bool is_done = false;
    Chunk chunk;
    while (!is_done && !predicate(chunk))
    {
        is_done = !executor.pull(chunk);
    }
    return chunk;
}
}


Block DeduplicationInfo::goRetry(SharedHeader && header, Chunk && data, ContextPtr context)
{
    auto builder = QueryPipelineBuilder();
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(data))));
    builder.addChain(insert_dependencies->createChainForDeduplicationRetry(*this));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    chassert(pipeline.pulling());

    auto result_header = pipeline.getSharedHeader();

    auto filter =[this] (const Chunk & chunk) -> bool
    {
        if (!chunk)
            return false;

        auto info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();

        LOG_DEBUG(this->logger, "examine chunk with deduplication info: {}", info->debug());
        return info->parts == this->parts;
    };

    auto result_chunk = context->getSettingsRef()[Setting::use_async_executor_for_materialized_views]
        ? exec<PullingAsyncPipelineExecutor>(pipeline, std::move(filter))
        : exec<PullingPipelineExecutor>(pipeline, std::move(filter));

    if (!result_chunk)
        return result_header->cloneEmpty();

    return result_header->cloneWithColumns(result_chunk.detachColumns());
}


void DeduplicationInfo::setOriginalBlock(const Block & block, InsertDependenciesBuilderConstPtr insert_dependencies_)
{
    original_block = block;
    insert_dependencies = std::move(insert_dependencies_);
}


void DeduplicationInfo::setRootViewID(const StorageIDMaybeEmpty & id)
{
    LOG_DEBUG(logger, "Setting root view ID '{}' in deduplication tokens", id);
    if (!id.empty())
    {
        auto as_str = id.hasUUID() ? toString(id.uuid) : id.getFullNameNotQuoted();
        addExtraPart(fmt::format("root-view-id-{}", as_str));
    }
    else
    {
        addExtraPart("root-view-id-empty");
    }
    visited_views.push_back(id);
}


void DeduplicationInfo::setViewID(const StorageID & id)
{
    LOG_DEBUG(logger, "Setting view ID '{}' in deduplication tokens", id);
    auto as_str = id.hasUUID() ? toString(id.uuid) : id.getFullNameNotQuoted();
    addExtraPart(fmt::format("view-id-{}", as_str));
    visited_views.push_back(id);

    // part hash is used only for deduplication in target table
    // after that we need to account all added parts
    redefinePartWriterHashes();
}


void DeduplicationInfo::rememberPartitionChoise(const std::string & partition_id)
{
    chassert(!visited_views.empty());
    LOG_DEBUG(
        logger,
        "Remembering partition choice '{}' for view '{}', debug: {}",
        partition_id,
        visited_views.back(),
        debug());
    last_partition_choice = partition_id;
}


void DeduplicationInfo::setViewBlockNumber(size_t block_number)
{
    addExtraPart(fmt::format("view-block-{}", block_number));
}


Block DeduplicationInfo::deduplicateBlock(
    const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context)
{
    auto result = filterOriginalBlock(existing_block_ids, partition_id);
    chassert(result.block.rows() + result.removed_count == original_block.rows());
    chassert(result.removed_count > 0);

    auto chunk = Chunk(result.block.getColumns(), result.block.rows());
    chunk.getChunkInfos().add(result.tokens);

    auto header = std::make_shared<const Block>(result.block.cloneEmpty());

    /// TODO: do not go retry if no rows are left
    return goRetry(std::move(header), std::move(chunk), context);
}


const std::vector<StorageIDMaybeEmpty> & DeduplicationInfo::getVisitedViews() const
{
    return visited_views;
}


const std::string & DeduplicationInfo::getLastPartitionChoice() const
{
    return last_partition_choice;
}


void DeduplicationInfo::setSourceBlockNumber(size_t block_number)
{
    addExtraPart(fmt::format("source-number-{}", block_number));
    stage = DEFINE_VIEW;
}


bool DeduplicationInfo::empty() const
{
    return offsets.empty();
}


size_t DeduplicationInfo::getTokenBegin(size_t pos) const
{
    chassert(!empty());
    chassert(pos < offsets.size());
    if (pos == 0)
        return 0;
    return offsets[pos - 1];
}


size_t DeduplicationInfo::getTokenEnd(size_t pos) const
{
    chassert(!empty());
    chassert(pos < offsets.size());
    return offsets[pos];
}


size_t DeduplicationInfo::getTokenRows(size_t pos) const
{
    return getTokenEnd(pos) - getTokenBegin(pos);
}


void DeduplicationInfo::addExtraPart(const String & part)
{
    parts.push_back(part);
}


bool DeduplicationInfo::TokenDefinition::empty() const
{
    return by_user.empty() && by_part_writer.empty();
}


void DeduplicationInfo::Annotation::clear()
{
    rows = 0;
    block_ids.clear();
    block_id_to_offsets.clear();
}
}
