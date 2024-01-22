/*
 * Copyright (C) 2017 CAMELab
 *
 * This file is part of SimpleSSD.
 *
 * SimpleSSD is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SimpleSSD is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SimpleSSD.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "ftl/fast.hh"

#include <algorithm>
#include <limits>
#include <random>

#include "util/algorithm.hh"
#include "util/bitset.hh"

namespace SimpleSSD {

namespace FTL {

FastMapping::FastMapping(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
    : AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      lastFreeBlock(param.pageCountToMaxPerf),
      lastFreeBlockIOMap(param.ioUnitInPage),
      bReclaimMore(false) {
  blocks.reserve(param.totalPhysicalBlocks);
  table.reserve(param.totalLogicalBlocks * param.pagesInBlock);
  bmap.reserve(param.totalLogicalBlocks);

  //logbmap.reserve(7);   //  1 SW, 6 RW
  
  for (uint32_t i = 0; i <= 6; i++) {
    LogMap tmp;
    logbmap.push_back(tmp);
    logbmap[i].lpn.reserve(param.pagesInBlock);
    logbmap[i].lpnStat.reserve(param.pagesInBlock);
    logbmap[i].pbn = -1;
    logbmap[i].freePageCnt = param.pagesInBlock;
  }
  SWflag = -1;  // not alloc yet
  currRW = 1;

  for (uint32_t i = 0; i < param.totalPhysicalBlocks; i++) {
    freeBlocks.emplace_back(Block(i, param.pagesInBlock, param.ioUnitInPage));
  }

  nFreeBlocks = param.totalPhysicalBlocks;

  status.totalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;

  // Allocate free blocks
  //for (uint32_t i = 0; i < param.pageCountToMaxPerf; i++) {
  //  lastFreeBlock.at(i) = getFreeBlock(i);
  //}

  lastFreeBlockIndex = 0;

  memset(&stat, 0, sizeof(stat));

  bRandomTweak = conf.readBoolean(CONFIG_FTL, FTL_USE_RANDOM_IO_TWEAK);
  bitsetSize = bRandomTweak ? param.ioUnitInPage : 1;
}

FastMapping::~FastMapping() {}

bool FastMapping::initialize() {
  uint64_t nPagesToWarmup;
  uint64_t nPagesToInvalidate;
  uint64_t nTotalLogicalPages;
  uint64_t maxPagesBeforeGC;
  uint64_t tick;
  uint64_t valid;
  uint64_t invalid;
  FILLING_MODE mode;

  Request req(param.ioUnitInPage);

  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization started");

  nTotalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;
  nPagesToWarmup =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_FILL_RATIO);
  nPagesToInvalidate =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_INVALID_PAGE_RATIO);
  mode = (FILLING_MODE)conf.readUint(CONFIG_FTL, FTL_FILLING_MODE);
  maxPagesBeforeGC =
      param.pagesInBlock *
      (param.totalPhysicalBlocks *
           (1 - conf.readFloat(CONFIG_FTL, FTL_GC_THRESHOLD_RATIO)) -
       param.pageCountToMaxPerf);  // # free blocks to maintain

  if (nPagesToWarmup + nPagesToInvalidate > maxPagesBeforeGC) {
    warn("ftl: Too high filling ratio. Adjusting invalidPageRatio.");
    nPagesToInvalidate = maxPagesBeforeGC - nPagesToWarmup;
  }

  debugprint(LOG_FTL_PAGE_MAPPING, "Total logical pages: %" PRIu64,
             nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total logical pages to fill: %" PRIu64 " (%.2f %%)",
             nPagesToWarmup, nPagesToWarmup * 100.f / nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total invalidated pages to create: %" PRIu64 " (%.2f %%)",
             nPagesToInvalidate,
             nPagesToInvalidate * 100.f / nTotalLogicalPages);

  req.ioFlag.set();

  // Step 1. Filling
  if (mode == FILLING_MODE_0 || mode == FILLING_MODE_1) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Step 2. Invalidating
  if (mode == FILLING_MODE_0) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else if (mode == FILLING_MODE_1) {
    // Random
    // We can successfully restrict range of LPN to create exact number of
    // invalid pages because we wrote in sequential mannor in step 1.
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nPagesToWarmup - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Report
  calculateTotalPages(valid, invalid);
  debugprint(LOG_FTL_PAGE_MAPPING, "Filling finished. Page status:");
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total valid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             valid, valid * 100.f / nTotalLogicalPages, nPagesToWarmup,
             (int64_t)(valid - nPagesToWarmup));
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total invalid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             invalid, invalid * 100.f / nTotalLogicalPages, nPagesToInvalidate,
             (int64_t)(invalid - nPagesToInvalidate));
  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization finished");

  return true;
}

//done
void FastMapping::read(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    readInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "READ  | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ);
}

void FastMapping::write(Request &req, uint64_t &tick) {
  uint64_t begin = tick;

  if (req.ioFlag.count() > 0) {
    writeInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "WRITE | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE);
}

void FastMapping::trim(Request &req, uint64_t &tick) {}

void FastMapping::format(LPNRange &range, uint64_t &tick) {}

Status *FastMapping::getStatus(uint64_t lpnBegin, uint64_t lpnEnd) {return &status;}

float FastMapping::freeBlockRatio() {
  return (float)nFreeBlocks / param.totalPhysicalBlocks;
}

uint32_t FastMapping::convertBlockIdx(uint32_t blockIdx) {
  return blockIdx % param.pageCountToMaxPerf;
}

//done
uint32_t FastMapping::getFreeBlock() {
  uint32_t blockIndex = 0;

  if (nFreeBlocks > 0) {
    auto iter = freeBlocks.begin();

    blockIndex = iter->getBlockIndex();

    if (blocks.find(blockIndex) != blocks.end()) {
      panic("Corrupted");
    }

    blocks.emplace(blockIndex, std::move(*iter));

    freeBlocks.erase(iter);
    nFreeBlocks--;
  }
  else {
    panic("No free block left");
  }

  return blockIndex;
}

uint32_t FastMapping::getLastFreeBlock(Bitset &iomap) {
  /*
  if (!bRandomTweak || (lastFreeBlockIOMap & iomap).any()) {
    // Update lastFreeBlockIndex
    lastFreeBlockIndex++;

    if (lastFreeBlockIndex == param.pageCountToMaxPerf) {
      lastFreeBlockIndex = 0;
    }

    lastFreeBlockIOMap = iomap;
  }
  else {
    lastFreeBlockIOMap |= iomap;
  }

  auto freeBlock = blocks.find(lastFreeBlock.at(lastFreeBlockIndex));

  // Sanity check
  if (freeBlock == blocks.end()) {
    panic("Corrupted");
  }

  // If current free block is full, get next block
  if (freeBlock->second.getNext//WritePageIndex() == param.pagesInBlock) {
    lastFreeBlock.at(lastFreeBlockIndex) = getFreeBlock(lastFreeBlockIndex);

    bReclaimMore = true;
  }

  return lastFreeBlock.at(lastFreeBlockIndex);
  */
 panic("get last free block?");
 return 0;
}

// calculate weight of each block regarding victim selection policy
void FastMapping::calculateVictimWeight(
    std::vector<std::pair<uint32_t, float>> &weight, const EVICT_POLICY policy,
    uint64_t tick) {
  float temp;

  weight.reserve(blocks.size());

  switch (policy) {
    case POLICY_GREEDY:
    case POLICY_RANDOM:
    case POLICY_DCHOICE:
      for (auto &iter : blocks) {
        if (iter.second.getNextWritePageIndex() != param.pagesInBlock) {
          continue;
        }

        weight.push_back({iter.first, iter.second.getValidPageCountRaw()});
      }

      break;
    case POLICY_COST_BENEFIT:
      for (auto &iter : blocks) {
        if (iter.second.getNextWritePageIndex() != param.pagesInBlock) {
          continue;
        }

        temp = (float)(iter.second.getValidPageCountRaw()) / param.pagesInBlock;

        weight.push_back(
            {iter.first,
             temp / ((1 - temp) * (tick - iter.second.getLastAccessedTime()))});
      }

      break;
    default:
      panic("Invalid evict policy");
  }
}

void FastMapping::selectVictimBlock(std::vector<uint32_t> &list,
                                    uint64_t &tick) {
  static const GC_MODE mode = (GC_MODE)conf.readInt(CONFIG_FTL, FTL_GC_MODE);
  static const EVICT_POLICY policy =
      (EVICT_POLICY)conf.readInt(CONFIG_FTL, FTL_GC_EVICT_POLICY);
  static uint32_t dChoiceParam =
      conf.readUint(CONFIG_FTL, FTL_GC_D_CHOICE_PARAM);
  uint64_t nBlocks = conf.readUint(CONFIG_FTL, FTL_GC_RECLAIM_BLOCK);
  std::vector<std::pair<uint32_t, float>> weight;

  list.clear();

  // Calculate number of blocks to reclaim
  if (mode == GC_MODE_0) {
    // DO NOTHING
  }
  else if (mode == GC_MODE_1) {
    static const float t = conf.readFloat(CONFIG_FTL, FTL_GC_RECLAIM_THRESHOLD);

    nBlocks = param.totalPhysicalBlocks * t - nFreeBlocks;
  }
  else {
    panic("Invalid GC mode");
  }

  // reclaim one more if last free block fully used
  if (bReclaimMore) {
    nBlocks += param.pageCountToMaxPerf;

    bReclaimMore = false;
  }

  // Calculate weights of all blocks
  calculateVictimWeight(weight, policy, tick);

  if (policy == POLICY_RANDOM || policy == POLICY_DCHOICE) {
    uint64_t randomRange =
        policy == POLICY_RANDOM ? nBlocks : dChoiceParam * nBlocks;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, weight.size() - 1);
    std::vector<std::pair<uint32_t, float>> selected;

    while (selected.size() < randomRange) {
      uint64_t idx = dist(gen);

      if (weight.at(idx).first < std::numeric_limits<uint32_t>::max()) {
        selected.push_back(weight.at(idx));
        weight.at(idx).first = std::numeric_limits<uint32_t>::max();
      }
    }

    weight = std::move(selected);
  }

  // Sort weights
  std::sort(
      weight.begin(), weight.end(),
      [](std::pair<uint32_t, float> a, std::pair<uint32_t, float> b) -> bool {
        return a.second < b.second;
      });

  // Select victims from the blocks with the lowest weight
  nBlocks = MIN(nBlocks, weight.size());

  for (uint64_t i = 0; i < nBlocks; i++) {
    list.push_back(weight.at(i).first);
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::SELECT_VICTIM_BLOCK);
}

void FastMapping::doGarbageCollection(std::vector<uint32_t> &blocksToReclaim,
                                      uint64_t &tick) {
  PAL::Request req(param.ioUnitInPage);
  std::vector<PAL::Request> readRequests;
  std::vector<PAL::Request> writeRequests;
  std::vector<PAL::Request> eraseRequests;
  std::vector<uint64_t> lpns;
  Bitset bit(param.ioUnitInPage);
  uint64_t beginAt;
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;

  if (blocksToReclaim.size() == 0) {
    return;
  }

  // For all blocks to reclaim, collecting request structure only
  for (auto &iter : blocksToReclaim) {
    auto block = blocks.find(iter);

    if (block == blocks.end()) {
      panic("Invalid block");
    }

    // Copy valid pages to free block
    for (uint32_t pageIndex = 0; pageIndex < param.pagesInBlock; pageIndex++) {
      // Valid?
      if (block->second.getPageInfo(pageIndex, lpns, bit)) {
        if (!bRandomTweak) {
          bit.set();
        }

        // Retrive free block
        auto freeBlock = blocks.find(getLastFreeBlock(bit));

        // Issue Read
        req.blockIndex = block->first;
        req.pageIndex = pageIndex;
        req.ioFlag = bit;

        readRequests.push_back(req);

        // Update mapping table
        uint32_t newBlockIdx = freeBlock->first;

        for (uint32_t idx = 0; idx < bitsetSize; idx++) {
          if (bit.test(idx)) {
            // Invalidate
            block->second.invalidate(pageIndex, idx);

            auto mappingList = table.find(lpns.at(idx));

            if (mappingList == table.end()) {
              panic("Invalid mapping table entry");
            }

            pDRAM->read(&(*mappingList), 8 * param.ioUnitInPage, tick);

            auto &mapping = mappingList->second.at(idx);

            uint32_t newPageIdx = freeBlock->second.getNextWritePageIndex(idx);

            mapping.first = newBlockIdx;
            mapping.second = newPageIdx;

            freeBlock->second.write(newPageIdx, lpns.at(idx), idx, beginAt);

            // Issue Write
            req.blockIndex = newBlockIdx;
            req.pageIndex = newPageIdx;

            if (bRandomTweak) {
              req.ioFlag.reset();
              req.ioFlag.set(idx);
            }
            else {
              req.ioFlag.set();
            }

            writeRequests.push_back(req);

            stat.validPageCopies++;
          }
        }

        stat.validSuperPageCopies++;
      }
    }

    // Erase block
    req.blockIndex = block->first;
    req.pageIndex = 0;
    req.ioFlag.set();

    eraseRequests.push_back(req);
  }

  // Do actual I/O here
  // This handles PAL2 limitation (SIGSEGV, infinite loop, or so-on)
  for (auto &iter : readRequests) {
    beginAt = tick;

    pPAL->read(iter, beginAt);

    readFinishedAt = MAX(readFinishedAt, beginAt);
  }

  for (auto &iter : writeRequests) {
    beginAt = readFinishedAt;

    pPAL->write(iter, beginAt);

    writeFinishedAt = MAX(writeFinishedAt, beginAt);
  }

  for (auto &iter : eraseRequests) {
    beginAt = readFinishedAt;

    eraseInternal(iter, beginAt);

    eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  }

  tick = MAX(writeFinishedAt, eraseFinishedAt);
  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::DO_GARBAGE_COLLECTION);
}

//done
void FastMapping::readInternal(Request &req, uint64_t &tick) {
  PAL::Request palRequest(req);
  uint64_t beginAt;
  uint64_t finishedAt = tick;
  uint64_t lbn = req.lpn / param.pagesInBlock;
  uint64_t poffset = req.lpn % param.pagesInBlock;

  auto mappingList = bmap.find(lbn);
  uint64_t val = 1;
  //log block
  for (uint64_t i = 0; (i <= 6) && val; i++) {
    for (uint64_t j = 0; j < param.pagesInBlock; j++) {
      if(logbmap[i].pbn == -1) continue;
      auto checkblock = blocks.find(logbmap[i].pbn);
      
      if ((logbmap[i].lpnStat[j] == 1) && (logbmap[i].lpn[j] == req.lpn)) {
        palRequest.blockIndex = logbmap[i].pbn;
        palRequest.pageIndex = j;

        auto block = blocks.find(palRequest.blockIndex);

        if (block == blocks.end()) {
          panic("Block is not in use");
        }
        uint32_t idx = 0;
        beginAt = tick;

        block->second.read(palRequest.pageIndex, idx, beginAt);
        pPAL->read(palRequest, beginAt);

        finishedAt = MAX(finishedAt, beginAt);
            
        tick = finishedAt;
        tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ_INTERNAL);
        val = 0;
        break;
      }
    }
  }
  if (val && (mappingList != bmap.end())) {
    //data block

    palRequest.blockIndex = mappingList->second;
    palRequest.pageIndex = poffset;

    auto block = blocks.find(palRequest.blockIndex);

    if (block == blocks.end()) {
      panic("Block is not in use");
    }
    uint32_t idx = 0;
    beginAt = tick;

    block->second.read(palRequest.pageIndex, idx, beginAt);
    pPAL->read(palRequest, beginAt);

    finishedAt = MAX(finishedAt, beginAt);
        
    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ_INTERNAL);
  }
}

uint64_t FastMapping::getRW() {
  if(logbmap[currRW].freePageCnt == 0) {
      currRW++;
    
      if(currRW == 7) {
        // spill
        return -1;
      }
    }
    
    if(logbmap[currRW].pbn == -1) {
        logbmap[currRW].pbn = getFreeBlock();
        logbmap[currRW].freePageCnt = param.pagesInBlock;
        for (uint64_t i = 0; i < param.pagesInBlock; i++) {
          logbmap[currRW].lpn[i] = 0;
          logbmap[currRW].lpnStat[i] = 0;
        }
    }

    return (currRW);
}

void FastMapping::writeInternal(Request &req, uint64_t &tick, bool sendToPAL) {
  PAL::Request palRequest(req);
  //std::unordered_map<uint32_t, Block>::iterator block;
  uint64_t lbn = req.lpn / param.pagesInBlock;
  uint64_t poffset = req.lpn % param.pagesInBlock;
  auto mappingList = bmap.find(lbn);
  

  uint64_t beginAt;
  uint64_t finishedAt = tick;

  if (mappingList != bmap.end()){ 
    uint64_t pbn = mappingList->second;
  }
  else {
    uint64_t mypbn = getFreeBlock();
    auto myret = bmap.emplace(lbn, mypbn);
    mappingList = myret.first;
  }
  auto block = blocks.find(mappingList->second);
  /*
  if (mappingList != bmap.end()) {
    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      if (req.ioFlag.test(idx) || !bRandomTweak) {
        auto &mapping = mappingList->second.at(idx);

        if (mapping.first < param.totalPhysicalBlocks &&
            mapping.second < param.pagesInBlock) {
          block = blocks.find(mapping.first);

          // Invalidate current page
          block->second.invalidate(mapping.second, idx);
        }
      }
    }
  }
  else {
    // Create empty mapping
    auto ret = table.emplace(
        req.lpn,
        std::vector<std::pair<uint32_t, uint32_t>>(
            bitsetSize, {param.totalPhysicalBlocks, param.pagesInBlock}));

    if (!ret.second) {
      panic("Failed to insert new mapping");
    }

    mappingList = ret.first;
  }
  */
  /*
  if (sendToPAL) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), 8 * req.ioFlag.count(), tick);
      pDRAM->write(&(*mappingList), 8 * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), 8, tick);
      pDRAM->write(&(*mappingList), 8, tick);
    }
  }
  */
  if (block->second.checkErase(poffset) && (poffset >= block->second.getNextWritePageIndex())) {
    //
    uint64_t pbn = mappingList->second;
    block->second.write(poffset, req.lpn, 0, beginAt);
  }
  else {
    // invalidate data in data block
    // and (might) in RW block
    uint64_t pbn = mappingList->second;
    uint64_t inval = 1;
    // invalidiate RW block
    for (uint64_t i = 1; (i < 7); i++) {
      for (uint64_t j = 0; j < param.pagesInBlock; j++) {
        auto checkblock = blocks.find(logbmap[i].pbn);
        //if ((logbmap[i].lpn[j] == req.lpn) && (logbmap[i].lpnStat[j] == 1)) {
        if ((logbmap[i].lpn[j] == req.lpn) && (logbmap[i].lpnStat[j] == 1)) {
          auto block = blocks.find(logbmap[i].pbn);
          block->second.invalidate(j, 0);
          logbmap[i].lpnStat[j] = -1;
          inval = 0;
        }
      }
    }
    {
      //invalidiate data block
      auto block = blocks.find(pbn);
      block->second.invalidate(poffset, 0);
    }

    if (SWflag == -1) {
      auto block = blocks.find(getFreeBlock());

      if (block == blocks.end()) {
        panic("No such block");
      }
      //SW not alloc
      SWflag = 1;
      logbmap[0].pbn = block->first;
      logbmap[0].freePageCnt = param.pagesInBlock;
      logbmap[0].dbn = -1;
      for (uint64_t i = 0; i < param.pagesInBlock; i++) {
        logbmap[0].lpn[i] = 0;
        logbmap[0].lpnStat[i] = 0;
      }
    }
    uint32_t debugtmp = 0;
    if (poffset == 0) {
      // write to SW
      if (logbmap[0].freePageCnt == param.pagesInBlock) {
        // empty SW, directly write
        debugtmp = 1;
        auto debugblock = blocks.find(logbmap[0].pbn);
        if (debugblock == blocks.end()) {
          auto dfblock = blocks.find(getFreeBlock());
          logbmap[0].pbn = dfblock->first;
          logbmap[0].freePageCnt = param.pagesInBlock;
          logbmap[0].dbn = -1;
          for (uint64_t i = 0; i < param.pagesInBlock; i++) {
            logbmap[0].lpn[i] = 0;
            logbmap[0].lpnStat[i] = 0;
          }
        }
      }
      else if (logbmap[0].freePageCnt == 0){
        switchMerge(logbmap[0].pbn, logbmap[0].dbn, beginAt);
        debugtmp = 2;
      }
      else {
        partialMerge(logbmap[0].pbn, logbmap[0].dbn, -1, logbmap[0].freePageCnt, beginAt);
        debugtmp = 3;
      }
      auto block = blocks.find(logbmap[0].pbn);
      uint32_t idx = 0;
      uint32_t pageIndex = block->second.getNextWritePageIndex();
      //auto &mapping = mappingList->second.at(idx);
      beginAt = tick;

      block->second.write(pageIndex, req.lpn, idx, beginAt);

      // update mapping to table
      //logbmap[0].pbn = block->first;
      uint64_t tmpidx = param.pagesInBlock - logbmap[0].freePageCnt;
      logbmap[0].lpn[tmpidx] = req.lpn;
      logbmap[0].lpnStat[tmpidx] = 1;
      logbmap[0].freePageCnt -= 1;
      logbmap[0].dbn = mappingList->second;
      

      if (sendToPAL) {
        palRequest.blockIndex = block->first;
        palRequest.pageIndex = pageIndex;
        /*
        if (bRandomTweak) {
          palRequest.ioFlag.reset();
          palRequest.ioFlag.set(idx);
        }
        else {
          palRequest.ioFlag.set();
        }
        */
        pPAL->write(palRequest, beginAt);
      }

      finishedAt = MAX(finishedAt, beginAt);
    }

    else if (mappingList->second == logbmap[0].dbn) {
      // same SW
      uint64_t tmpidx = param.pagesInBlock - logbmap[0].freePageCnt;
      if (req.lpn == logbmap[0].lpn[tmpidx - 1] + 1) {
        //sequential
        auto block = blocks.find(logbmap[0].pbn);
        uint32_t idx = 0;
        uint32_t pageIndex = block->second.getNextWritePageIndex();
        
        //auto &mapping = mappingList->second.at(idx);
        beginAt = tick;

        block->second.write(pageIndex, req.lpn, idx, beginAt);

        // update mapping to table
        uint64_t tmpidx = param.pagesInBlock - logbmap[0].freePageCnt;
        logbmap[0].lpn[tmpidx] = req.lpn;
        logbmap[0].lpnStat[tmpidx] = 1;
        logbmap[0].freePageCnt -= 1;

        if (sendToPAL) {
          palRequest.blockIndex = block->first;
          palRequest.pageIndex = pageIndex;
          /*
          if (bRandomTweak) {
            palRequest.ioFlag.reset();
            palRequest.ioFlag.set(idx);
          }
          else {
            palRequest.ioFlag.set();
          }
          */
          pPAL->write(palRequest, beginAt);
        }

        finishedAt = MAX(finishedAt, beginAt);
      }
      else if (req.lpn > logbmap[0].lpn[tmpidx - 1] + 1) {
        partialMerge(logbmap[0].pbn, logbmap[0].dbn, req.lpn, logbmap[0].freePageCnt, beginAt);
      }
      else {
        fullMergeSW(req.lpn, beginAt);
      }
    }
    else {
      // RW
      // invalidation done before

      uint64_t nowRW = getRW();
      if (nowRW == -1) {
        beginAt = tick;
        fullMergeRW(1, beginAt);
        currRW = 1;
        nowRW = 1;
      }
      auto block = blocks.find(logbmap[nowRW].pbn);
      uint32_t idx = 0;
      uint32_t pageIndex = block->second.getNextWritePageIndex();
      if (pageIndex == param.pagesInBlock) {
        //RW full
        beginAt = tick;
        fullMergeRW(nowRW, beginAt);
        pageIndex = 0;
      }
      //auto &mapping = mappingList->second.at(idx);
      beginAt = tick;

      block->second.write(pageIndex, req.lpn, idx, beginAt);

      // update mapping to table
      //logbmap[nowRW].pbn = block->first;
      uint64_t tmpidx = param.pagesInBlock - logbmap[nowRW].freePageCnt;
      logbmap[nowRW].lpn[pageIndex] = req.lpn;
      logbmap[nowRW].lpnStat[pageIndex] = 1;
      logbmap[nowRW].freePageCnt -= 1;
      

      if (sendToPAL) {
        palRequest.blockIndex = block->first;
        palRequest.pageIndex = pageIndex;
        /*
        if (bRandomTweak) {
          palRequest.ioFlag.reset();
          palRequest.ioFlag.set(idx);
        }
        else {
          palRequest.ioFlag.set();
        }
        */
        pPAL->write(palRequest, beginAt);
      }

      finishedAt = MAX(finishedAt, beginAt);



    }

  }
  // Exclude CPU operation when initializing
  if (sendToPAL) {
    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE_INTERNAL);
  }


  /*
  // GC if needed
  // I assumed that init procedure never invokes GC
  static float gcThreshold = conf.readFloat(CONFIG_FTL, FTL_GC_THRESHOLD_RATIO);

  if (freeBlockRatio() < gcThreshold) {
    if (!sendToPAL) {
      panic("ftl: GC triggered while in initialization");
    }

    std::vector<uint32_t> list;
    uint64_t beginAt = tick;

    selectVictimBlock(list, beginAt);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "GC   | On-demand | %u blocks will be reclaimed", list.size());

    doGarbageCollection(list, beginAt);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "GC   | Done | %" PRIu64 " - %" PRIu64 " (%" PRIu64 ")", tick,
               beginAt, beginAt - tick);

    stat.gcCount++;
    stat.reclaimedBlocks += list.size();
  }
  */
}

void FastMapping::switchMerge(uint64_t pbn, uint64_t dbn, uint64_t &tick) {
  //  switchMerge for SW
  //  occurs when 1)offset == 0 and 2)SW has no free page
  for(std::unordered_map<uint64_t, uint32_t>::iterator it = bmap.begin(); it!=bmap.end();it++) 
	{
    // 朴素遍历，理论上应该有更快的算法
		if(it->second == dbn) {
      it->second = pbn;
      break;
    }
	} 

  auto dblock = blocks.find(dbn);
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    dblock->second.invalidate(i, 0);
  }
  PAL::Request req(param.ioUnitInPage);
  uint64_t beginAt = tick;
  uint64_t eraseFinishedAt = tick;
  req.blockIndex = dblock->first;
  req.pageIndex = 0;
  req.ioFlag.set();
  eraseInternal(req, beginAt);
  eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  tick = MAX(tick, eraseFinishedAt);

  auto block = blocks.find(getFreeBlock());

  if (block == blocks.end()) {
    panic("No such block");
  }
  //SW not alloc
  SWflag = 1;
  logbmap[0].pbn = block->first;
  logbmap[0].freePageCnt = param.pagesInBlock;
  logbmap[0].dbn = -1;
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    logbmap[0].lpn[i] = 0;
    logbmap[0].lpnStat[i] = 0;
  }
}

void FastMapping::partialMerge(uint64_t pbn, uint64_t dbn, uint64_t lpn, uint64_t fpc, uint64_t &tick) {
  // only for SW
  // occurs when 1) offset == 0 while SW is partially used or
  //             2) same SW, but lpn > tmpidx + 1
  std::unordered_map<uint64_t, uint32_t>::iterator it = bmap.begin();
  uint64_t poffset = lpn % param.pagesInBlock;
  uint64_t tmpdbn;
  PAL::Request palRequest(param.ioUnitInPage);
  for(; it!=bmap.end();it++) 
	{
		if(it->second == dbn) {
      tmpdbn = it->first;
      it->second = pbn;
      break;
    }
	}
  auto block = blocks.find(dbn);
  auto logblock = blocks.find(pbn);
  uint64_t tmpidx = param.pagesInBlock - logbmap[0].freePageCnt; // from here we copy from data block
  
  for (uint64_t i = tmpidx; i < param.pagesInBlock; i++) {
    int tmplpn = i + tmpdbn * param.pagesInBlock;
    uint64_t beginAt = tick;
    if ((lpn != -1) && (i == poffset)) {
      // 和当前写请求重复，按照最新的写
      logblock->second.write(i, lpn, 0, beginAt);
      logbmap[0].freePageCnt--;
      logbmap[0].lpnStat[i] = 1;
      block->second.invalidate(i, 0);
    }
    else if ((block->second).checkValid(i)) {
      // page in data block is valid
      // no need to updata log block information now, since we will
      // translate it to data block
      logbmap[0].freePageCnt--;
      logbmap[0].lpnStat[i] = 1;
      block->second.invalidate(i, 0);
      logblock->second.write(i, tmplpn, 0, beginAt);
    }
    else {
      //page in data block invalid
      //go to RW
      logbmap[0].freePageCnt--;
      logbmap[0].lpnStat[i] = 1;
      uint64_t val = 1;
      for (uint64_t ii = 1; (ii <= 6) && val; ii++) {
        for (uint64_t j = 0; (j < param.pagesInBlock) && val; j++) {
          if (logbmap[ii].pbn == -1) continue;
          auto checkblock = blocks.find(logbmap[ii].pbn);
          if ((logbmap[ii].lpnStat[j] == 1) && (logbmap[ii].lpn[j] == tmplpn)) {
            palRequest.blockIndex = logbmap[ii].pbn;
            palRequest.pageIndex = j;

            auto myblock = blocks.find(palRequest.blockIndex);

            if (myblock == blocks.end()) {
              panic("Block is not in use");
            }
            uint32_t idx = 0;
            logblock->second.write(i, tmplpn, 0, beginAt);
            logbmap[ii].lpnStat[j] = -1;
            myblock->second.invalidate(j, 0);
            val = 0;
            break;
          }
        }
      }
    }
    
  }

  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    block->second.invalidate(i, 0);
  }

  PAL::Request req(param.ioUnitInPage);
  uint64_t beginAt = tick;
  uint64_t eraseFinishedAt = tick;
  req.blockIndex = block->first;
  req.pageIndex = 0;
  req.ioFlag.set();
  eraseInternal(req, beginAt);
  eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  tick = MAX(tick, eraseFinishedAt);

  // alloc a new SW
  auto nblock = blocks.find(getFreeBlock());

  if (nblock == blocks.end()) {
    panic("No such block");
  }
  SWflag = 1;
  logbmap[0].pbn = nblock->first;
  logbmap[0].freePageCnt = param.pagesInBlock;
  logbmap[0].dbn = -1;
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    logbmap[0].lpn[i] = 0;
    logbmap[0].lpnStat[i] = 0;
  }
}

void FastMapping::fullMergeSW(uint64_t lpn, uint64_t &tick) {
  // for SW
  // occurs when same SW while lpn <= tmpidx
  auto fblock = blocks.find(getFreeBlock());  //  free block for full merging
  PAL::Request palRequest(param.ioUnitInPage);
  uint64_t poffset = lpn % param.pagesInBlock;
  uint64_t tmpidx = param.pagesInBlock - logbmap[0].freePageCnt;
  uint64_t beginAt = tick;
  auto swblock = blocks.find(logbmap[0].pbn);
  auto dblock = blocks.find(logbmap[0].dbn);
  for (uint64_t i = 0; i < tmpidx; i++) {
    if (i == poffset) {
      fblock->second.write(i, lpn, 0, beginAt);
      logbmap[0].lpnStat[i] = -1;
      swblock->second.invalidate(i, 0);
      dblock->second.invalidate(i, 0);
    }
    else {
      // from logblock to fblock
      fblock->second.write(i, lpn, 0, beginAt);
      logbmap[0].lpnStat[i] = -1;
      swblock->second.invalidate(i, 0);
      dblock->second.invalidate(i, 0);
    }
  }
  uint64_t dbn = logbmap[0].dbn;
  uint64_t pbn = logbmap[0].pbn;
  auto block = blocks.find(dbn);
  int tmplpn;
  for (uint64_t i = tmpidx; i < param.pagesInBlock; i++) {
    uint64_t beginAt = tick;
    tmplpn = i + dbn * param.pagesInBlock;
    if ((block->second).checkValid(i)) {
      // page in data block is valid
      // no need to updata log block information now, since we will
      // translate it to data block
      fblock->second.write(i, tmplpn, 0, beginAt);
      block->second.invalidate(i, 0);
      swblock->second.invalidate(i, 0);
    }
    else {
      //page in data block invalid
      //go to RW
      uint64_t val = 1;
      for (uint64_t ii = 1; (ii <= 6) && val; ii++) {
        for (uint64_t j = 0; (j < param.pagesInBlock) && val; j++) {
          if (logbmap[ii].pbn == -1) continue;
          auto checkblock = blocks.find(logbmap[ii].pbn);
          if ((logbmap[ii].lpnStat[j] == 1) && (logbmap[ii].lpn[j] == tmplpn)) {
            palRequest.blockIndex = logbmap[ii].pbn;
            palRequest.pageIndex = j;
            logbmap[ii].lpnStat[j] = -1;
            auto block = blocks.find(palRequest.blockIndex);
            block->second.invalidate(j, 0);

            if (block == blocks.end()) {
              panic("Block is not in use");
            }
            uint32_t idx = 0;
            fblock->second.write(i, tmplpn, 0, beginAt);
            val = 0;
            break;
          }
        }
      }
    }
  }

  std::unordered_map<uint64_t, uint32_t>::iterator it = bmap.begin();
  for(; it!=bmap.end();it++) 
  {
    if(it->second == dbn) {
      it->second = fblock->first;
      break;
    }
  }

  PAL::Request req(param.ioUnitInPage);
  beginAt = tick;
  uint64_t eraseFinishedAt = tick;
  req.blockIndex = block->first;
  req.pageIndex = 0;
  req.ioFlag.set();
  eraseInternal(req, beginAt);
  eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  tick = MAX(tick, eraseFinishedAt);

  auto logblock = blocks.find(pbn);
  PAL::Request req2(param.ioUnitInPage);
  req2.blockIndex = logblock->first;
  req2.pageIndex = 0;
  req2.ioFlag.set();
  eraseInternal(req2, beginAt);
  eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  tick = MAX(tick, eraseFinishedAt);

  // alloc a new SW
  auto nblock = blocks.find(getFreeBlock());

  if (nblock == blocks.end()) {
    panic("No such block");
  }
  SWflag = 1;
  logbmap[0].pbn = nblock->first;
  logbmap[0].freePageCnt = param.pagesInBlock;
  logbmap[0].dbn = -1;
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    logbmap[0].lpn[i] = 0;
    logbmap[0].lpnStat[i] = 0;
  }
}

void FastMapping::fullMergeRW(uint64_t logBlkIdx, uint64_t &tick) {
  //  RW merge must be full merge
  PAL::Request palRequest(param.ioUnitInPage);
  uint64_t poffset, lbn;
  uint64_t beginAt = tick;
  uint64_t eraseFinishedAt;
  uint64_t logpbn = logbmap[logBlkIdx].pbn;
  auto logblock = blocks.find(logpbn);
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    if (!logblock->second.checkValid(i)) continue;
    poffset = logbmap[logBlkIdx].lpn[i] % param.pagesInBlock;
    lbn = logbmap[logBlkIdx].lpn[i] / param.pagesInBlock;
    auto mappingList = bmap.find(lbn);
    uint64_t dbn = mappingList->second;
    auto block = blocks.find(mappingList->second);
    
    if (mappingList->second == logbmap[0].dbn) {
      // this data block is connected to SW
      // partial merge first
      partialMerge(logbmap[0].pbn, logbmap[0].dbn, -1, logbmap[0].freePageCnt, tick);
      auto mappingList = bmap.find(lbn);
      auto block = blocks.find(mappingList->second);
    }

    auto fblock = blocks.find(getFreeBlock());  //  free block for full merging
    
    for (uint64_t j = 0; j < param.pagesInBlock; j++) {
      uint64_t tmplpn = j + param.pagesInBlock * mappingList->first;
      //uint64_t tmpppn = j + param.pagesInBlock * mappingList->second;
      if ((block->second).checkValid(j)) {
        // page in data block is valid
        // no need to updata log block information now, since we will
        // translate it to data block
        fblock->second.write(j, tmplpn, 0, beginAt);
        block->second.invalidate(j, 0);
      }
      else {
        //page in data block invalid
        //go to RW
        uint64_t val = 1;
        for (uint64_t ii = 1; (ii <= 6) && val; ii++) {
          for (uint64_t jj = 0; (jj < param.pagesInBlock) && val; jj++) {
            if (logbmap[ii].pbn == -1) continue;
            auto checkblock = blocks.find(logbmap[ii].pbn);
            if (((logbmap[ii].lpnStat[jj] == 1) || (checkblock->second.checkValid(jj))) && (logbmap[ii].lpn[jj] == tmplpn)) {
              palRequest.blockIndex = logbmap[ii].pbn;
              palRequest.pageIndex = jj;
              auto nowblock = blocks.find(palRequest.blockIndex);
              
              if (nowblock == blocks.end()) {
                panic("Block is not in use");
              }
              fblock->second.write(j, tmplpn, 0, beginAt);
              
              logbmap[ii].lpnStat[jj] = -1;
              nowblock->second.invalidate(jj, 0);
              //val = 0;
            }
          }
        }
      }
    }
    
    std::unordered_map<uint64_t, uint32_t>::iterator it = bmap.begin();
    for(; it!=bmap.end();it++) 
    {
      if(it->second == dbn) {
        it->second = fblock->first;
        break;
      }
    }
    
    //mappingList->second = fblock->first;
    PAL::Request req(param.ioUnitInPage);
    uint64_t beginAt = tick;
    uint64_t eraseFinishedAt = tick;
    req.blockIndex = block->first;
    req.pageIndex = 0;
    req.ioFlag.set();
    eraseInternal(req, beginAt);
    eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
    tick = MAX(tick, eraseFinishedAt);


  }

  PAL::Request req2(param.ioUnitInPage);
  req2.blockIndex = logblock->first;
  req2.pageIndex = 0;
  req2.ioFlag.set();
  eraseInternal(req2, beginAt);
  eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  tick = MAX(tick, eraseFinishedAt);

  // alloc a new RW
  auto nblock = blocks.find(getFreeBlock());

  if (nblock == blocks.end()) {
    panic("No such block");
  }
  logbmap[logBlkIdx].pbn = nblock->first;
  logbmap[logBlkIdx].freePageCnt = param.pagesInBlock;
  for (uint64_t i = 0; i < param.pagesInBlock; i++) {
    logbmap[logBlkIdx].lpn[i] = 0;
    logbmap[logBlkIdx].lpnStat[i] = 0;
  }
}


void FastMapping::trimInternal(Request &req, uint64_t &tick) {
  auto mappingList = table.find(req.lpn);

  if (mappingList != table.end()) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), 8 * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), 8, tick);
    }

    // Do trim
    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      auto &mapping = mappingList->second.at(idx);
      auto block = blocks.find(mapping.first);

      if (block == blocks.end()) {
        panic("Block is not in use");
      }

      block->second.invalidate(mapping.second, idx);
    }

    // Remove mapping
    table.erase(mappingList);

    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::TRIM_INTERNAL);
  }
}

void FastMapping::eraseInternal(PAL::Request &req, uint64_t &tick) {
  static uint64_t threshold =
      conf.readUint(CONFIG_FTL, FTL_BAD_BLOCK_THRESHOLD);
  auto block = blocks.find(req.blockIndex);

  // Sanity checks
  if (block == blocks.end()) {
    panic("No such block");
  }

  if (block->second.getValidPageCount() != 0) {
    panic("There are valid pages in victim block");
  }

  // Erase block
  block->second.erase();

  pPAL->erase(req, tick);

  // Check erase count
  uint32_t erasedCount = block->second.getEraseCount();

  if (erasedCount < threshold) {
    // Reverse search
    auto iter = freeBlocks.end();

    while (true) {
      iter--;

      if (iter->getEraseCount() <= erasedCount) {
        // emplace: insert before pos
        iter++;

        break;
      }

      if (iter == freeBlocks.begin()) {
        break;
      }
    }

    // Insert block to free block list
    freeBlocks.emplace(iter, std::move(block->second));
    nFreeBlocks++;
  }

  // Remove block from block list
  blocks.erase(block);

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::ERASE_INTERNAL);
}

float FastMapping::calculateWearLeveling() {
  uint64_t totalEraseCnt = 0;
  uint64_t sumOfSquaredEraseCnt = 0;
  uint64_t numOfBlocks = param.totalLogicalBlocks;
  uint64_t eraseCnt;

  for (auto &iter : blocks) {
    eraseCnt = iter.second.getEraseCount();
    totalEraseCnt += eraseCnt;
    sumOfSquaredEraseCnt += eraseCnt * eraseCnt;
  }

  // freeBlocks is sorted
  // Calculate from backward, stop when eraseCnt is zero
  for (auto riter = freeBlocks.rbegin(); riter != freeBlocks.rend(); riter++) {
    eraseCnt = riter->getEraseCount();

    if (eraseCnt == 0) {
      break;
    }

    totalEraseCnt += eraseCnt;
    sumOfSquaredEraseCnt += eraseCnt * eraseCnt;
  }

  if (sumOfSquaredEraseCnt == 0) {
    return -1;  // no meaning of wear-leveling
  }

  return (float)totalEraseCnt * totalEraseCnt /
         (numOfBlocks * sumOfSquaredEraseCnt);
}

void FastMapping::calculateTotalPages(uint64_t &valid, uint64_t &invalid) {
  valid = 0;
  invalid = 0;

  for (auto &iter : blocks) {
    valid += iter.second.getValidPageCount();
    invalid += iter.second.getDirtyPageCount();
  }
}

void FastMapping::getStatList(std::vector<Stats> &list, std::string prefix) {
  Stats temp;

  temp.name = prefix + "page_mapping.gc.count";
  temp.desc = "Total GC count";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.reclaimed_blocks";
  temp.desc = "Total reclaimed blocks in GC";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.superpage_copies";
  temp.desc = "Total copied valid superpages during GC";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.page_copies";
  temp.desc = "Total copied valid pages during GC";
  list.push_back(temp);

  // For the exact definition, see following paper:
  // Li, Yongkun, Patrick PC Lee, and John Lui.
  // "Stochastic modeling of large-scale solid-state storage systems: analysis,
  // design tradeoffs and optimization." ACM SIGMETRICS (2013)
  temp.name = prefix + "page_mapping.wear_leveling";
  temp.desc = "Wear-leveling factor";
  list.push_back(temp);
}

void FastMapping::getStatValues(std::vector<double> &values) {
  values.push_back(stat.gcCount);
  values.push_back(stat.reclaimedBlocks);
  values.push_back(stat.validSuperPageCopies);
  values.push_back(stat.validPageCopies);
  values.push_back(calculateWearLeveling());
}

void FastMapping::resetStatValues() {
  memset(&stat, 0, sizeof(stat));
}

}  // namespace FTL

}  // namespace SimpleSSD
