#!/usr/bin/env python
import brawlstartistics as bs
import asyncio
import sys
import logging

logger = logging.getLogger("brawlstartistics")
logger.setLevel(logging.INFO)

async def crawl(limit=100):
    async with bs.Client() as client:
        await client.crawl(limit)

def main():
    limit = 100
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
    logger.info(f"Crawling limit is set to {limit}!")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawl(limit))


if __name__ == "__main__":
    main()
