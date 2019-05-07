#!/usr/bin/env python
import brawlstartistics as bs
import asyncio
import sys

async def crawl(limit=100):
    client = bs.Client()
    await client.crawl(limit)

def main():
    limit = 100
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])
    print(f"Crawling limit is set to {limit}!")
    asyncio.run(crawl(limit))


if __name__ == "__main__":
    main()
