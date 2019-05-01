#!/usr/bin/env python
import brawlstartistics as bs
import asyncio
import pandas as pd
import logging
from progressbar import progressbar, ProgressBar
import math

logger = logging.getLogger("brawlstartistics")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(filename='logs/fetch.log', encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

CHUNK_SIZE = 100

def get_next_index(df, default=0):
    if len(df.index) > 0:
        return df.index[-1]+1
    else:
        return default

def get_n_chunks(l, n):
    return math.ceil(float(len(l))/n)

def split_in_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

async def main():
    clubTags = bs.read_club_list()
    print(f"Got {len(clubTags)} tags from club list")
    #Remove already fetched tags
    last_club_tag = bs.get_state("fetch", "last_club")
    if last_club_tag != "all":
        reduced = clubTags.loc[last_club_tag:].iloc[1:]
        if len(reduced) > 0:
            clubTags = reduced
        print(f"Last club tag {last_club_tag} -> reduced to {len(clubTags)} tags!")

    clubTags = clubTags.index.values

    playerTags = bs.read_player_list()
    print(f"Got {len(playerTags)} tags from player list")
    #Remove already fetched tags
    last_player_tag = bs.get_state("fetch", "last_player")
    if last_player_tag != "all":
        reduced = playerTags.loc[last_player_tag:].iloc[1:]
        if len(reduced) > 0:
            playerTags = reduced
        print(f"Last player tag {last_player_tag} -> reduced to {len(playerTags)} tags!")

    playerTags = playerTags.index.values


    clubsDf = bs.read_clubs()
    playersDf = bs.read_players()
    brawlersDf = bs.read_brawlers()

    clubIdcs = {}

    fetchedClubs = 0
    fetchedPlayers = 0
    fetchedBrawlers = 0
    print(f"Fetching clubs in chunks of {CHUNK_SIZE}...")
    with ProgressBar(max_value=len(clubTags)) as bar:
        for chunkedTags in split_in_chunks(clubTags, CHUNK_SIZE):
            bar.update(fetchedClubs)
            async with bs.Client() as client:
                clubs = await client.get_clubs(chunkedTags)
            for club in clubs:
                if club is None:
                    continue
                clubDict = dict(club)
                clubIdx = get_next_index(clubsDf, 0)
                clubIdcs[club.tag] = clubIdx
                clubDict["datetime"] = pd.Timestamp.now()
                clubsDf = bs.append(clubsDf, clubDict)
                fetchedClubs += 1

            bs.save_clubs(clubsDf)
            bs.set_state("fetch", "last_club", chunkedTags[-1])


        bar.update(len(clubTags))
        print(clubsDf)


    print(f"Fetching players & brawlers in chunks of {CHUNK_SIZE}...")
    with ProgressBar(max_value=len(playerTags)) as bar:
        for chunkedTags in split_in_chunks(playerTags, CHUNK_SIZE):
            bar.update(fetchedPlayers)
            async with bs.Client() as client:
                players = await client.get_players(chunkedTags)
            for player in players:
                if player is None:
                    continue
                playerDict = dict(player)
                playerIdx = get_next_index(playersDf, 0)
                time = pd.Timestamp.now()
                playerDict["datetime"] = time
                if player.club:
                    playerDict["clubIdx"] = clubIdcs.get(player.club.tag, -1)
                else:
                    playerDict["clubIdx"] = -1
                playersDf = bs.append(playersDf, playerDict)
                fetchedPlayers += 1
                for brawler in player.brawlers:
                    brawlerDict = dict(brawler)
                    brawlerDict["datetime"] = time
                    brawlerDict["playerIdx"] = playerIdx
                    if not brawlerDict["skin"]:
                        brawlerDict["skin"] = "None"
                    brawlersDf = bs.append(brawlersDf, brawlerDict)
                    fetchedBrawlers += 1

            bs.save_players(playersDf)
            bs.set_state("fetch", "last_player", chunkedTags[-1])
            bs.save_brawlers(brawlersDf)

        bar.update(len(playerTags))
        print(playersDf)
        print(brawlersDf)



    print("\n===========================")
    print(f"Fetched Clubs: {fetchedClubs}")
    print(f"Fetched Players: {fetchedPlayers}")
    print(f"Fetched Brawlers: {fetchedBrawlers}")


# run the async loop
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
