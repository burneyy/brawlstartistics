#!/usr/bin/env python
import brawlstartistics as bs
import asyncio
import pandas as pd
import logging
from progressbar import progressbar
import sys

logger = logging.getLogger("brawlstartistics")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(filename='logs/lists.log', encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)


newly_added = { "Player" : 0, "Club" : 0 }



#Add club or player to dataframe (if necessary)
def add_to_list(obj, df):
    objType = "Player"
    if "membersCount" in dict(obj):
        objType = "Club"
    if not obj.tag in df.index:
        logger.info(f"{objType} added: {obj.name} ({obj.tag})")
        df = df.append(pd.DataFrame({"added" : pd.Timestamp.now()}, index=[obj.tag]),
                       verify_integrity=True)
        newly_added[objType] += 1

    return df


async def main():

    clubListDf = bs.read_club_list()
    playerListDf = bs.read_player_list()

    async with bs.Client() as client:
        if len(sys.argv) > 1:
            filename = sys.argv[1]
            print(f"Crawling tags in {filename}...")
            playerTags, clubTags = bs.find_tags_in_file(filename)

        else:
            #Crawl Top200 clubs (& players)
            print("Crawling Top 200 club tags...")
            partialClubs = await client.get_leaderboard("clubs")
            clubTags = [ club.tag for club in partialClubs ]

            #Crawl Top200 players
            print("Crawling Top 200 players tags...")
            partialPlayers = await client.get_leaderboard("players")
            playerTags = [ player.tag for player in partialPlayers ]


        print(f"Found {len(playerTags)} player and {len(clubTags)} club tags!")
        print("Performing deep crawling of found tags...")

        players, clubs = await client.crawl(playerTags, clubTags)

    for player in players:
        playerListDf = add_to_list(player, playerListDf)

    for club in clubs:
        clubListDf = add_to_list(club, clubListDf)


    print(clubListDf)
    print(playerListDf)
    bs.save_club_list(clubListDf)
    bs.save_player_list(playerListDf)

    print("\n===========================")
    print(f"Newly added Clubs: {newly_added['Club']}")
    print(f"Newly added Players: {newly_added['Player']}")


# run the async loop
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
