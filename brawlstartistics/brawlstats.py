import os
import aiohttp
import asyncio
import brawlstats
import logging
logger = logging.getLogger("brawlstartistics.brawlstats")

from .constants import BASE_DIR, ALL_BRAWLERS
from .database import Client as DbClient
from .database import Club, Player, UniqueClub, UniquePlayer


#Main object
class Client(brawlstats.Client):
    def __init__(self, **kwargs):
        self.requestcnt = 0
        echo = kwargs.pop("echo", False)

        with open(os.path.join(BASE_DIR, "token.txt"), 'r') as file:
            self.token = file.read().strip()

        self.db = DbClient(echo=echo)
        self.logger = logging.getLogger("brawlstartistics.brawlstats.Client")

        httpconnector = aiohttp.TCPConnector(limit=50) #50 requests at a time
        httpsession = aiohttp.ClientSession(connector=httpconnector)

        super().__init__(self.token, session=httpsession, is_async=True,
                         prevent_ratelimit=True, **kwargs)

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        return self.close()

    def close(self):
        self.db.close()
        return super().close()

    async def aclose(self):
        self.db.close()
        return await super().close()

    async def __aexit__(self, exception_type, exception_value, traceback):
        return await self.aclose()

    #Patch _aget_model to try again and print some messages
    async def _aget_model(self, url, model, key=None):
        obj = None
        consec_errs = 0
        while obj is None:
            try:
                obj = await super()._aget_model(url, model, key)
            except brawlstats.errors.RateLimitError:
                wait = 0.5
                self.logger.info(f"{url}: RateLimitError occurred, waiting...")
                await asyncio.sleep(wait)
            except brawlstats.errors.ServerError as err:
                wait = 60
                consec_errs += 1
                if consec_errs > 5:
                    raise err
                else:
                    self.logger.info(f"{url}: ServerError occurred, waiting {wait}s...")
                    await asyncio.sleep(wait)
            except brawlstats.errors.NotFoundError:
                return None


        return obj


    async def get_players(self, tags):
        results = await asyncio.gather(*(self.get_player(tag) for tag in tags))
        return results

    async def get_clubs(self, tags):
        results = await asyncio.gather(*(self.get_club(tag) for tag in tags))
        return results

    async def crawl(self, player_limit=100):
        #1. Read random tags from database
        unique_players = self.db.get_random_db_entries(Player, player_limit)
        player_tags = [ player.tag for player in unique_players ]

        #2. Update information for these players
        self.logger.info(f"Updating information for {len(player_tags)} random player tags...")
        players = await self.get_players(player_tags)
        players = [ player for player in players if player is not None ]
        self.logger.info(f"Successfully updated information for {len(players)} players.")

        #3. Update information from their clubs
        club_tags = [ p.club.tag for p in players if p.club is not None ]
        self.logger.info(f"Updating information for {len(club_tags)} clubs (of the players)...")
        clubs = await self.get_clubs(club_tags)
        clubs = [ club for club in clubs if club is not None ]
        self.logger.info(f"Successfully updated information for {len(clubs)} clubs.")

        #4. Building database club objects out of them (and updating member information)
        db_clubs = []
        self.logger.info(f"Processing clubs to update their members...")
        for i, club in enumerate(clubs):
            self.logger.info(f"Processing club {i+1} of {len(clubs)} (#{club.tag}) with {club.membersCount} members...")
            db_club = await Club.from_brawlstats(club, self)
            db_clubs.append(db_club)

        #5. Set balanceChangeId
        balance_change_id = self.db.get_last_balance_change()
        if balance_change_id is not None:
            balance_change_id = balance_change_id.id
        brawler_change_ids = {}
        for bname in ALL_BRAWLERS:
            brawler_change_ids[bname] = self.db.get_last_brawler_change(bname)
            if brawler_change_ids[bname] is not None:
                brawler_change_ids[bname] = brawler_change_ids[bname].id
        self.logger.info("Setting balance change ids to {} "
                       "and brawler change ids to {!r}..."
                       "".format(balance_change_id, brawler_change_ids))
        for club in db_clubs:
            club.balanceChangeId = balance_change_id
            for member in club.members:
                member.balanceChangeId = balance_change_id
                for brawler in member.brawlers:
                    brawler.brawlerChangeId = brawler_change_ids[brawler.name]


        self.logger.info(f"Storing crawled clubs, players and brawlers in database...")
        self.db.add_clubs(db_clubs)
        self.db.commit()

        n_players = self.db.get_number_db_entries(UniquePlayer)
        n_clubs = self.db.get_number_db_entries(UniqueClub)

        self.logger.info(f"The database now contains {n_clubs} unique clubs and "
                       f"{n_players} players.")

        return db_clubs
