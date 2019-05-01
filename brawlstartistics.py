import os
import aiohttp
import asyncio
import brawlstats
import random
import pandas as pd
import numpy as np
import progressbar
import re
import configparser
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
metadata = Base.metadata
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.orm import relationship, sessionmaker

MAX_TAG_LENGTH = 12
MAX_NAME_LENGTH = 30
MAX_TIME_LENGTH = 20
MAX_REGION_LENGTH = 3

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

FILE_DATA = os.path.join(BASE_DIR, "database/data.h5")
FILE_LISTS = os.path.join(BASE_DIR, "database/lists.h5")

TAG_CHARS = "0289PYLQGRJCUV"

PLAYER_ATTRS = {'tag' : object,
                'datetime' : np.datetime64,
                'clubIdx' : np.int64,
                'name' : object,
                'nameColorCode' : object,
                'brawlersUnlocked' : np.int64,
                'victories' : np.int64,
                'soloShowdownVictories' : np.int64,
                'duoShowdownVictories' : np.int64,
                'totalExp' : np.int64,
                'expFmt' : object,
                'expLevel' : np.int64,
                'trophies' : np.int64,
                'highestTrophies' : np.int64,
                'avatarId' : np.int64,
                'avatarUrl' : object,
                'bestTimeAsBigBrawler' : object,
                'bestRoboRumbleTime' : object,
                'hasSkins' : bool
                }

class BalanceChange(Base):
    __tablename__ = "balance_changes"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    description = Column(Text)
    brawlersAffected = relationship("BrawlerChange", back_populates="balanceChange")
    playerViews = relationship("Player", back_populates="balanceChange")
    clubViews = relationship("Club", back_populates="balanceChange")

class BrawlerChange(Base):
    __tablename__ = "brawler_changes"

    id = Column(Integer, primary_key=True)
    name = Column(String(MAX_NAME_LENGTH))
    type = Column(String(20))  #e.g. new, nerf, buff
    datetime = Column(DateTime)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"))
    balanceChange = relationship("BalanceChange", back_populates="brawlersAffected")
    description = Column(Text)



class UniquePlayer(Base):
    __tablename__ = "player_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

class UniqueClub(Base):
    __tablename__ = "club_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    clubId = Column(Integer, ForeignKey("clubs.id"))
    club = relationship("Club", back_populates="members")
    tag = Column(String(MAX_TAG_LENGTH), ForeignKey("player_list.tag"))
    datetime = Column(DateTime)
    name = Column(String(MAX_NAME_LENGTH))
    nameColorCode = Column(Text)
    brawlers = relationship("Brawler", back_populates="player")
    brawlersUnlocked = Column(Integer)
    victories = Column(Integer)
    soloShowdownVictories = Column(Integer)
    duoShowdownVictories = Column(Integer)
    totalExp = Column(Integer)
    expFmt = Column(Text)
    expLevel = Column(Integer)
    trophies = Column(Integer)
    highestTrophies = Column(Integer)
    avatarId = Column(Integer)
    avatarUrl = Column(Text)
    bestTimeAsBigBrawler = Column(String(MAX_TIME_LENGTH))
    bestRoboRumbleTime = Column(String(MAX_TIME_LENGTH))
    hasSkins = Column(Boolean)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"))
    balanceChange = relationship("BalanceChange", back_populates="playerViews")

CLUB_ATTRS = {'tag' : object,
              'datetime' : np.datetime64,
              'name' : object,
              'region' : object,
              'badgeId' : np.int64,
              'badgeUrl' : object,
              'status' : object,
              'membersCount' : np.int64,
              'onlineMembers' : np.int64,
              'trophies' : np.int64,
              'requiredTrophies' : np.int64,
              'description' : object
              }

class Club(Base):
    __tablename__ = "clubs"

    id = Column(Integer, primary_key=True)
    tag = Column(String(MAX_TAG_LENGTH), ForeignKey("club_list.tag"))
    datetime = Column(DateTime)
    name = Column(String(MAX_NAME_LENGTH))
    region = Column(String(MAX_REGION_LENGTH))
    badgeId = Column(Integer)
    badgeUrl = Column(Text)
    status = Column(Text)
    members = relationship("Player", back_populates="club")
    membersCount = Column(Integer)
    onlineMembers = Column(Integer)
    trophies = Column(Integer)
    requiredTrophies = Column(Integer)
    description = Column(Text)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"))
    balanceChange = relationship("BalanceChange", back_populates="clubViews")


BRAWLER_ATTRS = {'name' : object,
                 'datetime' : np.datetime64,
                 'playerIdx' : np.int64,
                 'hasSkin': bool,
                 'skin' : object,
                 'trophies' : np.int64,
                 'highestTrophies' : np.int64,
                 'power' : np.int64,
                 'rank' : np.int64
                 }

class Brawler(Base):
    __tablename__ = "brawlers"

    id = Column(Integer, primary_key=True)
    playerId = Column(Integer, ForeignKey("players.id"))
    player = relationship("Player", back_populates="brawlers")
    name = Column(String(MAX_NAME_LENGTH))
    datetime = Column(DateTime)
    hasSkin= Column(Boolean)
    skin = Column(String(MAX_NAME_LENGTH))
    trophies = Column(Integer)
    highestTrophies = Column(Integer)
    power = Column(Integer)
    rank = Column(Integer)

def connect_db(echo=True):
    engine = create_engine('mysql+pymysql://burney::0j5eTQapIe5_65aDNe5@localhost/burney_brawlstartistics', echo=echo)
    Session = sessionmaker(bind=engine)
    return (engine, Session())

def to_sql(dataframe, table, con, if_exists="append_ignore", index=True, index_label="id", **kwargs):
    if if_exists == "append_ignore":
        dataframe.to_sql("tempTable", con, if_exists="replace", index=index, index_label=index_label, **kwargs)
        cur = con.cursor()
        cur.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM tempTable;")
        con.commit()
    else:
        dataframe.to_sql(table, con, if_exists=if_exists, index=index, index_label=index_label, **kwargs)

def read_sql(table, con, index_col="id", **kwargs):
    return pd.read_sql_query(f"SELECT * from {table};", con, index_col=index_col, **kwargs)



def get_state(sec, key):
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR, "state.ini"))
    return config[sec][key]

def set_state(sec, key, value):
    config = configparser.ConfigParser()
    inifile = os.path.join(BASE_DIR, "state.ini")
    config.read(inifile)
    config[sec][key] = value
    with open(inifile, "w") as cfgfile:
        config.write(cfgfile)


def my_token():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(thisdir, 'token.txt'), 'r') as file:
        return file.read().strip()


def read(file, key, default):
    try:
        return pd.read_hdf(file, key, mode="a")
    except:
        return default

def save(df, file, key):
    df.to_hdf(file, key, mode="a")

#List of all clubs and players
def create_list():
    df = pd.DataFrame(columns=["tag", "added"])
    df.set_index("tag", inplace=True)
    return df

def read_player_list():
    return read(FILE_LISTS, "players", create_list())

def save_player_list(df):
    save(df, FILE_LISTS, "players")


def read_club_list():
    return read(FILE_LISTS, "clubs", create_list())

def save_club_list(df):
    save(df, FILE_LISTS, "clubs")


#All player/club/brawler current
def read_players():
    return read(FILE_DATA, "players", pd.DataFrame(columns=PLAYER_ATTRS.keys()).astype(PLAYER_ATTRS, copy=False))

def save_players(df):
    save(df, FILE_DATA, "players")

def read_clubs():
    return read(FILE_DATA, "clubs", pd.DataFrame(columns=CLUB_ATTRS.keys()).astype(CLUB_ATTRS, copy=False))

def save_clubs(df):
    save(df, FILE_DATA, "clubs")

def read_brawlers():
    return read(FILE_DATA, "brawlers", pd.DataFrame(columns=BRAWLER_ATTRS.keys()).astype(BRAWLER_ATTRS, copy=False))

def save_brawlers(df):
    save(df, FILE_DATA, "brawlers")

def append(df, rowDict):
    #Remove redundant keys in dict
    rowDict = {k: rowDict[k] for k in df.columns if k in rowDict}
    next_idx = df.index[-1]+1 if len(df.index) > 0 else 0
    return df.append(pd.DataFrame(rowDict, index=[next_idx]), verify_integrity=True)

def random_tag(minlen=3, maxlen=9):
    chars = TAG_CHARS
    length = random.randint(minlen, maxlen)

    return "".join([random.choice(chars) for i in range(length)])

def find_tags_in_file(filename):
    playerTags = []
    clubTags = []
    baseUrl = "https://link.brawlstars.com/invite"
    with open(filename) as f:
        for line in f:
            playerTags.extend(re.findall(baseUrl+r'/friend/\w{2}\?tag=(['+TAG_CHARS+r']{3,9})', line))
            clubTags.extend(re.findall(baseUrl+r'/band/\w{2}\?tag=(['+TAG_CHARS+r']{3,9})', line))

    return (playerTags, clubTags)




class Client(brawlstats.Client):
    def __init__(self, is_async=True):
        #connector = aiohttp.TCPConnector(limit=2)
        #session = aiohttp.ClientSession(connector=connector)
        self._ratelimit = 4 #per second
        self.requestcnt = 0
        session = None
        if is_async:
            self.lock = asyncio.Lock()
            connector = aiohttp.TCPConnector(limit=50)
            session = aiohttp.ClientSession(connector=connector)
        brawlstats.Client.__init__(self, my_token(), session=session, is_async=is_async, timeout=20)

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        brawlstats.Client.close(self)

    async def __aexit__(self, exception_type, exception_value, traceback):
        await brawlstats.Client.close(self)



    async def _aget_model(self, url, model, key=None, progressbar=None):
        obj = None
        consec_errs = 0
        while obj is None:
            try:
                async with self.lock:
                    if progressbar:
                        progressbar.update(min(self.requestcnt, progressbar.max_value))
                        self.requestcnt += 1
                    obj = await brawlstats.Client._aget_model(self, url, model, key)
                    await asyncio.sleep(1./self._ratelimit)
            except brawlstats.errors.RateLimitError:
                print(f"{url}: RateLimitError occurred, waiting...")
                await asyncio.sleep(0.5)
            except brawlstats.errors.ServerError as err:
                print(f"{url}: ServerError occurred, waiting...")
                consec_errs += 1
                if consec_errs > 5:
                    raise err
                else:
                    await asyncio.sleep(4)
            except brawlstats.errors.NotFoundError:
                return None

        return obj


    async def _aget_models(self, urls, model, key=None, progressbar=None):
        results = await asyncio.gather(*(self._aget_model(url, model=model, key=key,
                                                          progressbar=progressbar)
                                         for url in urls))
        return results


    def _get_models(self, urls, model, key=None, progressbar=None):
        self.requestcnt = 0
        if self.is_async:
            return self._aget_models(urls, model=model, key=key, progressbar=progressbar)
        else:
            return [_get_model(url, model=model, key=key)
                    for url in urls]

    def get_players(self, tags, progressbar=None):
        urls = ['{}?tag={}'.format(self.api.PROFILE, tag) for tag in tags]
        return self._get_models(urls, model=brawlstats.Player, progressbar=progressbar)

    def get_clubs(self, tags, progressbar=None):
        urls = ['{}?tag={}'.format(self.api.CLUB, tag) for tag in tags]
        return self._get_models(urls, model=brawlstats.Club, progressbar=progressbar)

    async def crawl(self, playerTags=[], clubTags=[]):
        clubs = []
        players = []

        #1. Get all players
        print(f"Get players for {len(playerTags)} tags...")
        with progressbar.ProgressBar(max_value=len(playerTags)) as bar:
            players = await self.get_players(playerTags, bar)

        players = [ player for player in players if player is not None ]

        nativeClubTags = len(clubTags)
        #2. Get club tags of players
        for player in players:
            if player.club and player.club.tag not in clubTags:
                clubTags.append(player.club.tag)

        #3. Get all clubs
        newClubTags = len(clubTags)-nativeClubTags
        print(f"Get clubs for {nativeClubTags} native and {newClubTags} tags from players...")
        with progressbar.ProgressBar(max_value=len(clubTags)) as bar:
            clubs = await self.get_clubs(clubTags, bar)
        clubs = [ club for club in clubs if club is not None ]

        #4. Get all players in clubs
        print(f"Get (partial) players in crawled clubs...")
        newPlayers = 0
        for club in clubs:
            for member in club.members:
                if member.tag not in playerTags:
                    players.append(member)
                    newPlayers += 1

        print(f"Found {newPlayers} new players in clubs - Done!")

        return (players, clubs)
