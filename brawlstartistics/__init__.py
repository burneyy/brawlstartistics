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
import datetime

#SQLALCHEMY
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
Base = declarative_base()
metadata = Base.metadata

#Global variables
MAX_TAG_LENGTH = 12
MAX_NAME_LENGTH = 30
MAX_TIME_LENGTH = 20
MAX_REGION_LENGTH = 3

TAG_CHARS = "0289PYLQGRJCUV"


class BalanceChange(Base):
    __tablename__ = "balance_changes"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    description = Column(Text)

    def __repr__(self):
        return "<BalanceChange {}: {}>".format(self.datetime.date(), self.description)

class BrawlerChange(Base):
    __tablename__ = "brawler_changes"

    id = Column(Integer, primary_key=True)
    name = Column(String(MAX_NAME_LENGTH))
    type = Column(String(20))  #e.g. new, nerf, buff
    datetime = Column(DateTime)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"))
    description = Column(Text)

    def __repr__(self):
        return "<BrawlerChange {}: {} ({}, {})>".format(
            self.datetime.date(), self.description, self.name, self.type)

BrawlerChange.balanceChange = relationship("BalanceChange", back_populates="brawlersAffected")
BalanceChange.brawlersAffected = relationship("BrawlerChange", back_populates="balanceChange")

class UniquePlayer(Base):
    __tablename__ = "player_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

    def __repr__(self):
        return "<UniquePlayer {}: {}>".format(
           self. added.date(), self.tag)

class UniqueClub(Base):
    __tablename__ = "club_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

    def __repr__(self):
        return "<UniqueClub {}: {}>".format(
            self.added.date(), self.tag)


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    clubId = Column(Integer, ForeignKey("clubs.id"))
    tag = Column(String(MAX_TAG_LENGTH), ForeignKey("player_list.tag"))
    datetime = Column(DateTime)
    name = Column(String(MAX_NAME_LENGTH))
    nameColorCode = Column(Text)
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

    def __repr__(self):
        return "<Player {}: {}>".format(
            self.datetime.date(), self.tag)

Player.balanceChange = relationship("BalanceChange", back_populates="playerViews")
BalanceChange.playerViews = relationship("Player", back_populates="balanceChange")


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
    membersCount = Column(Integer)
    onlineMembers = Column(Integer)
    trophies = Column(Integer)
    requiredTrophies = Column(Integer)
    description = Column(Text)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"))

    def __repr__(self):
        return "<Club {}: {}>".format(
            self.datetime.date(), self.tag)

Club.members = relationship("Player", back_populates="club")
Player.club = relationship("Club", back_populates="members")
Club.balanceChange = relationship("BalanceChange", back_populates="clubViews")
BalanceChange.clubViews = relationship("Club", back_populates="balanceChange")


class Brawler(Base):
    __tablename__ = "brawlers"

    id = Column(Integer, primary_key=True)
    playerId = Column(Integer, ForeignKey("players.id"))
    name = Column(String(MAX_NAME_LENGTH))
    datetime = Column(DateTime)
    hasSkin= Column(Boolean)
    skin = Column(String(MAX_NAME_LENGTH))
    trophies = Column(Integer)
    highestTrophies = Column(Integer)
    power = Column(Integer)
    rank = Column(Integer)

    def __repr__(self):
        return "<Brawler {}: {} (player: {})>".format(
            self.datetime.date(), self.name, self.tag)

Brawler.player = relationship("Player", back_populates="brawlers")
Player.brawlers = relationship("Brawler", back_populates="player")



#Main object
class Client(brawlstats.Client):
    def __init__(self, **kwargs):
        self.requestcnt = 0
        self.token = kwargs.pop("token", None)
        self.verbose = kwargs.pop("verbose", True)
        if self.token is None:
            with open(os.path.join(BASE_DIR, "token.txt"), 'r') as file:
                self.token = file.read().strip()

        with open(os.path.join(BASE_DIR, "db.txt"), 'r') as file:
            db = file.read().strip()

        self.dbengine = create_engine(db, echo=self.verbose)
        dbsession = sessionmaker(bind=self.dbengine)
        self.dbsession = dbsession()

        httpconnector = aiohttp.TCPConnector(limit=50) #50 requests at a time
        httpsession = aiohttp.ClientSession(connector=httpconnector)

        super(Client, self).__init__(self.token, session=httpsession, is_async=True,
                                     **kwargs)

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        super(Client, self).close()

    async def __aexit__(self, exception_type, exception_value, traceback):
        await super(Client, self).close()

    def print_msg(self, msg):
        if self.verbose:
            print(msg)

    async def _aget_model(self, url, model, key=None):
        obj = None
        consec_errs = 0
        while obj is None:
            try:
                obj = await super(Client, self)._aget_model(url, model, key)
            except brawlstats.errors.RateLimitError:
                print(f"{url}: RateLimitError occurred, waiting...")
                await asyncio.sleep(0.5)
            except brawlstats.errors.ServerError as err:
                print(f"{url}: ServerError occurred, waiting 60s...")
                consec_errs += 1
                if consec_errs > 5:
                    raise err
                else:
                    await asyncio.sleep(60)
            except brawlstats.errors.NotFoundError:
                return None

        return obj


    async def _aget_models(self, urls, model, key=None):
        results = []
        results = await asyncio.gather(*(self._aget_model(url, model=model, key=key)
                                         for url in urls))
        return results


    def _get_models(self, urls, model, key=None):
        if self.is_async:
            return self._aget_models(urls, model=model, key=key)
        else:
            return [self.get_model(url, model=model, key=key)
                    for url in urls]

    def get_players(self, tags):
        urls = ['{}?tag={}'.format(self.api.PROFILE, tag) for tag in tags]
        return self._get_models(urls, model=brawlstats.Player)

    def get_clubs(self, tags):
        urls = ['{}?tag={}'.format(self.api.CLUB, tag) for tag in tags]
        return self._get_models(urls, model=brawlstats.Club)

    async def crawl(self, playerTags=[], clubTags=[]):
        clubs = []
        players = []

        #1. Get all players
        self.print_msg(f"Get players for {len(playerTags)} tags...")
        players = await self.get_players(playerTags, bar)

        players = [ player for player in players if player is not None ]

        nativeClubTags = len(clubTags)
        #2. Get club tags of players
        for player in players:
            if player.club and player.club.tag not in clubTags:
                clubTags.append(player.club.tag)

        #3. Get all clubs
        newClubTags = len(clubTags)-nativeClubTags
        self.print_msg(f"Get clubs for {nativeClubTags} native and {newClubTags} tags from players...")
        clubs = await self.get_clubs(clubTags, bar)
        clubs = [ club for club in clubs if club is not None ]

        #4. Get all players in clubs
        self.print_msg(f"Get (partial) players in crawled clubs...")
        newPlayers = 0
        for club in clubs:
            for member in club.members:
                if member.tag not in playerTags:
                    players.append(member)
                    newPlayers += 1

        self.print_msg(f"Found {newPlayers} new players in clubs - Done!")

        return (players, clubs)

    def convert_time_string(self, timestring):
        return datetime.datetime.strptime(timestring, '%d.%m.%y %H:%M:%S')

    def get_last_db_entry(self, db_object):
        return self.dbsession.query(db_object).order_by(db_object.id.desc()).first()

    def get_last_balance_change(self):
        return self.get_last_db_entry(BalanceChange)

    def table_to_df(self, tablename):
        return pd.read_sql_query(f"SELECT * from {tablename};", self.dbengine)

    def commit(self):
        self.dbsession.commit()


    def new_balance_change(self, description, timestring):
        change = BalanceChange(description=description, datetime=self.convert_time_string(timestring))
        self.dbsession.add(change)
        #self.dbsession.commit()

        self.print_msg("New balance change {} inserted!".format(change))

    def new_brawler_change(self, description, name, type, timestring):
        allowed_types = ["new", "buff", "nerf"]
        if type not in allowed_types:
            raise ValueError("Type must be in {}!".format(", ".join(allowed_types)))

        balance_change = self.get_last_balance_change()

        self.print_msg("Adding new brawler change belonging to {}...".format(balance_change))

        brawler = BrawlerChange(description=description, name=name, type=type,
                                datetime=self.convert_time_string(timestring),
                                balanceChangeId=balance_change.id)

        self.dbsession.add(brawler)
        #self.dbsession.commit()

        self.print_msg("New brawler change {} inserted!".format(brawler))

    def new_unique_player(self, tag, added):
        player = UniquePlayer(tag=tag, added=added)
        self.dbsession.add(player)

        self.print_msg("New unique player {} inserted!".format(player))














#DEPRECATED
BASE_DIR = "/home/burney/opt/brawlstartistics"
FILE_DATA = os.path.join(BASE_DIR, "database/data.h5")
FILE_LISTS = os.path.join(BASE_DIR, "database/lists.h5")

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

def my_id():
    return "LLCUJQC2"


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




