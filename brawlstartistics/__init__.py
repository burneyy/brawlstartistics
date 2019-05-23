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
import dateutil
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)-8s: %(name)-12s: %(message)s',
                    datefmt='%d.%m.%y %H:%M')
logger = logging.getLogger("brawlstartistics")

#SQLALCHEMY
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy import ForeignKey, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.exc import IntegrityError
Base = declarative_base()
metadata = Base.metadata

#Global variables
MAX_TAG_LENGTH = 12
MAX_NAME_LENGTH = 30
MAX_TIME_LENGTH = 20
MAX_REGION_LENGTH = 3

TAG_CHARS = "0289PYLQGRJCUV"

ALL_BRAWLERS = [
    'Shelly', 'Nita', 'Colt', 'Bull', 'Jessie',  # league reward 0-500
    'Brock', 'Dynamike', 'Bo',                   # league reward 1000+
    'El Primo', 'Barley', 'Poco', 'Rosa',        # rare
    'Rico', 'Penny', 'Darryl', 'Carl',           # super rare
    'Frank', 'Pam', 'Piper', 'Bibi',             # epic
    'Mortis', 'Tara', 'Gene',                    # mythic
    'Spike', 'Crow', 'Leon'                      # legendary
]


def valid_tag(tag):
    if len(tag) < 3 or len(tag) > 10:
        return False
    for char in tag:
        if char not in TAG_CHARS:
            return False

    return True


class BalanceChange(Base):
    __tablename__ = "balance_changes"

    id = Column(Integer, primary_key=True)
    datetime = Column(DateTime)
    description = Column(Text)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

class BrawlerChange(Base):
    __tablename__ = "brawler_changes"

    id = Column(Integer, primary_key=True)
    name = Column(String(MAX_NAME_LENGTH), nullable=False)
    type = Column(String(20), nullable=False)  #e.g. new, nerf, buff
    datetime = Column(DateTime)
    balanceChangeId = Column(Integer, ForeignKey("balance_changes.id"), nullable=False)
    description = Column(Text)

    def __init__(self, **kwargs):
        if kwargs["name"] not in ALL_BRAWLERS:
            raise ValueError("{} is not a valid brawler!".format(kwargs["name"]))

        if kwargs["type"] not in ["nerf", "buff", "new"]:
            raise ValueError("{} is no valid type!".format(kwargs["type"]))

        for key in kwargs:
            setattr(self, key, kwargs[key])


    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

BrawlerChange.balanceChange = relationship("BalanceChange", back_populates="brawlersAffected")
BalanceChange.brawlersAffected = relationship("BrawlerChange", back_populates="balanceChange")

class UniquePlayer(Base):
    __tablename__ = "player_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

    def __init__(self, **kwargs):
        if not valid_tag(kwargs["tag"]):
            raise ValueError("{} is no valid tag!".format(kwargs["tag"]))

        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

class UniqueClub(Base):
    __tablename__ = "club_list"

    tag = Column(String(MAX_TAG_LENGTH), primary_key=True)
    added = Column(DateTime)

    def __init__(self, **kwargs):
        if not valid_tag(kwargs["tag"]):
            raise ValueError("{} is no valid tag!".format(kwargs["tag"]))
        for key in kwargs:
            setattr(self, key, kwargs[key])


    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    clubId = Column(Integer, ForeignKey("clubs.id"))
    tag = Column(String(MAX_TAG_LENGTH), ForeignKey("player_list.tag"), nullable=False)
    datetime = Column(DateTime)
    name = Column(String(MAX_NAME_LENGTH), nullable=False)
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
    #Club specific
    role = Column(String(MAX_NAME_LENGTH))
    onlineLessThanOneHourAgo = Column(Boolean)

    def __init__(self, **kwargs):
        if not valid_tag(kwargs["tag"]):
            raise ValueError("{} is no valid tag!".format(kwargs["tag"]))
        for key in kwargs:
            setattr(self, key, kwargs[key])

    @classmethod
    def from_brawlstats(cls, bs_player):
        return cls(**cls.brawlstats_to_dict(bs_player))

    @staticmethod
    def brawlstats_to_dict(bs_player):
        try:
            date = dateutil.parser.parse(bs_player.resp.headers["Date"])
        except AttributeError:
            logger.warning(f"Could not get response time for player #{bs_player.tag}"
                   " - take current UTC time instead")
            date = datetime.datetime.utcnow()
        d = { "datetime" : date }
        for k, v in bs_player.raw_data.items():
            if k == "brawlers":
                d["brawlers"] = [ Brawler.from_brawlstats(b, date) for b in v ]
            elif k != "id" and k != "club":
                d[k] = v
        return d

    async def update(self, client):
        """Update information from api"""
        if not valid_tag(self.tag):
            raise ValueError("{} is no valid tag!".format(self.tag))

        player = await client.get_player(self.tag)
        if player is not None:
            self.__init__(**Player.brawlstats_to_dict(player))
        return self



    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

Player.balanceChange = relationship("BalanceChange", back_populates="playerViews")
BalanceChange.playerViews = relationship("Player", back_populates="balanceChange")


class Club(Base):
    __tablename__ = "clubs"

    id = Column(Integer, primary_key=True)
    tag = Column(String(MAX_TAG_LENGTH), ForeignKey("club_list.tag"), nullable=False)
    datetime = Column(DateTime)
    name = Column(String(MAX_NAME_LENGTH), nullable=False)
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

    def __init__(self, **kwargs):
        if not valid_tag(kwargs["tag"]):
            raise ValueError("{} is no valid tag!".format(kwargs["tag"]))

        for key in kwargs:
            setattr(self, key, kwargs[key])

    @classmethod
    async def from_brawlstats(cls, bs_club, client=None):
        try:
            date = dateutil.parser.parse(bs_club.resp.headers["Date"])
        except AttributeError:
            logger.warning(f"Warning: Could not get response time for club #{bs_club.tag}"
                   " - take current UTC time instead")
            date = datetime.datetime.utcnow()

        d = { "datetime" : date }
        for k, v in bs_club.raw_data.items():
            if k == "members":
                members = []
                for member in v:
                    member = dict(member)
                    del member["id"]
                    members.append(Player(**member))
                if client is not None:
                    members = await asyncio.gather(*(m.update(client) for m in members))
                d["members"] = members
            elif k != "id":
                d[k] = v

        return cls(**d)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

Club.members = relationship("Player", back_populates="club")
Player.club = relationship("Club", back_populates="members")
Club.balanceChange = relationship("BalanceChange", back_populates="clubViews")
BalanceChange.clubViews = relationship("Club", back_populates="balanceChange")


class Brawler(Base):
    __tablename__ = "brawlers"

    id = Column(Integer, primary_key=True)
    playerId = Column(Integer, ForeignKey("players.id"), nullable=False)
    brawlerChangeId = Column(Integer, ForeignKey("brawler_changes.id"))
    name = Column(String(MAX_NAME_LENGTH), nullable=False)
    datetime = Column(DateTime)
    hasSkin= Column(Boolean)
    skin = Column(String(MAX_NAME_LENGTH))
    trophies = Column(Integer)
    highestTrophies = Column(Integer)
    power = Column(Integer)
    rank = Column(Integer)

    def __init__(self, **kwargs):
        if kwargs["name"] not in ALL_BRAWLERS:
            raise ValueError("{} is not a valid brawler!".format(kwargs["name"]))

        for key in kwargs:
            setattr(self, key, kwargs[key])

    def from_brawlstats(self, bs_brawler, datetime=None):
        self.datetime = datetime
        if self.datetime is None:
            self.datetime = datetime.datetime.now()
        for k, v in bs_brawler.raw_data.items():
            setattr(self, k, v)

    @classmethod
    def from_brawlstats(cls, bs_brawler, date=None):
        if date is None:
            date = datetime.datetime.now()
        d = { "datetime" : date }
        for k, v in bs_brawler.items():
            d[k] = v

        return cls(**d)


    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)

Brawler.player = relationship("Player", back_populates="brawlers")
Player.brawlers = relationship("Brawler", back_populates="player")
Brawler.brawlerChange = relationship("BrawlerChange", back_populates="brawlers")
BrawlerChange.brawlers = relationship("Brawler", back_populates="brawlerChange")



#Main object
class Client(brawlstats.Client):
    def __init__(self, **kwargs):
        self.requestcnt = 0
        self.token = kwargs.pop("token", None)
        self.verbose = kwargs.pop("verbose", True)
        is_async = kwargs.pop("is_async", True)
        echo = kwargs.pop("echo", False)
        if self.token is None:
            with open(os.path.join(BASE_DIR, "token.txt"), 'r') as file:
                self.token = file.read().strip()

        with open(os.path.join(BASE_DIR, "db.txt"), 'r') as file:
            db = file.read().strip()

        self.dbengine = create_engine(db, echo=echo)
        dbsession = sessionmaker(bind=self.dbengine)
        self.dbsession = dbsession()

        if is_async:
            httpconnector = aiohttp.TCPConnector(limit=50) #50 requests at a time
            httpsession = aiohttp.ClientSession(connector=httpconnector)

            super().__init__(self.token, session=httpsession, is_async=True,
                             prevent_ratelimit=True, **kwargs)
        else:
            super().__init__(self.token, loop=None, is_async=False,
                             **kwargs)

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        return self.close()

    def close(self):
        self.dbsession.close()
        return super().close()

    async def aclose(self):
        self.dbsession.close()
        return await super().close()

    async def __aexit__(self, exception_type, exception_value, traceback):
        return await self.aclose()

    def print_msg(self, msg):
        if self.verbose:
            logger.info(msg)

    async def _aget_model(self, url, model, key=None):
        obj = None
        consec_errs = 0
        while obj is None:
            try:
                obj = await super()._aget_model(url, model, key)
            except brawlstats.errors.RateLimitError:
                wait = 0.5
                self.print_msg(f"{url}: RateLimitError occurred, waiting...")
                await asyncio.sleep(wait)
            except brawlstats.errors.ServerError as err:
                wait = 60
                consec_errs += 1
                if consec_errs > 5:
                    raise err
                else:
                    self.print_msg(f"{url}: ServerError occurred, waiting {wait}s...")
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
        unique_players = self.get_random_db_entries(Player, player_limit)
        player_tags = [ player.tag for player in unique_players ]

        #2. Update information for these players
        self.print_msg(f"Updating information for {len(player_tags)} random player tags...")
        players = await self.get_players(player_tags)
        players = [ player for player in players if player is not None ]
        self.print_msg(f"Successfully updated information for {len(players)} players.")

        #3. Update information from their clubs
        club_tags = [ p.club.tag for p in players if p.club is not None ]
        self.print_msg(f"Updating information for {len(club_tags)} clubs (of the players)...")
        clubs = await self.get_clubs(club_tags)
        clubs = [ club for club in clubs if club is not None ]
        self.print_msg(f"Successfully updated information for {len(clubs)} clubs.")

        #4. Building database club objects out of them (and updating member information)
        db_clubs = []
        self.print_msg(f"Processing clubs to update their members...")
        for i, club in enumerate(clubs):
            self.print_msg(f"Processing club {i+1} of {len(clubs)} (#{club.tag}) with {club.membersCount} members...")
            db_club = await Club.from_brawlstats(club, self)
            db_clubs.append(db_club)

        #5. Set balanceChangeId
        balance_change_id = self.get_last_balance_change()
        if balance_change_id is not None:
            balance_change_id = balance_change_id.id
        brawler_change_ids = {}
        for bname in ALL_BRAWLERS:
            brawler_change_ids[bname] = self.get_last_brawler_change(bname)
            if brawler_change_ids[bname] is not None:
                brawler_change_ids[bname] = brawler_change_ids[bname].id
        self.print_msg("Setting balance change ids to {} "
                       "and brawler change ids to {!r}..."
                       "".format(balance_change_id, brawler_change_ids))
        for club in db_clubs:
            club.balanceChangeId = balance_change_id
            for member in club.members:
                member.balanceChangeId = balance_change_id
                for brawler in member.brawlers:
                    brawler.brawlerChangeId = brawler_change_ids[brawler.name]


        self.print_msg(f"Storing crawled clubs, players and brawlers in database...")
        self.add_clubs(db_clubs)
        self.commit()

        n_players = self.get_number_db_entries(UniquePlayer)
        n_clubs = self.get_number_db_entries(UniqueClub)

        self.print_msg(f"The database now contains {n_clubs} unique clubs and "
                       f"{n_players} players.")

        return db_clubs

    def convert_time_string(self, timestring):
        return datetime.datetime.strptime(timestring, '%d.%m.%y %H:%M:%S')

    def get_last_db_entry(self, dbmodel, **filters):
        return self.query(dbmodel).filter_by(**filters).order_by(dbmodel.id.desc()).first()

    def get_random_db_entries(self, dbmodel, limit=100):
        """Return random entries from database"""
        return self.query(dbmodel).order_by(func.rand()).limit(limit).all()

    def get_number_db_entries(self, dbmodel):
        return self.query(dbmodel).count()

    def get_last_brawler_change(self, name):
        return self.get_last_db_entry(BrawlerChange, name=name)


    def get_last_balance_change(self):
        return self.get_last_db_entry(BalanceChange)

    def table_to_df(self, tablename):
        return pd.read_sql_query(f"SELECT * from {tablename};", self.dbengine)

    def query(self, *args, **kwargs):
        return self.dbsession.query(*args, **kwargs)

    def commit(self):
        self.dbsession.commit()

    def add(self, dbobject):
        self.dbsession.add(dbobject)

    def add_if_not_exists(self, dbobject):
        try:
            self.add(dbobject)
            self.commit()
            return True
        except IntegrityError:
            self.dbsession.rollback()
            return False

    def add_all(self, dbobjects):
        self.dbsession.add_all(dbobjects)

    def add_clubs(self, dbclubs):
        #Build unique clubs and players out of it
        n_clubs = 0
        new_clubs = 0
        n_players = 0
        new_players = 0
        for club in dbclubs:
            n_clubs += 1
            if self.add_if_not_exists(
                    UniqueClub(tag=club.tag, added=datetime.datetime.utcnow())):
                new_clubs += 1
            for player in club.members:
                n_players += 1
                if self.add_if_not_exists(
                        UniquePlayer(tag=player.tag, added=datetime.datetime.utcnow())):
                    new_players += 1

        self.add_all(dbclubs)
        self.print_msg(f"Added ({new_clubs}) {n_clubs} (new) clubs.")
        self.print_msg(f"Added ({new_players}) {n_players} (new) players.")



    def new_balance_change(self, description, timestring):
        change = BalanceChange(description=description, datetime=self.convert_time_string(timestring))
        self.dbsession.add(change)

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


def read(file, key, default=None):
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
    return read(FILE_DATA, "players")

def save_players(df):
    save(df, FILE_DATA, "players")

def read_clubs():
    return read(FILE_DATA, "clubs")

def save_clubs(df):
    save(df, FILE_DATA, "clubs")

def read_brawlers():
    return read(FILE_DATA, "brawlers")

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




