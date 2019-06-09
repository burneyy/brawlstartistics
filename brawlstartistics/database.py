import os
import pandas as pd
import asyncio
import datetime
import dateutil
import logging
logger = logging.getLogger("brawlstartistics.database")

#SQLALCHEMY
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy import ForeignKey, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.exc import IntegrityError
Base = declarative_base()
metadata = Base.metadata

from .constants import MAX_NAME_LENGTH, MAX_REGION_LENGTH, MAX_TAG_LENGTH, MAX_TIME_LENGTH
from .constants import TAG_CHARS, ALL_BRAWLERS, BASE_DIR


def valid_tag(tag):
    if len(tag) < 3 or len(tag) > 10:
        return False
    for char in tag:
        if char not in TAG_CHARS:
            return False

    return True

def convert_time_string(self, timestring):
    return datetime.datetime.strptime(timestring, '%d.%m.%y %H:%M:%S')



# ====================================================
# ===== Define Database Structure ====================
# ====================================================
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



# ====================================================
# ===== Client to interact with our database =========
# ====================================================
class Client():
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('brawlstartistics.database.Client')
        with open(os.path.join(BASE_DIR, "db.txt"), 'r') as file:
            db = file.read().strip()

        self.dbengine = create_engine(db, echo=kwargs.pop("echo", False))
        dbsession = sessionmaker(bind=self.dbengine)
        self.dbsession = dbsession()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        return self.close()

    def close(self):
        return self.dbsession.close()

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
        self.logger.info(f"Added ({new_clubs}) {n_clubs} (new) clubs.")
        self.logger.info(f"Added ({new_players}) {n_players} (new) players.")



    def new_balance_change(self, description, timestring):
        change = BalanceChange(description=description, datetime=self.convert_time_string(timestring))
        self.dbsession.add(change)

        self.logger.info("New balance change {} inserted!".format(change))

    def new_brawler_change(self, description, name, type, timestring):
        allowed_types = ["new", "buff", "nerf"]
        if type not in allowed_types:
            raise ValueError("Type must be in {}!".format(", ".join(allowed_types)))

        balance_change = self.get_last_balance_change()

        self.logger.info("Adding new brawler change belonging to {}...".format(balance_change))

        brawler = BrawlerChange(description=description, name=name, type=type,
                                datetime=self.convert_time_string(timestring),
                                balanceChangeId=balance_change.id)

        self.dbsession.add(brawler)

        self.logger.info("New brawler change {} inserted!".format(brawler))

    def new_unique_player(self, tag, added):
        player = UniquePlayer(tag=tag, added=added)
        self.dbsession.add(player)

        self.logger.info("New unique player {} inserted!".format(player))
