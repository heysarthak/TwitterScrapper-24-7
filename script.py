# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import tweepy as tw
import re
import string
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from tqdm import tqdm

from datetime import date, timedelta

from decouple import config


def clean_text(df):
    all_reviews = list()
    lines = df["Tweets"].values.tolist()
    for text in lines:
        text = text.lower()
        pattern = re.compile(
            "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        )
        text = pattern.sub("", text)
        text = re.sub(r"[,.\"!@#$%^&*(){}?/;`~:<>+=-]", "", text)
        tokens = word_tokenize(text)
        table = str.maketrans("", "", string.punctuation)
        stripped = [w.translate(table) for w in tokens]
        words = [word for word in stripped if word.isalpha()]
        stop_words = set(stopwords.words("english"))
        stop_words.discard("not")
        PS = PorterStemmer()
        words = [PS.stem(w) for w in words if not w in stop_words]
        words = " ".join(words)
        all_reviews.append(words)
    return all_reviews


consumer_key = config("CONSUMER_KEY", "-----------")
consumer_secret = config("CONSUMER_SECRET", "-----------")
access_token = config("ACCESS_TOKEN", "-----------")
access_token_secret = config("ACCESS_TOKEN_SECRET", "-----------")


auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

my_data = []

search_words = ["traffic", "water"]
date_since = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")

for search_word in tqdm(search_words):
    tweets = tw.Cursor(
        api.search,
        q=search_word,
        geocode="30.7333,76.7794,8km",
        lang="en",
        since=date_since,
    ).items(20)

    # [print(tweet) for tweet in tweets]
    # data = pd.DataFrame(data=[[tweet.id, tweet.text, tweet.user.location, search_word, ] for tweet in tweets],
    #                     columns=['twitter_id','Tweets', 'Location', 'Search_word'])
    # my_data = pd.concat([my_data, data])

    my_data.extend(
        [
            [
                tweet.id,
                tweet.text,
                tweet.user.location,
                search_word,
            ]
            for tweet in tweets
        ]
    )

print("Finished fetching tweets.")

# %%
import sqlalchemy
from sqlalchemy import Column, String, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

engine: Engine = sqlalchemy.create_engine("sqlite:///tweets.sqlite3", echo=False)

session: Session = sessionmaker(bind=engine)()

Base = declarative_base()


class Tweet(Base):
    __tablename__ = "tweets"

    twitter_id = Column(String, primary_key=True)
    content = Column(String)
    location = Column(String)
    search_word = Column(String)

    def __init__(self, tw_id, content, location, sw):
        self.twitter_id = tw_id
        self.content = content
        self.location = location
        self.search_word = sw

    def __repr__(self):
        return f"[Tweet:{self.location} ({self.content[:100]})]"


Base.metadata.create_all(engine)


print("Initialize database tables.")



for row in my_data:
    new_tweet = Tweet(tw_id=row[0], content=row[1], location=row[2], sw=row[3])

    try:
        session.merge(new_tweet)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error occurred : {e}")


print("Merged new tweets into database")