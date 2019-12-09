
# coding: utf-8

import numpy as np
import pandas as pd

data = pd.read_csv("ted_main.csv")

transcripts = pd.read_csv("transcripts.csv")


# 3 duplicates in transcripts - remove from data

transcripts.drop_duplicates(inplace=True)


# join transcripts to data

data = pd.merge(left = data, right = transcripts, on = "url", how = "left")

data = pd.DataFrame(data)


# clean dates

from datetime import datetime

data['film_date'] = data['film_date'].apply(lambda x: datetime.fromtimestamp( int(x)).strftime('%d-%m-%Y'))

data['published_date'] = data['published_date'].apply(lambda x: datetime.fromtimestamp( int(x)).strftime('%d-%m-%Y'))


# make nulls blank instead

data["transcript"][data["transcript"].isnull()] = ""



# make tables

# videos

video_titles = ["title", "description"]

videos = data[video_titles].drop_duplicates()


# use index to create id column

videos.reset_index(inplace = True)

videos['id'] = videos.index

videos = videos[['id', 'title', 'description']]


# videos table is ready - join IDs back to data

data = pd.merge(left = data, right = videos, how = 'left', on = ['title', 'description'])

data.rename(columns = {'id':'video_id'}, inplace = True)

# video IDs successfully added to data


# events

# to make events, we need to make start dates and end dates

by_event = data.groupby('event')

min_by_event = by_event.agg({'film_date':"min"})

max_by_event = by_event.agg({'film_date':"max"})


dates = pd.concat([min_by_event, max_by_event], axis = 1)

dates.columns = ['start_date', 'end_date']


events = dates.copy()

events['event'] = events.index

events.index = range(1, len(events) + 1)


# make separate id column with index numbers

events['id'] = events.index

events = events[['id', 'event', 'start_date', 'end_date']]


# add event IDs to data

data = pd.merge(left = data, right = events, how = 'left', on = 'event')

data.rename(columns = {'id':'event_id'}, inplace = True)


# occupations

occupations = pd.DataFrame(data['speaker_occupation'].unique())

occupations['id'] = occupations.index

occupations.columns = ['occupation', 'id']

occupations = occupations[['id', 'occupation']]


# add occupation IDs back to data

data = pd.merge(left = data, right = occupations, how = 'left',
                left_on = 'speaker_occupation', right_on = 'occupation')


# delete occupation column and rename occpation_id

data.drop('occupation', axis = 1, inplace = True)

data.rename(columns={'id':'occupation_id'}, inplace=True)


# languages

# we only have numbers, so combining video IDs and languages columns

languages = data[['video_id', 'languages']]


# main_speakers

main_speakers = data[['main_speaker', 'occupation_id']]

main_speakers.drop_duplicates(inplace=True)


# create main speaker IDs

main_speakers.reset_index(inplace=True)

main_speakers['id'] = main_speakers.index

main_speakers = main_speakers[['id', 'main_speaker', 'occupation_id']]


# add IDs back to data

data = pd.merge(left = data, right = main_speakers, how = 'left', on = ['main_speaker', 'occupation_id'])

data.rename(columns = {'id':'speaker_id'}, inplace = True)


# presentations - all unique combos of videos events and speakers

presentations = data[['video_id', 'event_id', 'speaker_id']].drop_duplicates()


# url_links

url_links = data[['video_id', 'url']].drop_duplicates()


# transcripts

transcripts = data[['video_id', 'transcript']].drop_duplicates()


# dates

dates = data[['video_id', 'film_date', 'published_date']].drop_duplicates()


# video stats

video_stats = data[['video_id', 'duration', 'num_speaker', 'views', 'comments']].drop_duplicates()


# video tags

video_tags = data[['video_id', 'tags']]


# clean tags

vid_tags = video_tags['tags'].apply(lambda x: str.split(x,sep=","))

video_tags['tags'] = vid_tags

video_tags = video_tags.explode('tags')


# clean brackets in tag names

video_tags['tags'].replace('[^\w\s]', '', regex = True, inplace = True)


# create tags table

video_tags.rename(columns = {'tags':'tag'}, inplace = True)

tags = video_tags['tag'].drop_duplicates()

tags = pd.DataFrame(tags)

tags.reset_index(drop = True, inplace = True)

tags['id'] = tags.index


# join tag IDs back to video tags

video_tags = pd.merge(left = video_tags, right = tags, how = 'left', on = 'tag')

video_tags.rename(columns = {'id':'tag_id'}, inplace = True)


# delete tag from video tags

video_tags.drop(columns = 'tag', inplace = True)


# related talks

import ast

data['related_talks'] = data['related_talks'].apply(lambda x: ast.literal_eval(x))

s = data.apply(lambda x: pd.Series(x['related_talks']),axis=1).stack().reset_index(level=1, drop=True)

s.name = 'related'

related_df = data.drop('related_talks', axis=1).join(s)

dic = related_df.reset_index()['related'][0]

for i in dic.keys():

    related_df['related_'+i] = related_df['related'].apply(lambda x: x[i])


related_df.drop('related', axis=1, inplace=True)

related_videos = related_df[['video_id', 'related_id']].drop_duplicates()


# ratings

data['ratings'] = data['ratings'].apply(lambda x: ast.literal_eval(x))


s = data.apply(lambda x: pd.Series(x['ratings']),axis=1).stack().reset_index(level=1, drop=True)

s.name = 'ratings'


main_df = data.drop('ratings', axis=1).join(s)


dic = main_df.reset_index()['ratings'][0]


for i in dic.keys():

    main_df['ratings_'+i] = main_df['ratings'].apply(lambda x: x[i])


main_df.drop('ratings', axis=1, inplace=True)


ratings_df = pd.DataFrame(main_df, columns = ['ratings_id', 'ratings_name'])

ratings_df.rename(columns = {'ratings_id':'id', 'ratings_name':'rating'}, inplace = True)

ratings_df = ratings_df.drop_duplicates()


# video_ratings

video_ratings = main_df[['video_id', 'ratings_id']].drop_duplicates()

video_ratings.rename(columns = {'ratings_id':'rating_id'}, inplace = True)


# SQL

from sqlalchemy import create_engine


# Pass the connection string to a variable, conn_url
conn_url = 'postgresql://postgres:64ddswmi@f19server.apan5310.com:50202/ted_talks'

# Create an engine that connects to PostgreSQL server
engine = create_engine(conn_url)

# Establish a connection
connection = engine.connect()


stmt = '''

CREATE TABLE videos (
    id integer,
    title varchar(150) NOT NULL,
    description text,
    PRIMARY KEY (id)
);

CREATE TABLE events (
    id integer,
    event_name varchar(150),
    start_date date,
    end_date date,
    PRIMARY KEY (id)
);

CREATE TABLE occupations (
    id integer,
    occupation_name varchar(100),
    PRIMARY KEY (id)
);

CREATE TABLE languages (
    video_id integer,
    languages integer,
    PRIMARY KEY (video_id, languages),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

CREATE TABLE main_speakers (
    id integer,
    speaker_name varchar(100),
    speaker_occupation_id integer,
    PRIMARY KEY (id),
    FOREIGN KEY (speaker_occupation_id) REFERENCES occupations (id) ON UPDATE
    CASCADE ON DELETE CASCADE
);

CREATE TABLE presentations (
    video_id integer,
    event_id integer,
    speaker_id integer,
    PRIMARY KEY (video_id, speaker_id),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES events (id) ON UPDATE CASCADE ON
    DELETE CASCADE,
    FOREIGN KEY (speaker_id) REFERENCES main_speakers (id) ON UPDATE
    CASCADE ON DELETE CASCADE
);

CREATE TABLE url_links (
    video_id integer,
    url_link text,
    PRIMARY KEY(video_id, url_link),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

CREATE TABLE transcripts (
    video_id integer,
    transcript text,
    PRIMARY KEY(video_id),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

CREATE TABLE dates (
    video_id integer,
    film_date date,
    published_date date,
    PRIMARY KEY(video_id, film_date, published_date),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

CREATE TABLE video_stats (
    video_id integer,
    duration numeric NOT NULL,
    number_views numeric,
    number_comments numeric,
    number_speakers integer,
    PRIMARY KEY (video_id, duration),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

CREATE TABLE related_videos (
    video_id integer,
    related_video_id integer,
    PRIMARY KEY (video_id, related_video_id),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE,
    FOREIGN KEY (related_video_id) REFERENCES videos (id) ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE TABLE tags (
    id integer,
    tag varchar (30),
    PRIMARY KEY (id)
);

CREATE TABLE video_tags (
    video_id integer,
    tag_id integer,
    PRIMARY KEY (video_id ,tag_id),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags (id) ON UPDATE CASCADE ON DELETE
    CASCADE
);

CREATE TABLE ratings (
    id integer,
    rating text,
    PRIMARY KEY (id)
);

CREATE TABLE video_ratings (
    video_id integer,
    rating_id integer,
    PRIMARY KEY (video_id , rating_id),
    FOREIGN KEY (video_id) REFERENCES videos (id) ON UPDATE CASCADE ON
    DELETE CASCADE,
    FOREIGN KEY (rating_id) REFERENCES ratings (id) ON UPDATE CASCADE ON
    DELETE CASCADE
);

'''


connection.execute(stmt)


# videos

videos.to_sql('videos', engine, if_exists='append', index = False)



events.rename(columns={'event': 'event_name'}, inplace=True)

events

s = '14-03-2009'

from datetime import datetime
datetime.strptime(s, '%d-%m-%Y').strftime('%Y-%m-%d')

events['start_date'] = events['start_date'].apply(lambda x: x[6:] + '-' + x[3:5] + '-' + x[0:2])
events['end_date'] = events['end_date'].apply(lambda x: x[6:] + '-' + x[3:5] + '-' + x[0:2])

# events

events.to_sql('events', engine, if_exists='append', index = False)


occupations.rename(columns={'occupation': 'occupation_name'}, inplace=True)


# occupations

occupations.to_sql('occupations', engine, if_exists='append', index = False)


# languages

languages.to_sql('languages', engine, if_exists='append', index = False)


main_speakers.rename(columns={'main_speaker': 'speaker_name', 'occupation_id': 'speaker_occupation_id'}, inplace=True)


# main_speakers

main_speakers.to_sql('main_speakers', engine, if_exists='append', index = False)


# presentations

presentations.to_sql('presentations', engine, if_exists='append', index = False)



url_links.rename(columns={'url': 'url_link'}, inplace=True)

# url_links

url_links.to_sql('url_links', engine, if_exists='append', index = False)


transcripts['video_id'].duplicated().sum()

# transcripts

transcripts.to_sql('transcripts', engine, if_exists='append', index = False)


dates['film_date'] = dates['film_date'].apply(lambda x: x[6:] + '-' + x[3:5] + '-' + x[0:2])
dates['published_date'] = dates['published_date'].apply(lambda x: x[6:] + '-' + x[3:5] + '-' + x[0:2])


# dates

dates.to_sql('dates', engine, if_exists='append', index = False)


video_stats.rename(columns={'num_speaker': 'number_speakers',
                              'views': 'number_views',
                              'comments': 'number_comments'}, inplace=True)


video_stats = video_stats[['video_id','duration','number_views','number_comments','number_speakers']]

# video_stats

video_stats.to_sql('video_stats', engine, if_exists='append', index = False)


related_videos.rename(columns={'related_id': 'related_video_id'}, inplace=True)

related_videos[~related_videos['related_video_id'].isin(videos['id'])]

related_videos = related_videos[related_videos['related_video_id'].isin(videos['id'])]

# related_videos

related_videos.to_sql('related_videos', engine, if_exists='append', index = False)


# tags

tags.to_sql('tags', engine, if_exists='append', index = False)


# video_tags

video_tags.to_sql('video_tags', engine, if_exists='append', index = False)


# ratings

ratings_df.to_sql('ratings', engine, if_exists='append', index = False)


# video_ratings

video_ratings.to_sql('video_ratings', engine, if_exists='append', index = False)
