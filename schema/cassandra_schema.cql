CREATE KEYSPACE IF NOT EXISTS social_media WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE social_media;

CREATE TABLE IF NOT EXISTS users (
    user_id text PRIMARY KEY,
    username text,
    email text,
    created_at text
);

CREATE TABLE IF NOT EXISTS posts (
    post_id text PRIMARY KEY,
    user_id text,
    content text,
    platform text,
    posted_time text
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id text PRIMARY KEY,
    post_id text,
    content text,
    commented_time text
);
