CREATE TABLE users (
    user_id VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    email VARCHAR(255),
    created_at VARCHAR(255),
    PRIMARY KEY (user_id)
);
PARTITION TABLE users ON COLUMN user_id;

CREATE TABLE posts (
    post_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255),
    content VARCHAR(2000),
    platform VARCHAR(255),
    posted_time VARCHAR(255),
    PRIMARY KEY (post_id)
);
PARTITION TABLE posts ON COLUMN post_id;

CREATE TABLE comments (
    comment_id VARCHAR(255) NOT NULL,
    post_id VARCHAR(255),
    content VARCHAR(2000),
    commented_time VARCHAR(255),
    PRIMARY KEY (comment_id)
);
PARTITION TABLE comments ON COLUMN comment_id;
