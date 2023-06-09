CREATE PROCEDURE InsertUsers AS 
INSERT INTO users (user_id, username, email, created_at) VALUES (?, ?, ?, ?);

CREATE PROCEDURE UpdateUsers AS 
UPDATE users SET username = ?, email = ? WHERE user_id = ?;

CREATE PROCEDURE SelectUsers AS 
SELECT * FROM users WHERE user_id = ?;

CREATE PROCEDURE DeleteUsers AS 
DELETE FROM users WHERE user_id = ?;

CREATE PROCEDURE InsertPosts AS 
INSERT INTO posts (post_id, user_id, content, platform, posted_time) VALUES (?, ?, ?, ?, ?);

CREATE PROCEDURE UpdatePosts AS 
UPDATE posts SET content = ?, platform = ? WHERE post_id = ?;

CREATE PROCEDURE SelectPosts AS 
SELECT * FROM posts WHERE post_id = ?;

CREATE PROCEDURE DeletePosts AS 
DELETE FROM posts WHERE post_id = ?;

CREATE PROCEDURE InsertComments AS 
INSERT INTO comments (comment_id, post_id, content, commented_time) VALUES (?, ?, ?, ?);

CREATE PROCEDURE UpdateComments AS 
UPDATE comments SET content = ? WHERE comment_id = ?;

CREATE PROCEDURE SelectComments AS 
SELECT * FROM comments WHERE comment_id = ?;

CREATE PROCEDURE DeleteComments AS 
DELETE FROM comments WHERE comment_id = ?;
