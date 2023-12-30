CREATE DATABASE IF NOT EXISTS finalproj;

USE finalproj;

DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS users;

CREATE TABLE users
(
    userid       int not null AUTO_INCREMENT,
    username     varchar(64) not null,
    pwdhash      varchar(256) not null,
    PRIMARY KEY  (userid),
    UNIQUE       (username)
);

ALTER TABLE users AUTO_INCREMENT = 80001;  -- starting value

CREATE TABLE jobs
(
    jobid             int not null AUTO_INCREMENT,
    userid            int not null,
    status            varchar(256) not null,  -- pending, completed, error msg
    originaldatafile  varchar(256) not null,  -- original name from user
    datafilekey       varchar(256) not null,  -- filename in the bucket
    resultsfilekey    varchar(256) not null,  -- results filename in bucket
    PRIMARY KEY (jobid),
    FOREIGN KEY (userid) REFERENCES users(userid)
);

ALTER TABLE jobs AUTO_INCREMENT = 1001;  -- starting value

--
-- Insert some users to start with:
-- 
-- PWD hashing: https://phppasswordhash.com/
--
INSERT INTO users(username, pwdhash)  -- pwd = abc123!!
            values('y_li', '$2y$10$/8B5evVyaHF.hxVx0i6dUe2JpW89EZno/VISnsiD1xSh6ZQsNMtXK');

INSERT INTO users(username, pwdhash)  -- pwd = abc456!!
            values('r_wei', '$2y$10$F.FBSF4zlas/RpHAxqsuF.YbryKNr53AcKBR3CbP2KsgZyMxOI2z2');

INSERT INTO users(username, pwdhash)  -- pwd = abc789!!
            values('a_militchinski', '$2y$10$GmIzRsGKP7bd9MqH.mErmuKvZQ013kPfkKbeUAHxar5bn1vu9.sdK');

--
-- creating user accounts for database access:
--
-- ref: https://dev.mysql.com/doc/refman/8.0/en/create-user.html
--

DROP USER IF EXISTS 'finalproj-read-only';
DROP USER IF EXISTS 'finalproj-read-write';

CREATE USER 'finalproj-read-only' IDENTIFIED BY 'abc123!!';
CREATE USER 'finalproj-read-write' IDENTIFIED BY 'def456!!';

GRANT SELECT, SHOW VIEW ON finalproj.* 
      TO 'finalproj-read-only';
GRANT SELECT, SHOW VIEW, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ON finalproj.* 
      TO 'finalproj-read-write';
      
FLUSH PRIVILEGES;

--
-- done
--

