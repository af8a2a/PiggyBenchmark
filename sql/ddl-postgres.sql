DROP TABLE IF EXISTS checking;
DROP TABLE IF EXISTS savings;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    custid int  PRIMARY KEY ,
    name   varchar(64) NOT NULL
);
CREATE INDEX idx_accounts_name ON accounts (name);

CREATE TABLE savings (
    custid int PRIMARY KEY,
    bal    float  NOT NULL
);

CREATE TABLE checking (
    custid int PRIMARY KEY,
    bal    float  NOT NULL
);
