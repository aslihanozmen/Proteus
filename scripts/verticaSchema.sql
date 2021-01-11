CREATE TABLE new_tables_tokenized_full (
id 	INT PRIMARY KEY NOT NULL,
externalid VARCHAR(50),
numRows INT NOT NULL,
url VARCHAR(400),
title VARCHAR(4000),
title_tokenized VARCHAR(4000),
pageTitle VARCHAR(4000),
pageTitle_tokenized VARCHAR(4000),
context VARCHAR(3000),
context_tokenized VARCHAR(3000),
content LONG VARCHAR(32000000),
confidence REAL ENCODING RLE,
source INT ENCODING RLE
)
SEGMENTED BY HASH(id) ALL NODES;

CREATE TABLE columns_tokenized (
tableid INT NOT NULL,
colid INT NOT NULL,
header VARCHAR(1000) ENCODING AUTO,
header_tokenized VARCHAR(1000) ENCODING AUTO,
source INT NOT NULL DEFAULT 0 ENCODING RLE,
PRIMARY KEY(tableid, colid)
)
SEGMENTED BY HASH(tableid) ALL NODES;


CREATE PROJECTION header_tokenized_to_col (
hedaer_tokenized,
header,
tableid,
colid
) AS
SELECT header_tokenized, header, tableid, colid
FROM columns_tokenized
ORDER BY header_tokenized, tableid, colid
SEGMENTED BY HASH(header_tokenized) ALL NODES;

CREATE PROJECTION header_tokenized_to_col_b1 (
hedaer_tokenized,
header,
tableid,
colid
) AS
SELECT header_tokenized, header, tableid, colid
FROM columns_tokenized
ORDER BY header_tokenized, tableid, colid
SEGMENTED BY HASH(header_tokenized) ALL NODES OFFSET 1;

CREATE TABLE main_tokenized (
tableid INT NOT NULL ENCODING RLE,
colid INT NOT NULL,
rowid INT NOT NULL,
term VARCHAR(200) ENCODING AUTO,
tokenized VARCHAR(200) ENCODING AUTO,
PRIMARY KEY(tableid, colid, rowid)
)
SEGMENTED BY HASH(tableid) ALL NODES;


CREATE PROJECTION tokenized_to_col (
tokenized,
term,
tableid,
colid,
rowid) AS
SELECT tokenized, term, tableid, colid, rowid
FROM main_tokenized
ORDER BY tokenized, tableid, colid, rowid
SEGMENTED BY HASH(tokenized	) ALL NODES;


CREATE PROJECTION tokenized_to_col_b1 (
tokenized,
term,
tableid,
colid,
rowid) AS
SELECT tokenized, term, tableid, colid, rowid
FROM main_tokenized
ORDER BY tokenized, tableid, colid, rowid
SEGMENTED BY HASH(tokenized	) ALL NODES OFFSET 1;


CREATE PROJECTION term (
term
) AS
SELECT term
FROM main_tokenized
ORDER BY term
SEGMENTED BY HASH(term) ALL NODES;

CREATE PROJECTION term_b1 (
term
) AS
SELECT term
FROM main_tokenized
ORDER BY term
SEGMENTED BY HASH(term) ALL NODES OFFSET 1;


CREATE TABLE queries (
id 	auto_increment PRIMARY KEY NOT NULL,
query VARCHAR(10000) NOT NULL)
SEGMENTED BY HASH(id) ALL NODES;
