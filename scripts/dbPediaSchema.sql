CREATE TABLE dbpedia_triples (
id 	INT PRIMARY KEY NOT NULL,
subject VARCHAR(4000) NOT NULL,
predicate VARCHAR(4000),
object VARCHAR(4000),
object_tokenized VARCHAR(4000)
)
SEGMENTED BY HASH(id) ALL NODES;


CREATE PROJECTION dbpedia_object_tokenized_to_SP (
object_tokenized,
subject,
predicate,
object
) AS
SELECT object_tokenized, subject, predicate, object
FROM dbpedia_triples
ORDER BY object_tokenized, subject, predicate, object
SEGMENTED BY HASH(object_tokenized) ALL NODES;

CREATE PROJECTION dbpedia_object_tokenized_to_SP_b1 (
object_tokenized,
subject,
predicate,
object
) AS
SELECT object_tokenized, subject, predicate, object
FROM dbpedia_triples
ORDER BY object_tokenized, subject, predicate, object
SEGMENTED BY HASH(object_tokenized) ALL NODES OFFSET 1;


CREATE PROJECTION dbpedia_object_tokenized_to_PS (
object_tokenized,
predicate,
subject,
object
) AS
SELECT object_tokenized, predicate, subject, object
FROM dbpedia_triples
ORDER BY object_tokenized, predicate, subject, object
SEGMENTED BY HASH(object_tokenized) ALL NODES;


CREATE PROJECTION dbpedia_object_tokenized_to_PS_b1 (
object_tokenized,
predicate,
subject,
object
) AS
SELECT object_tokenized, predicate, subject, object
FROM dbpedia_triples
ORDER BY object_tokenized, predicate, subject, object
SEGMENTED BY HASH(object_tokenized) ALL NODES OFFSET 1;

