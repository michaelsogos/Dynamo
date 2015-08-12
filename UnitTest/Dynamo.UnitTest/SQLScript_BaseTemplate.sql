

CREATE TABLE IF NOT EXISTS FirstTable
(
	ID        INTEGER     PRIMARY KEY ,
	Name STRING NOT NULL,
	Test INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS FourthTable
(
	ID INTEGER     INTEGER     PRIMARY KEY ,
	ParentID INTEGER NOT NULL,
	Name STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS SecondTable(
	ID INTEGER     INTEGER     PRIMARY KEY ,
	ParentID INTEGER NOT NULL,
	Name STRING NOT NULL,
	AData timestamp NOT NULL,
	UTCDate timestamp  NOT NULL ,
	IntegerNumber integer NOT NULL,
	DecimalNumber decimal(18,2) NOT NULL,
	BooleanValue boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS ThirdTable(
	ID INTEGER     INTEGER     PRIMARY KEY ,
	ParentID INTEGER NOT NULL,
	Name STRING NOT NULL,
	NullableInt integer NULL,
	NullableDate timestamp NULL,
	NullableBoolean boolean NULL
);
