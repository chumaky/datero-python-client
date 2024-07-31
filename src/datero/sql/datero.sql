-- stmt
CREATE TABLE IF NOT EXISTS datero.servers
( id                SERIAL          PRIMARY KEY
, name              VARCHAR(50)     NOT NULL CONSTRAINT servers_name_uk UNIQUE 
, fdw_name          VARCHAR(100)    NOT NULL
, description       VARCHAR(4000)
, custom_options    JSONB
, created           TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
, modified          TIMESTAMP
);
