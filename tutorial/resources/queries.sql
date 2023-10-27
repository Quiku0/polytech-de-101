drop table if exists consolidate.consolidate_gists;
create table consolidate.consolidate_gists(
    "id"            VARCHAR(256)  primary KEY
    ,"url"          TEXT
    ,"commits_url"  TEXT
    ,"comments_url" TEXT 
    ,"public"       BOOL 
    ,"created_at"   TEXT 
    ,"updated_at"   TEXT 
    ,"description"  TEXT
    ,"comments"     INTEGER 
    ,"owner.login"  VARCHAR(256) 
    ,"owner.id"     VARCHAR(256) 
);


drop table if exists consolidate.consolidate_gist_files;
create table consolidate.consolidate_gist_files(
    "filename"    TEXT
    ,"id_gist"    TEXT
    ,"type"       TEXT 
    ,"language"   TEXT
    ,"raw_url"    TEXT
    ,"size"       INTEGER
    , PRIMARY KEY ("filename", "id_gist")
);


drop table if exists consolidate.consolidate_gist_commits;
create table consolidate.consolidate_gist_commits(
    "version"                   VARCHAR(256)
    ,"id_gist"                  VARCHAR(256)
    ,"committed_at"             VARCHAR(256) 
    ,"url"                      TEXT 
    ,"change_status.deletions"  INTEGER
    ,"change_status.additions"  INTEGER
    ,"change_status.total"      INTEGER
    , PRIMARY KEY ("version", "id_gist")
);

drop table if exists aggregation.dim_gists;
create table aggregation.dim_gists(
    "id"            VARCHAR(256)  primary KEY
    ,"url"          TEXT
    ,"public"       BOOL 
    ,"created_at"   TEXT 
    ,"updated_at"   TEXT 
    ,"description"  TEXT
    ,"comments"     INTEGER
);

drop table if exists aggregation.dim_gist_commits;
create table aggregation.dim_gist_commits(
    "version"                   VARCHAR(256)
    ,"id_gist"                  VARCHAR(256)
    ,"committed_at"             TEXT 
    ,"url"                      TEXT
    ,"change_status.deletions"  INTEGER
    ,"change_status.additions"  INTEGER
    ,"change_status.total"      INTEGER
    , PRIMARY KEY ("version", "id_gist")
);

drop table if exists aggregation.dim_gist_files;
create table aggregation.dim_gist_files(
    "filename"    TEXT
    ,"id_gist"    VARCHAR(256)
    ,"type"       TEXT 
    ,"language"   TEXT
    ,"size"       INTEGER
    , PRIMARY KEY ("filename", "id_gist")
);