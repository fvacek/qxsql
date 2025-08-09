create table events
(
    id INTEGER primary key autoincrement,
    api_token  TEXT,
    owner      TEXT,
    constraint events_api_token_uindex unique (api_token)
);
