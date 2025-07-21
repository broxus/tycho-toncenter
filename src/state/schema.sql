--
CREATE TABLE IF NOT EXISTS node_state (
    param   INTEGER NOT NULL PRIMARY KEY,
    value   BLOB
) WITHOUT ROWID;

--
CREATE TABLE IF NOT EXISTS known_interfaces (
    code_hash   BLOB NOT NULL PRIMARY KEY,
    interface   INTEGER NOT NULL,
    is_broken   INTEGER NOT NULL
);

--
CREATE TABLE IF NOT EXISTS jetton_masters (
    address             BLOB NOT NULL PRIMARY KEY,
    total_supply        BLOB NOT NULL,
    mintable            INTEGER NOT NULL,
    admin_address       BLOB,
    jetton_content      TEXT,
    wallet_code_hash    BLOB NOT NULL,
    last_transaction_lt INTEGER NOT NULL,
    code_hash           BLOB NOT NULL,
    data_hash           BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS jetton_masters_index_1 ON jetton_masters (admin_address);

--
CREATE TABLE IF NOT EXISTS jetton_wallets (
    address             BLOB NOT NULL PRIMARY KEY,
    balance             BLOB NOT NULL,
    owner               BLOB NOT NULL,
    jetton              BLOB NOT NULL,
    last_transaction_lt INTEGER NOT NULL,
    code_hash           BLOB NOT NULL,
    data_hash           BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS jetton_wallets_index_1 ON jetton_wallets (owner);
CREATE INDEX IF NOT EXISTS jetton_wallets_index_2 ON jetton_wallets (jetton);
CREATE INDEX IF NOT EXISTS jetton_wallets_index_3 ON jetton_wallets (owner ASC, balance DESC);
CREATE INDEX IF NOT EXISTS jetton_wallets_index_4 ON jetton_wallets (jetton ASC, balance DESC);

--
INSERT OR IGNORE INTO db_version (id, major, minor, patch)
VALUES (1, 1, 0, 0);
