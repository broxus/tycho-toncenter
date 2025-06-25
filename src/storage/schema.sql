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

--
INSERT OR IGNORE INTO db_version (id, major, minor, patch)
VALUES (1, 1, 0, 0);
