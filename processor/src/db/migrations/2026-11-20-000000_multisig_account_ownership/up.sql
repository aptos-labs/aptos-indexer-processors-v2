-- Current `0x1::multisig_account::MultisigAccount` ownership, derived from
-- committed resources so an owner who has never sent a transaction is found.

-- One row per multisig account ever seen. `last_transaction_version` is the
-- version of the last `MultisigAccount` resource write or delete applied, and
-- guards every write to this account's memberships. A null quorum marks a
-- deleted resource: the row stays so an older write cannot resurrect it.
CREATE TABLE multisig_account_states (
    multisig_address VARCHAR(66) COLLATE "C" PRIMARY KEY,
    last_transaction_version NUMERIC(20,0) NOT NULL
        CHECK (last_transaction_version BETWEEN 0 AND 18446744073709551615),
    num_signatures_required NUMERIC(20,0)
        CHECK (num_signatures_required BETWEEN 1 AND 18446744073709551615)
);

CREATE TABLE multisig_account_memberships (
    multisig_address VARCHAR(66) COLLATE "C" NOT NULL
        REFERENCES multisig_account_states(multisig_address),
    owner_address VARCHAR(66) COLLATE "C" NOT NULL,
    PRIMARY KEY (multisig_address, owner_address)
);

CREATE INDEX multisig_account_memberships_owner_multisig_idx
    ON multisig_account_memberships (owner_address, multisig_address);

-- The relation consumers query: one row per (live account, current owner).
CREATE VIEW current_multisig_account_owners AS
    SELECT m.multisig_address, m.owner_address, s.num_signatures_required, s.last_transaction_version
    FROM multisig_account_memberships m
    JOIN multisig_account_states s USING (multisig_address)
    WHERE s.num_signatures_required IS NOT NULL;
