-- The original UNIQUE (sender, sequence_number) constraint was created before orderless
-- transactions existed. With orderless transactions, sequence_number is NULL and the
-- constraint is bypassed (NULLs are never equal in PostgreSQL). However, the blanket
-- constraint can cause failures during re-processing if ON CONFLICT (version) doesn't
-- cover it. Replace it with a partial unique index that only enforces uniqueness for
-- non-orderless transactions (where sequence_number IS NOT NULL).
ALTER TABLE user_transactions DROP CONSTRAINT IF EXISTS user_transactions_sender_sequence_number_key;

CREATE UNIQUE INDEX IF NOT EXISTS user_transactions_sender_sequence_number_key
ON user_transactions (sender, sequence_number)
WHERE sequence_number IS NOT NULL;
