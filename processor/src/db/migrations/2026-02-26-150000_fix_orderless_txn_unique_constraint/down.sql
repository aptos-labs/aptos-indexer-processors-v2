-- Revert to the original blanket unique constraint.
DROP INDEX IF EXISTS user_transactions_sender_sequence_number_key;

ALTER TABLE user_transactions ADD CONSTRAINT user_transactions_sender_sequence_number_key
UNIQUE (sender, sequence_number);
