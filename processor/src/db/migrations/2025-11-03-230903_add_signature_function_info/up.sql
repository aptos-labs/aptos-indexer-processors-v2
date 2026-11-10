-- Your SQL goes here
ALTER TABLE signatures
ADD COLUMN IF NOT EXISTS function_info VARCHAR(1000);
