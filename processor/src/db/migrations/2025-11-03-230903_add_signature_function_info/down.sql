-- This file should undo anything in `up.sql`
ALTER TABLE signatures
DROP COLUMN IF EXISTS function_info;