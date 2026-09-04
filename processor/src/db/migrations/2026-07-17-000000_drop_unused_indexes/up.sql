-- Drop unused/redundant indexes (INC-22633). Confirmed 0 reads on the read-pool replicas;
-- dropped by hand with DROP INDEX CONCURRENTLY on prod, so this is a no-op there and applies
-- on fresh/small DBs.

DROP INDEX IF EXISTS o_object_skh_idx;
DROP INDEX IF EXISTS o_insat_idx;
DROP INDEX IF EXISTS faa_insat_idx;
DROP INDEX IF EXISTS cufab_insat_index;
DROP INDEX IF EXISTS co_object_skh_idx;

-- Orphaned invalid indexes left by the v1->v2 migration (never valid).
DROP INDEX IF EXISTS lm1_ccb_ct_a_cfaab_index;
DROP INDEX IF EXISTS lm1_cv_ci_tv_index;
DROP INDEX IF EXISTS lm1_ccb_ct_a_index;
DROP INDEX IF EXISTS lm1_curr_to_oa_tt_ltv_index;
DROP INDEX IF EXISTS lm1_curr_to_oa_tt_am_ltv_index;
DROP INDEX IF EXISTS lm1_ca_ct_a_index;
DROP INDEX IF EXISTS lm1_ca_oa_ct_at_index;
DROP INDEX IF EXISTS lm1_ca_oa_igf_index;
DROP INDEX IF EXISTS lm1_ca_ct_at_a_index;
DROP INDEX IF EXISTS lm1_ta_tdih_pv_index;
