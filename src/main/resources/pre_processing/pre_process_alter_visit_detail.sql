Do
$$
DECLARE v_rowCount int;
BEGIN
--Temporarily deleted the FK constraints for the column precending_visit_detail_id
ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_preceding;
COMMIT;
ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_parent;
COMMIT;
END
$$;
