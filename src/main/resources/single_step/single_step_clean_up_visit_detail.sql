-- reset visit_detail table

--ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_preceding;
--COMMIT;

DELETE FROM visit_detail;


--ALTER TABLE visit_detail ADD CONSTRAINT fpk_v_detail_preceding FOREIGN KEY (preceding_visit_detail_id) REFERENCES visit_detail (visit_detail_id);


