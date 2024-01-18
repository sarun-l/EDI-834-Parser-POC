
DROP VIEW IF EXISTS public.EDI834;
DROP TABLE IF EXISTS public."PER";
DROP TABLE IF EXISTS public."DMG";
DROP TABLE IF EXISTS public."DTP";
DROP TABLE IF EXISTS public."HD";
DROP TABLE IF EXISTS public."N3";
DROP TABLE IF EXISTS public."N4";
DROP TABLE IF EXISTS public."NM1";
DROP TABLE IF EXISTS public."REF";
DROP TABLE IF EXISTS public.ins;
DROP TABLE IF EXISTS public.metadata;

CREATE TABLE public.metadata (
	metadata_id serial4 NOT null primary KEY,
	filename text NULL,
	interchange_control_number text NULL,
	sponsorname text NULL,
	payername text null,
	processeddate date null
);

CREATE TABLE public.INS (
	ins_id serial4 NOT null primary KEY,
	metadata_id int NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text NULL,
	col4 text NULL,
	col5 text NULL,
	col6 text NULL,
	col7 text NULL,
	col8 text null,
	CONSTRAINT fk_metadata
      FOREIGN KEY(metadata_id) 
	  REFERENCES metadata(metadata_id)
);

CREATE TABLE public."DMG" (
	dmg_id serial4 NOT null primary KEY,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text null,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."DTP" (
	dtp_id serial4 NOT null primary KEY,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text null,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."HD" (
	hd_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text NULL,
	col4 text NULL,
	col5 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."N3" (
	n3_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."N4" (
	n4_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."NM1" (
	nm1_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text NULL,
	col4 text NULL,
	col5 text NULL,
	col6 text NULL,
	col7 text NULL,
	col8 text NULL,
	col9 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."REF" (
	ref_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE TABLE public."PER" (
	per_id serial4 NOT null primary key,
	ins_id int4 NULL,
	col1 text NULL,
	col2 text NULL,
	col3 text NULL,
	col4 text NULL,
	CONSTRAINT fk_ins
      FOREIGN KEY(ins_id) 
	  REFERENCES ins(ins_id)
);

CREATE VIEW EDI834 AS
SELECT  meta.metadata_id , meta.interchange_control_number , meta.sponsorname , meta.payername
		,ins.ins_id , case when ins.col1 = 'Y' then 'SUB' else 'DEP' end as member_type
		,ins.col2 as relationship_code, ins.col3 as maintenence_type_code, ins.col4 as maintenence_reason_code, ins.col5 as benefit_status_code
		,ins.col8 as employment_status_code
		,ref.col2 as subscriber_id
		,dtp.col3 as coverage_start_date
		,dtp2.col3 as coverage_end_date
		,nm1.col3 as lastname, nm1.col4 as firstname, nm1.col5 as middleinitial, nm1.col6 as suffix
		,nm1.col9 as SSN
		,per.col4 as phone
		,n3.col1 as street_address
		,n4.col1 as city, n4.col2 as state, n4.col3 as postalcode
		,dmg.col2 as dateofbirth, dmg.col3 as gender
		,hd.col5 as HD_Code5
FROM public.metadata meta
inner join public.ins ins
	on meta.metadata_id = ins.metadata_id 
LEFT join "REF" ref
	on ins.ins_id = ref.ins_id 
	and ref.col1 = '0F'
left join "DTP" dtp
	on ins.ins_id = dtp.ins_id 
	and dtp.col1 = '348'
left join "DTP" dtp2
	on ins.ins_id = dtp2.ins_id 
	and dtp2.col1 = '349'
left join "NM1" nm1
	on ins.ins_id = nm1.ins_id
	and nm1.col1 = 'IL'
left join "PER" per
	on ins.ins_id = per.ins_id
left join "N3" n3
	on ins.ins_id = n3.ins_id
left join "N4" n4
	on ins.ins_id = n4.ins_id
left join "DMG" dmg
	on ins.ins_id = dmg.ins_id
left join "HD" hd
	on ins.ins_id = hd.ins_id;
