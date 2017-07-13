/**********************************************************************
Project: 	Program to map the department specialties as per VDW V4.	
JOBID/SID:	
PI:			Donald P McCarthy	
Co-PI:	

Program:	'/apps/sas/proj22/reev_vdw_dev/programs/dept_map/dept_map.sas'
Location:	
Author:		Nitesh Enduru
Purpose:	updating the department specilaty in util table to VDW V4 standard 

Input(s):	'/apps/sas/proj22/reev_vdw_dev/programs/dept_map/data/'

Format(s):	
Macro(s):	

Output(s):	'/apps/sas/proj22/reev_vdw_dev/ouput/mapped.sas7bdat'
			'/apps/sas/proj22/reev_vdw_dev/ouput/map_dept_spclty.sas7bdat'
Format(s):	
Macro(s):
log:		'/apps/sas/proj22/reev_vdw_dev/programs/dept_map/Log for dept_map.log'	
reference: 	"/apps/sas/proj22/reev_vdw_dev/programs/util/department_mapping"

Key Variables:
Notes: 	

History:
Date			Written by		Type			Description
---------		----------  	---------		---------------------------	
02AUG2016		Ntesh Enduru	Original
21SEP2016		Nitesh			Modified		updated the dept_specialty mapping for null_codes data
03Feb2017		Nitesh			Modified		updated ncoa ucoa cos mapping files	
**********************************************************************/

%LET _CLIENTTASKLABEL='dept_map';
%LET _CLIENTPROJECTPATH='/reev/proj/reev_vdw_dev/programs/dept_map/dept_map.egp';
%LET _CLIENTPROJECTNAME='dept_map.egp';
%LET _SASPROGRAMFILE='/reev/proj/reev_vdw_dev/programs/dept_map/dept_map.sas';

GOPTIONS ACCESSIBLE;
%include "/home/o222069/login.txt";
%include '/apps/sas/proj22/reev_vdw_dev/programs/util/map_macro.sas';

*Data Lab libname;
libname dlsvc teradata tdpid=tdp2 schema=DL_RES_SVC user=&user  pw=&pwd ;*tpt=yes fastexport=yes dbcommit=0 fastload=yes;
libname dluser teradata tdpid=tdp2 schema=DL_RES_USER user=&user pw=&pwd;

*path to save your SAS datasets to your project folder;
libname mypath '/apps/sas/proj22/reev_vdw_dev/programs/dept_map/data';
libname out '/apps/sas/reevtmp';
*time trace, timing information for DBMS calls is sent to the log;
options compress=yes sastrace=',,,s' sastraceloc=saslog no$stsuffix;
/* import ncoa_ucoa ad ncoa_def xls file using import procedure and move to mypath lib*/
data mypath.ncoa_ucoa_09252016(drop=Bus_Unit bus_unit_descr);
set ncoa_ucoa_09252016(where=(Bus_Unit like'08%'));
run;

proc sort data=MYPATH.NCOA_UCOA_09252016 nodup; by dept_ucoa;run;


/*creating util_loc_master table in mypath lib*/
proc sql;
	connect to teradata (tdpid=tdp2 user=&user pw=&pwd connection=global);
	create table mypath.util_loc_master as
		select * from connection to teradata
			(    		     
		select *
			from dl_res_svc.util_loc_master 
			);
	disconnect from teradata;
quit;

/*creating clarity_dep table in mypath lib*/
proc sql;
	connect to teradata (tdpid=tdp2 user=&user pw=&pwd connection=global);
	create table mypath.clarity_dep as
		select * from connection to teradata
			(    		     
		select *
			from HCCLSC.clarity_dep 
			);
	disconnect from teradata;
quit;

/*converting dept_sid to character*/
data mypath.clarity_dep( rename=(department_id=dept_id dept_id=department_id ));
	set mypath.clarity_dep;
	dept_id=left(put(department_id,19.));
run;

/*select distinct data from department mapping*/
proc sql;
	create table combined_data_left as 
		select distinct u.loc_sid, 
			u.loc_id,
			u.dept_ncoa as util_dept_ncoa, 
			u.dept_ucoa as util_dept_ucoa, 
			u.dept_specialty as util_specialty,  
			u.datasource, 
			u.dept_coa,c.department_id,c.dept_id
		from mypath.util_loc_master u
		left join mypath.clarity_dep c on (u.loc_sid = c.department_id);
quit;

/*data u1268;*/
/*set combined_data_left;*/
/*where util_dept_ncoa='1035';*/
/*run;*/
proc sql;
create table o222_dept_spl as 
select * from combined_data_left 
where util_dept_ncoa eq '' and util_dept_ucoa eq '' and dept_coa eq '' and util_specialty ne '';
quit;


/* to find any multiple jons between loc_sid and dept_id*/
proc sql;
	create table combined_data_dup as 
		select distinct count(loc_sid) as count,*
			from combined_data_left
				group by loc_sid,loc_id,datasource,util_dept_ncoa,util_dept_ucoa,dept_coa 
					having count(loc_sid) >= 2;
quit;



/*selecting only ucoa codes */
proc sql;
	create table ucoa as 
		select distinct * from combined_data_left
			where util_dept_ucoa ne '' and util_dept_ncoa eq '' and dept_coa eq ''
				order by util_specialty;
quit;

/*selecting only ncoa codes*/
proc sql;
	create table ncoa as 
		select distinct * from combined_data_left
			where util_dept_ncoa ne '' and util_dept_ucoa eq '' and dept_coa eq ''
				order by util_specialty;
quit;

/*selecting both ncoa_ucoa codes*/
proc sql;
	create table both as 
		select distinct * from combined_data_left
			where util_dept_ncoa ne '' and util_dept_ucoa ne '' and dept_coa eq ''
				order by util_specialty;
quit;

/* selecting only coa codes*/
proc sql;
	create table coa as 
		select distinct * from combined_data_left
			where dept_coa ne ''
				order by util_specialty;
quit;

/* selecting all ncoa ucoa coa codes which are missing*/
proc sql;
	create table null_codes as 
		select distinct * from combined_data_left
			where util_dept_ncoa eq '' and util_dept_ucoa eq '' and dept_coa eq ''
				order by util_specialty;
quit;

data null_codes;
set null_codes;
length DEPT_DESC $ 40.;
DEPT_DESC=upcase(util_specialty);
drop util_specialty;
run;
/*	mapping all codes
data map_file;
	set ucoa ncoa coa both null;
run;*/


/*imported ncoa_dept_def file from http://onelinkcoatt.kp.org/OneLinkQueryToolWeb/downloadMapping.html?reset=true  as ncoa_dept_def_04122017.sas7bdat*/
/*imported ncoa_ucoa_def file from http://onelinkcoatt.kp.org/OneLinkQueryToolWeb/downloadMapping.html?reset=true  as ncoa_ucoa_09252016.sas7bdat*/

/*match ncoa codes with ncoa_dept_def table for dept description*/
proc sql;
	create table ncoa_map as
		select distinct n.*, 
			u.*
		from ncoa n
			left join mypath.NCOA_DEPT_DEF_04122017 u
				on n.util_dept_ncoa=u.dept
			order by util_specialty;
quit;

/* check for non matching ncoa*/
data only_ncoa ; 
set ncoa_map;
if util_dept_ncoa ne dept ;
/*util_dept_ncoa='';*/
run;
/* mapping dept_desc with ncoa_dept_def description based on ncoa codes*/
data ncoa_map_util;
	length DEPT_DESC $ 40.;
	set ncoa_map;
	if util_dept_ncoa ne dept then util_dept_ncoa =''; 
	if util_specialty=' ' then 
		DEPT_DESC=description;
	if util_specialty ne  ' ' and util_specialty ne description  then 
	DEPT_DESC=COALESCEC(description,util_specialty);
	else DEPT_DESC=description;
run;




/* check for duplicates	*/
proc sql;
	create table ncoa_dup as
		select count(loc_sid) as count,loc_id,dept,loc_sid
			from ncoa_map_util
				group by  loc_sid,loc_id,datasource,util_dept_ncoa
					having count > 1;
quit;

/*match merging missing ncoa from ucoa table with ncoa_ucoa_09252016 table only if one-one mapping for ncoa and ucoa codes*/
proc sql;
	create table ucoa_map as 
		select distinct u.*, 
			c.dept_ncoa,
			c.dept_ucoa, 
			c.dept_descr_ncoa
		from ucoa u
			left join mypath.ncoa_ucoa_09252016 c on (u.util_dept_ucoa = c.dept_ucoa)
				order by util_specialty;
quit;


data only_ucoa ; 
set ucoa_map;
if util_dept_ucoa ne dept_ucoa ;
/*util_dept_ncoa='';*/
run;
/* check for duplicates removing multi ucoa ncoa mappings*/
/* we see duplicates as ncoa-ucoa have many-many mapping*/
proc sql;
	create table ucoa_map_dup as
		select count(*) as count1,*
			from ucoa_map 
				group by loc_sid,loc_id,datasource,util_dept_ncoa,util_dept_ucoa 
					having count1 >= 2;
quit;


/* SELECT DISTINCT*/
proc sql;
	create table ucoa_map_distinct as
		select count(*) as count1,* from ucoa_map group by loc_sid,loc_id,datasource,util_dept_ncoa,util_dept_ucoa having count1 < 2;
quit;

/* map dept_Desc and ncoa codes based on ucoa codes  */
data ucoa_map_util (drop=dept_ncoa dept_ucoa);
	length DEPT_DESC $ 40.;
	set ucoa_map_distinct;
if dept_ucoa^=' ' and dept_ucoa=util_dept_ucoa then
		do;
		util_dept_ncoa=dept_ncoa;
			DEPT_DESC=dept_descr_ncoa;
		end;
	else
	if util_specialty ne  '' and util_specialty ne dept_descr_ncoa then
		
			DEPT_DESC=coalescec(dept_descr_ncoa,util_specialty);
	else DEPT_DESC=dept_descr_ncoa;
run;

/* select distinct duplicate records*/
PROC SQL;
	CREATE TABLE WORK.UCOA_MAP_DUP AS 
		SELECT DISTINCT t1.LOC_SID, 
			t1.LOC_ID, 
			t1.util_dept_ncoa, 
			t1.util_dept_ucoa, 
			t1.util_specialty, 
			t1.DATASOURCE, 
			t1.DEPT_COA, 
			t1.department_id
		FROM WORK.UCOA_MAP_DUP t1;
QUIT;

/*merging distinct records with dept_desc records */
data ucoa_map_util_a;
	set ucoa_map_util UCOA_MAP_DUP;
	run;
	data ucoa_map_util_all;
	set ucoa_map_util_a;
	if  util_specialty ne '' and dept_desc eq ''  
	then dept_desc=util_specialty;

run;

/* check for duplicates	*/
proc sql;
	create table ucoa_dup as
		select count(loc_sid) as count2,loc_id,util_dept_ucoa,util_dept_ncoa,loc_sid,DEPT_DESC
			from ucoa_map_util_all
				group by  loc_sid,loc_id,datasource,util_dept_ucoa
					having count2 > 1;
quit;

/*match merging both ncoa-ucoa codes*/
/* to macth both codes with ncoa_dept_def we should map ucoa to ncoa_dept_def from ncoa_ucoa_09252016 to get ncoa codes*/
data both1;
	set both;
	util_code=right(trim(util_dept_ncoa))||"-"||left(trim(util_dept_ucoa));
run;

/*merged ncoa_def and ncoa_ucoa_09252016 for ncoa_ucoa codes as  n_nu.sas7bdat*/
PROC SQL;
	CREATE TABLE mypath.n_nu AS 
		SELECT DISTINCT t1.dept, 
			t1.description, 
			t3.dept_ncoa, 
			t3.dept_descr_ncoa, 
			t3.dept_ucoa, 
			t3.dept_descr_ucoa
		FROM MYPATH.NCOA_DEPT_DEF_04122017 t1
			LEFT JOIN MYPATH.NCOA_UCOA_09252016 t3 ON (t1.dept = t3.dept_ncoa);
QUIT;

/*merging both codes in NCOA_TO_NCOA_UCOA table	*/
data both2;
	set mypath.n_nu;

	if dept ^=' ' and dept=dept_ncoa then
		clarity_code=right(trim(dept_ncoa))||"-"||left(trim(dept_ucoa));
run;

/* joining two tabes based on ucoa_ncoa codes	*/
proc sql;
	create table both_map as 
		select distinct b1.*,b2.*
			from work.both1 b1
				left join work.both2 b2 on (b1.util_code = b2.clarity_code);
quit;

/* check for duplicates	*/
proc sql;
	create table both_dup as
		select count(loc_sid) as count,loc_id,util_dept_ucoa,util_dept_ncoa,loc_sid
			from both_map
				group by  loc_sid,loc_id,util_code
					having count > 1;
quit;

/* mapping data description from dept_descr_ncoa*/
data both_map_util(keep=dept_desc loc_sid loc_id util_dept_ncoa util_dept_ucoa dept_coa datasource util_specialty department_id description util_code clarity_code);
	length DEPT_DESC $ 40.;
	set both_map;

		if util_code =clarity_code and util_code^='' then
		DEPT_DESC=description;
	else if util_specialty ne  '' and util_specialty ne description then
		DEPT_DESC=coalescec(description,util_specialty);
		else DEPT_DESC=description;
run;

/*data mapping for coa codes*/
/*COA to UCOA mapping file Data set path: /reev/proj/re_clarity/data/vdw_dept_coa_ucoa */

/* use vdw_dept_coa_ucoa dataset to map coa codes*/

proc sql;
	create table coa_map as
		select distinct loc_sid,loc_id, util_dept_ncoa, util_dept_ucoa, dept_coa, datasource, util_specialty, department_id,
			dept1,dept2,department from coa c1 left join mypath.vdw_dept_coa_ucoa c2 on (c1.dept_coa=c2.dept1)
		order by util_specialty;
quit;

/*check for duplicates*/
proc sql;
	create table coa_map_dup as
		select count(loc_sid) as c,* from coa_map group by loc_sid,loc_id,datasource,dept_coa having c > 1;
quit;

/*select distinct from duplicates*/
PROC SQL;
	CREATE TABLE WORK.COA_MAP_DUP AS 
		SELECT DISTINCT t1.LOC_SID, 
			t1.LOC_ID, 
			t1.util_dept_ncoa, 
			t1.util_dept_ucoa, 
			t1.DEPT_COA, 
			t1.DATASOURCE, 
			t1.util_specialty, 
			t1.department_id
		FROM WORK.COA_MAP_DUP t1;
QUIT;

/*check for duplicates*/
proc sql;
	create table coa_map_distinct as
		select count(loc_sid) as c,* from coa_map group by loc_sid,loc_id,datasource,dept_coa having c < 2;
quit;

/*mapping matching ucoa codes from coa		*/
data coa_map_distinct1;
	set coa_map_distinct;

	if dept1 ^='' and dept1=dept_coa then
		util_dept_ucoa=dept2;
	drop dept1 dept2;
run;
data coa_map_distinct1;
set coa_map_distinct1 COA_MAP_DUP;
run;
/* mapping ncoa using ncoa-ucoa file*/
proc sql;
	create table coa_map1 as
		select distinct c1.*,c2.* from coa_map_distinct1 c1 left join mypath.ncoa_ucoa_09252016 c2 on (c1.util_dept_ucoa=c2.dept_ucoa)
			order by util_specialty;
quit;

/*check for duplicates*/
proc sql;
	create table coa_map1_dup as
		select count(loc_sid) as c1,* from coa_map1 group by loc_sid,loc_id,datasource,util_dept_ucoa,dept_coa having c1 > 1;
quit;

/*select distinct from duplicates	*/
PROC SQL;
	CREATE TABLE WORK.COA_MAP1_DUP AS 
		SELECT DISTINCT t1.LOC_SID, 
			t1.LOC_ID, 
			t1.util_dept_ncoa, 
			t1.util_dept_ucoa, 
			t1.DEPT_COA, 
			t1.DATASOURCE, 
			t1.util_specialty, 
			t1.department_id, 
			t1.DEPARTMENT
		FROM WORK.COA_MAP1_DUP t1;
QUIT;

/*check for duplicates*/
/* we see duplicates as coa-ucoa have many-many mapping*/

proc sql;
	create table coa_map1_distinct as
		select count(loc_sid) as c1,* from coa_map1 group by loc_sid,loc_id,datasource,util_dept_ucoa,dept_coa having c1 < 2;
quit;

/* mapping ncoa codes from ucoa using coa mapping*/
data coa_map1_distinct1;
	length DEPT_DESC $40.;
	set coa_map1_distinct;

	if dept_ucoa ^='' and dept_ucoa=util_dept_ucoa then
		do;
			util_dept_ncoa=dept_ncoa;
			DEPT_DESC=dept_descr_ncoa;
		end;

	keep DEPT_DESC loc_sid loc_id util_dept_ncoa util_dept_ucoa dept_coa datasource  util_specialty department_id department;
run;

/* mapping distinct duplicates with mapped data*/
data coa_map1_distinct1_all;
	set coa_map1_distinct coa_map1_dup;
run;

/*mapping all */
data map_all_(rename=(util_dept_ncoa=DEPT_NCOA util_dept_ucoa=DEPT_UCOA util_specialty=DEPT_SPECIALTY));
	set ncoa_map_util ucoa_map_util_all both_map_util coa_map1_distinct1_all null_codes;
	DEPT_DESC=upcase(DEPT_DESC);
run;
proc sql;
create table alla as select * from map_all_ where dept_specialty ne ' ';
quit;
proc freq data=map_all_;
table dept_desc;
run;

proc sql;
select * from map_all_ where loc_id =78941;
quit;
/* adding specialties to the department*/

proc sql;
create table map_spclty as 
	select *, 
	case when a.dept_desc in(select vdw_dept_desc from  mypath.mapping_spclty_final b) then 
	(select specialty1 from mypath.mapping_spclty_final b where a.dept_desc=b.vdw_dept_desc) end as spclty1,
	case when a.dept_desc in(select vdw_dept_desc from  mypath.mapping_spclty_final b) then 
	(select specialty2 from mypath.mapping_spclty_final b where a.dept_desc=b.vdw_dept_desc) end as spclty2
	from map_all_ a
order by loc_id;
quit;

proc sql;
create table map_spclty1 as 
	select a.*,b.specialty1 as spclty1,b.specialty2 as spclty2 
	from map_all_ a left join mypath.mapping_spclty_final b
	on  a.dept_desc=b.vdw_dept_desc
	order by loc_id;
quit;

proc compare base=map_spclty compare=map_spclty1;
run;
/*final mapping table*/
data out.dept_mapped;
set map_spclty;
keep dept_desc loc_sid loc_id dept_ncoa dept_ucoa dept_coa datasource department_id dept_desc dept_specialty spclty1 spclty2;
run;


/*selecting distinct codes*/
proc sql;
	create table out.dept_spclty as 
		select distinct dept_ncoa,dept_ucoa,dept_desc,dept_specialty,dept_coa 
			from out.dept_mapped 
				order by dept_desc;
quit;

proc printto;
run;
ods listing close;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

