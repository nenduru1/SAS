/**********************************************************************
Project:	Prostate Patient Data Mock Project
JOBID/SID:	reev_vdw_dev
PI:			Donald McCarthy
Co-PI:	

Program:	provider_map.sas
Location:	/reev/proj/reev_vdw_dev/programs/provider/prov_map.sas(old)
			/reev/proj/reev_vdw_dev/programs/provider/prov_map_updated.sas
Author:		Nitesh Enduru
Purpose:	mapping specilaties for the VDW provider V4

Input(s):	dl_res_svc.util_prov_master
			resevl.cpm_prov_spclty
			
Format(s):	
Macro(s):	%map_specialty	

Output(s):/apps/sas/proj22/reev_vdw_dev/output/provider_final.sas7bdat	
Format(s):	
Macro(s):

Key Variables: 	Optional, if there is any
Notes: 	Optional, if there is any (e.g., note other JOBID/SID associated with this program due to cost center changes)

History:
Date			Written by		Type			Description
---------		----------  	---------		---------------------------	
18JUL2016		Ntesh Enduru	Original
31MAY2017		Ntesh Enduru	modified		included all prov_cpm_id records and modified logic accordingly
**********************************************************************/
%include "/home/o222069/mypad.sas";
%include "/home/o222069/login.txt";
*%include '/apps/sas/proj22/reev_vdw_dev/programs/util/map_macro.sas';

*path to save your SAS datasets to your project folder;
libname prov "/apps/sas/proj22/reev_vdw_dev/output";

libname  map "/apps/sas/proj22/reev_vdw_dev/programs/provider";

*time trace, timing information for DBMS calls is sent to the log;
options compress=yes sastrace=',,,s' sastraceloc=saslog no$stsuffix;




/* create variables provider provdr_brthyr gender kpsc prov_sid prov_cpm_id and data source.*/
data reevtemp.provider;
set dlsvc.util_prov_master;
provider=put(prov_id,best.);
provider_brthyr=datepart(prov_dob);
provider_gender=prov_sex;
kpsc_prov_sid=prov_sid;
kpsc_prov_cpm_id=prov_cpm_id;
kpsc_datasource=datasource;
format provider_brthyr year4.;
*drop prov_id prov_dob;
/*if prov_cpm_id ne ' ';*/
/*output;*/
run;

proc sql;create table splw as
	select distinct * from reevtemp.provider p  left join ora.cpm_prov_spclty  c on 
	p.prov_cpm_id=c.rsrce_id /*where  rsrce_id is not null */;
quit;


proc sql;create table prov_specialty as
	select distinct kpsc_datasource,prvdr_spclty,provider,rsrce_id from splw
order by /*prov_cpm_id,*/ provider,kpsc_datasource;
quit;

proc transpose data=prov_specialty out=t_prov_specialty(drop= _NAME_ _LABEL_) prefix=description;
var prvdr_spclty;
by /*prov_cpm_id*/ provider kpsc_datasource;
run;
/*
proc sql;
select count(distinct prov_id) from dlsvc.util_prov_master;
quit;
*/

proc sql;
create table prov_spl as select  distinct 
  t1.PROV_ID, 
          t1.PROV_TYPE, 
          t1.PROV_CPM_ID, 
          t1.provider, 
          t1.provider_brthyr, 
          t1.provider_gender, 
          t1.kpsc_prov_sid, 
          t1.kpsc_prov_cpm_id, 
          t1.kpsc_datasource, 
          t2.description1, 
          t2.description2, 
          t2.description3, 
          t2.description4 from reevtemp.provider t1 left join t_prov_specialty t2 on t1.provider=t2.provider /*where t.prov_cpm_id is not null*/;
*create table prov_spl2 as select  * from prov.provider p left join prov.t_prov_specialty t on p.provider=t.provider;

quit; 

PROC SQL;
   CREATE TABLE prov_spl_typ AS 
   SELECT DISTINCT t1.PROV_ID, 
          t1.PROV_TYPE, 
          t1.PROV_CPM_ID, 
          t1.provider, 
          t1.provider_brthyr, 
          t1.provider_gender, 
          t1.kpsc_prov_sid, 
          t1.kpsc_prov_cpm_id, 
          t1.kpsc_datasource, 
          t1.description1, 
          t1.description2, 
          t1.description3, 
          t1.description4
      FROM WORK.PROV_SPL t1
           left join DLSVC.util_prov_master t2 ON (t1.PROV_ID = t2.PROV_ID);
/*      WHERE t1.PROV_TYPE = t2.PROV_TYPE;*/
/*      ORDER BY t1.PROV_CPM_ID;*/
QUIT;

/*selecting year*/
proc sql;
	create table yr as select distinct rsrce_id,msoc_yr_rcvd_tx from ora.cpm_prov_edu
/*where rsrce_id is not null*/
order by rsrce_id,msoc_yr_rcvd_tx desc;
quit;

/*selecting the latest year of graduate*/
data yr_grad;
set yr;
by rsrce_id;
if first.rsrce_id;
run;

proc sql; create table prov_yr as
	select * from prov_spl_typ p left join yr_grad y on p.prov_cpm_id = y.rsrce_id
	order by rsrce_id;
	quit;


proc freq data=ora.cpm_prov;
table ethnicity;
run;

/*selecting race */
proc sql;
create  table race as select * from prov_yr p left join ora.cpm_prov  c on 
	p.prov_cpm_id=c.rsrce_id /*where prov_cpm_id is not null*/ ;
quit;

PROC SQL;
   CREATE TABLE WORK.prov_race AS 
   SELECT DISTINCT t1.prov_id,t1.PROV_CPM_ID, 
   		  t1.PROV_TYPE,
          t1.provider, 
          t1.provider_brthyr, 
          t1.provider_gender, 
          t1.kpsc_prov_sid, 
          t1.kpsc_prov_cpm_id, 
          t1.kpsc_datasource,
		  t1.description1, 
          t1.description2, 
          t1.description3, 
          t1.description4, 
          input(t1.msoc_yr_rcvd_tx,4.) as year_graduated, 
          t1.ETHNICITY as provider_race,case when ethnicity='' then 'U' when ethnicity='HISPAN' 
			then 'Y' else 'N' end as provider_hispanic
      FROM WORK.RACE t1;
QUIT;


proc freq data=prov_race;
table provider_race provider_hispanic;
run;

proc sort data= prov_race out=s_prov_race;
by  descending description4 descending description3 descending description2 descending description1;
run;
/*retrieving multiple specialties for PROV_CPM_ID */
proc sql; create table p_map as
	select * ,
		case when description1 in (select prvdr_spclty from map.spclty m ) then (
			select specialty from map.spclty m where s.description1= m.prvdr_spclty) end as spl11,

			case when description1 in (select prvdr_spclty from map.spclty m ) then (
			select specialty2 from map.spclty m where s.description1= m.prvdr_spclty) end  as spl12 ,

			case when description1 in (select prvdr_spclty from map.spclty m ) then (
			select specialty3 from map.spclty m where s.description1= m.prvdr_spclty) end  as spl13 ,

		case when description2 in (select prvdr_spclty from map.spclty m ) then (
			select specialty from map.spclty m where s.description2= m.prvdr_spclty) end as spl21,

			case when description2 in (select prvdr_spclty from map.spclty m ) then (
			select specialty2 from map.spclty m where s.description2= m.prvdr_spclty) end as spl22,

			case when description2 in (select prvdr_spclty from map.spclty m ) then (
			select specialty3 from map.spclty m where s.description2= m.prvdr_spclty) end as spl23,

		case when description3 in (select prvdr_spclty from map.spclty m ) then (
			select specialty from map.spclty m where s.description3= m.prvdr_spclty) end as spl31,

			case when description3 in (select prvdr_spclty from map.spclty m ) then (
			select specialty2 from map.spclty m where s.description3= m.prvdr_spclty) end as spl32,

			case when description3 in (select prvdr_spclty from map.spclty m ) then (
			select specialty3 from map.spclty m where s.description3= m.prvdr_spclty) end as spl33,

		case when description4 in (select prvdr_spclty from map.spclty m ) then (
			select specialty from map.spclty m where s.description4= m.prvdr_spclty) end as spl41,

			case when description4 in (select prvdr_spclty from map.spclty m ) then (
			select specialty2 from map.spclty m where s.description4= m.prvdr_spclty) end as spl42,

			case when description4 in (select prvdr_spclty from map.spclty m ) then (
			select specialty3 from map.spclty m where s.description4= m.prvdr_spclty) end as spl43

from s_prov_race s
;
quit;


PROC SORT
	DATA=P_MAP(KEEP=spl11 spl12 spl13 spl21 spl22 spl23 spl31 spl32 spl33 spl41 spl42 spl43 provider kpsc_datasource provider_race provider_hispanic)
	OUT=WORK.SORTTempTableSorted
	;
	BY /*prov_cpm_id*/ provider;
RUN;
PROC TRANSPOSE DATA=WORK.SORTTempTableSorted
	OUT=TRNSTRANSPOSEDP_MAP(LABEL="Transposed PROV.P_MAP")
	PREFIX=Column
	NAME=Source
	LABEL=Label
;
	BY  /*prov_cpm_id*/ provider kpsc_datasource provider_race provider_hispanic;
	VAR spl11 spl12 spl13 spl21 spl22 spl23 spl31 spl32 spl33 spl41 spl42 spl43;

RUN; QUIT;
proc sql;
select count(distinct provider) from TRNSTRANSPOSEDP_MAP;
quit;
PROC SQL ;
   CREATE TABLE QUERY_FOR_TRNSTRANSPOSEDP_MAP AS 
   SELECT DISTINCT t1.provider, /*t1.prov_cpm_id,*/kpsc_datasource,provider_race, provider_hispanic,
          t1.Source, 
          t1.Column1
      FROM TRNSTRANSPOSEDP_MAP t1;
/*       where t1.Column1 NOT = '';*/
QUIT;

proc sql;
select count(distinct provider) from QUERY_FOR_TRNSTRANSPOSEDP_MAP;
quit;

PROC SQL;
	CREATE table SORTTempTableSorted_Map AS
		SELECT distinct T.Column1, T.provider/*,T.prov_cpm_id*/,T.kpsc_datasource,provider_race, provider_hispanic
	FROM QUERY_FOR_TRNSTRANSPOSEDP_MAP as T group by provider,Column1 order by Column1 desc
;
QUIT;
proc sort data=SORTTempTableSorted_Map;
by /*prov_cpm_id*/ provider;
run;

PROC TRANSPOSE DATA=SORTTempTableSorted_Map
	OUT=TRANS_FINAL_PROV(LABEL="Transposed WORK.QUERY_FOR_TRNSTRANSPOSEDP_MAP")
	PREFIX=specialty
	NAME=Source
	LABEL=Label
;
	BY  /*prov_cpm_id*/ provider kpsc_datasource provider_race provider_hispanic;
	VAR Column1;

RUN; QUIT;


PROC SQL;
   CREATE TABLE TRANS_FINAL_PROV_SORT AS 
   SELECT /* obs */
            (monotonic()) LABEL="obs" AS obs, 
          t1.provider, 
/*		  t1.prov_cpm_id,*/
		  t1.kpsc_datasource,provider_race, provider_hispanic,
          t1.specialty1, 
          t1.specialty2, 
          t1.specialty3, 
          t1.specialty4, 
          t1.specialty5, 
          t1.specialty6
      FROM TRANS_FINAL_PROV t1
      ORDER BY t1.specialty6 DESC,
               t1.specialty5 DESC,
               t1.specialty4 DESC,
               t1.specialty3 DESC,
               t1.specialty2 DESC,
               t1.specialty1 DESC;
QUIT;


%macro map_specialty();
%let i=1;
%let j=2;
data prov.prov_spl;
set TRANS_FINAL_PROV_SORT;

%do j=2 %to 6;
	if specialty&i eq specialty&j then
	specialty&j ='';
	else specialty&j=specialty&j;
%end;

%let i=2;
%let j=3;

%do j=3 %to 6;
		if specialty&i eq specialty&j then
		specialty&j ='';
		else if specialty&i eq '' and specialty&j ne '' then  do;
			specialty&i=specialty&j;specialty&j='';
			end;
		else specialty&j=specialty&j;
%end;



%let i=3;
%let j=4;
%do j=4 %to 6;
	if specialty&i eq specialty&j then
	specialty&j ='';
	else if specialty&i eq '' and specialty&j ne '' then  do;
		specialty&i=specialty&j;specialty&j='';
		end;
	else specialty&j=specialty&j;
%end;

%let i=4;
%let j=5;
%do j=5 %to 6;
	if specialty&i eq specialty&j then
	specialty&j ='';
	else if specialty&i eq '' and specialty&j ne '' then  do;
		specialty&i=specialty&j;specialty&j='';
		end;
	else specialty&j=specialty&j;
%end;

%let i=5;
%let j=6;

	if specialty&i eq specialty&j then
	specialty&j ='';
	else if specialty&i eq '' and specialty&j ne '' then  do;
		specialty&i=specialty&j;specialty&j='';
		end;
	else specialty&j=specialty&j;


run;
%mend;
%map_specialty();

PROC SQL;
   CREATE TABLE PROV.prov_merge AS 
   SELECT distinct t1.provider, 
/*   			t1.prov_cpm_id,*/
          t1.specialty1, 
          t1.specialty2, 
          t1.specialty3, 
          t1.specialty4, 
          t1.specialty5, 
          t2.PROV_TYPE, 
          t2.provider_brthyr, 
          t2.provider_gender, 
          t2.provider_race, 
          t2.provider_hispanic, 
          t2.kpsc_prov_sid, 
          t2.kpsc_prov_cpm_id,
		  t2.year_graduated, 
          t1.kpsc_datasource
      FROM PROV.PROV_SPL t1
           left JOIN P_MAP t2 ON (t1.provider = t2.provider) 
/*		where t1.prov_cpm_id ne ''*/;
QUIT;


proc sql; create table prov.prov_final as
	select left(provider) as provider,kpsc_prov_sid,kpsc_prov_cpm_id,specialty1,specialty2,specialty3,specialty4,specialty5,
		case when prov_type in (select prov_type from map.prov_type m ) then (
			select provider_type from map.prov_type m where s.prov_type= m.prov_type) end as provider_type,
			provider_brthyr as provider_birth_year,provider_gender /*format=$gender.*/,provider_race /*format=$race.*/,provider_hispanic /*format=$p_hisp.*/,
			year_graduated,kpsc_datasource

from prov.prov_merge s ;
quit;


proc sort data=prov.prov_final nodupkey out=te; by  provider descending provider_birth_year ;run;

data prov.provider_final;
*attrib  provider_race length=$2  provider_gender length=$1;
set te;
by provider /*kpsc_prov_cpm_id*/;
if provider_race = 'AMINDN' then provider_race = 'IN';/*'American Indian / Alaskan Native'*/
if provider_race = 'ASIAN' then provider_race= 'AS';/*'Asian'*/
if provider_race= 'BLACK' then provider_race = 'BA';/*'Black or African American'*/
if provider_race = 'HISPAN' then provider_race = 'OT';/*'Other'*/
if provider_race = 'OTHER' then provider_race = 'OT';/*'Other'*/
if provider_race = 'PACISL' then provider_race = 'HP';/*'Native Hawaiian / Pacific Islander'*/
if provider_race = 'WHITE' then provider_race = 'WH';/*'White'*/
if provider_race = ' ' then provider_race = 'UN';/*'Missing'*/
/*if first.kpsc_prov_cpm_id;*/
if provider_gender='' then provider_gender='U';
if provider_race='' then provider_race='UN';
run;

proc datasets lib=prov;
modify provider_final ;
rename specialty1=specialty;
*informat provider_race $2. provider_gender $1. ;
quit;
run;

proc sql;
alter table prov.provider_final
    modify provider_gender char(1), provider_race char(2);
quit;

/*
proc freq data=prov.provider_final;
table provider_race provider_hispanic;
run;


proc freq data=PROV.prov_merge;
table provider_race provider_hispanic;
run;
*/
