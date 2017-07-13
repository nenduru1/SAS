/********************************************************************************************
*Project: VDW
*PI:

*Program:  vdw_facility.sas 
*Location:   /reev/proj/reev_vdw_dev/programs/util/vdw_facility.sas
*Author:   DMC
*Purpose:   creating facility tables from different care types

*Input(s): 

*History:
*Date        Written by  Type        Description
*----------  ----------  ----------  ----------	-------
*			DMC
*Mar 9 2017	Nitesh Enduru			addes ocps,clarity facilities
**********************************************************************************************/	
OPTIONS COMPRESS=YES;
%include "/home/o222069/login.txt";
/*	%include "/home/t501240/conn/orapwd.sas";*/
/*	%include "/home/t501240/conn/terpwd.sas";*/

	*oracle schema 1;
		libname ora1 oracle user=&orauser password=&orapwd schema=util 	path=rdwd01;

	*oracle schema 2;
		libname ora2 oracle user=&orauser password=&orapwd schema=resevl 	path=rdwd01;
		libname ora oracle user=&orauser password=&orapwd schema=resevl 	path=rdwd01;

	*temporary oracle folder;
		libname myora oracle user=&orauser password=&orapwd schema=&orauser 	path=rdwd01;

	*teradata;
		libname tera teradata tdpid=tdp2 schema=HCCLSC user=&terauser pw=&terapwd mode=TERADATA dbmstemp=yes connection=global dbcommit=0;

	*Others;
		libname rclar "/apps/sas/proj1/re_clarity/data" access=readonly; 

		libname utl "/apps/sas/proj6/vdw/programs/utilization";
		libname map "/apps/sas/proj6/vdw/programs/utilization/department_mapping";
		libname map1 "/reev/proj/vdw/lab_work/VDW_LAB/Department_Mapping_Jun/";

		libname ushare teradata user=&terauser password=&terapwd schema=HCCLSC_USHARE TDPID=TDP2 tpt=yes fastload=yes dbcommit=0;
		libname dlsvc teradata tdpid=tdp2 schema=DL_RES_SVC user=&terauser pw=&terapwd tpt=yes fastexport=yes dbcommit=0;
		libname dluser teradata tdpid=tdp2 schema=DL_RES_USER user=&terauser pw=&terapwd tpt=yes fastload=yes fastexport=yes dbcommit=0;

		libname vdw "/apps/sas/proj22/reev_vdw_dev/output" ENABLEDIRECTIO USEDIRECTIO=YES ;
		

	options compress= yes;

	
/*map of medctr to fac_code*/
/*
medctr						$254.
kpsc_fac_name				$42.
facilty_code				$3.
relationship				$1.	
relationship_history		$1.
full_address				$62.
street_address				$36.
city						$36.
state 						$2.	
zip							$9.
address_facility_type 		$1.	
latitude					8.
longitude					8.
*/



	




/* Clarity Data source 	Facilities */
%include "/home/o222069/login.txt";
	proc sql noerrorstop;

		connect to teradata (tdpid=tdp2 user=&user password=&pwd connection=global mode=TERADATA);  

			execute (drop table DL_RES_USER.fac_clarity  		) by teradata;
			execute 
			(create multiset table DL_RES_USER.fac_clarity, no fallback as 

				(
					select distinct
						 a.fac_name
						,a.medctr
						,a.fac_ncoa
						 
					from dl_res_svc.util_loc_master a
					inner join dl_res_svc.util_loc b
						on a.loc_id=b.loc_id
					left join dl_res_svc.util_main c
						on b.util_id=c.util_id
					where c.datasource='CLARITY' and (c.care_type='HOSPITAL' or c.care_subtype in (
					'OFFICE VISIT'
					,'RADIOLOGY'
					,'URGENT CARE'
					,'ANTICOAGULATION'
					,'FUTURE OR STAND'
					,'ANESTHESIA'
					))and c.adyr=2016 and fac_ncoa not in ('82300', '82301', '001')

				)

			with data ;
			) by teradata;
		disconnect from teradata;
	quit;

	proc sql;
	create table fac_clarity as 
	select * from dluser.fac_clarity;
	create table dist_medctr_cl as 
	select distinct medctr from fac_clarity;
	quit;




		


/*zip codes for CA*/
proc sql;
create table zee as select * from sashelp.zipcode where statecode in(select distinct state1 from fac_clar);
quit; 
data fac_clar;
set fac_clar;
kpsc_data_source='CLARITY';
relationship1='O';
relationship_history1='O';
address_facility_type1='C';
run;
proc sql;
create table fac_clarity as
	select a.*,b.Y as latitude1,b.X as longitude1 from fac_clar a left join zee b on input(a.zip1,9.)=b.zip;
/*select a.*,b.* from fac1_clar a left join zee b on a.zip1=b.zip;*/
	quit;
proc sort data=fac_clarity  nodupkey out=reevtemp.fac_clar1;by fac_code1;
run;

	proc sql;
	create table fac_med as 
	select distinct medctr1 from fac_clar1;
	quit;

	data serviceareas_clarity;
		format medctr1 $254. service_area $60. ;
		infile datalines delimiter='|';

		input  medctr1 $ service_area $;
	cards;
ANTELOPE VALLEY MEDICAL CENTER AREA|ANTELOPE VALLEY
BALDWIN PARK MEDICAL CENTER AREA|BALDWIN PARK
COACHELLA VALLEY CENTER|COACHELLA VALLEY
DOWNEY MEDICAL CENTER AREA|DOWNEY
FONTANA MEDICAL CENTER AREA|FONTANA
KERN COUNTY MEDICAL CENTER AREA|KERN COUNTY
LOS ANGELES MEDICAL CENTER AREA|LOS ANGELES
MENTAL HEALTH LA|LOS ANGELES
MORENO VALLEY MEDICAL CENTER|MORENO VALLEY
OC ANAHEIM MEDICAL CENTER|ANAHEIM
OC IRVINE MEDICAL CENTER|IRVINE
ONTARIO CENTER|ONTARIO
PANORAMA MEDICAL CENTER AREA|PANORAMA
RIVERSIDE MEDICAL CENTER AREA|RIVERSIDE
SAN DIEGO MEDICAL CENTER AREA|SAN DIEGO
SOUTH BAY MEDICAL CENTER AREA|SOUTH BAY
WEST LA MEDICAL CENTER AREA|WEST LOS ANGELES
WEST VENTURA CENTER|WEST VENTURA
WOODLAND HILLS MEDICAL CENTER AREA|WOODLAND HILLS
;
run;


proc sql;
create table fac4 as select a.*,b.service_area as kpsc_service_area from fac_clar1 a left join
 serviceareas_clarity b on a.medctr1=b.medctr1;
 quit;

/
/*facility from ocps(claims) hospitals with (updated)provider id*/

%include "/home/o222069/login.txt";
	proc sql noerrorstop;

		connect to teradata (tdpid=tdp2 user=&user password=&pwd connection=global mode=TERADATA);  

			execute (drop table DL_RES_USER.hsp_claim  		) by teradata;
			
			execute 
			(create multiset table DL_RES_USER.hsp_claim, no fallback as 

				(
					select 
						 a.prov_name
						,a.prov_id
						,count(distinct c.util_id) as records 
					from dl_res_svc.util_prov_master a
					inner join dl_res_svc.util_prov b
						on a.prov_id=b.prov_id
					left join dl_res_svc.util_main c
						on b.util_id=c.util_id
					where c.datasource='OCPS' and  a.prov_type='HOSP' 
					group by 
						 a.prov_name, a.prov_id
						 having records > 4999
						 

				)
			with data primary index(prov_id);
			) by teradata;
		disconnect from teradata;
	quit;
/*address_facility_type=U, updated in the next below code	*/
d
/*zip codes for states*/
data fac_ocps;
set fac_ocps;
kpsc_data_source='OCPS';
address_facility_type1='U';
run;

proc sql;
create table zee as select * from sashelp.zipcode where statecode in(select distinct state1 from fac_ocps);
quit; 

proc sql;
create table fac_ocps1 as
	select a.*,b.Y as latitude1,b.X as longitude1 from fac_ocps a left join zee b on input(a.zip1,9.)=b.zip;
/*select a.*,b.* from fac1_clar a left join zee b on a.zip1=b.zip;*/
	quit;
proc sort data=fac_ocps1  nodupkey out=reevtemp.fac_ocps1;by fac_code1;
run;

/*mapping table for provder facility*/



/*OCPS Dialysis Facilities with records > 50000*/

%include "/home/o222069/login.txt";
	proc sql noerrorstop;

		connect to teradata (tdpid=tdp2 user=&user password=&pwd connection=global mode=TERADATA);  

/*			execute (drop table DL_RES_USER.fac_ocps_dial  		) by teradata;*/
			execute 
			(create multiset table DL_RES_USER.fac_ocps_dial, no fallback as 

				(
						select 
						 a.prov_name
						,a.prov_id
						,count(distinct c.util_id) as records,c.care_subtype,a.prov_type 
					from dl_res_svc.util_prov_master a
					inner join dl_res_svc.util_prov b
						on a.prov_id=b.prov_id
					left join dl_res_svc.util_main c
						on b.util_id=c.util_id
					where c.datasource='OCPS'  and c.care_subtype='DIALYSIS'
					group by c.care_subtype,
						 a.prov_name, a.prov_id,a.prov_type
						 having records > 4999

				)

			with data ;
			) by teradata;
		disconnect from teradata;
	quit;

	data fac_ocps_dial;
	set dluser.fac_ocps_dial;
	run;

	/*address_facility_type=U, updated in the next below code	*/




data ocps_dial;
set ocps_dial;
kpsc_data_source='OCPS';
address_facility_type1='U';
run;


proc sql;
create table zee as select * from sashelp.zipcode where statecode in(select distinct state1 from ocps_dial);
quit; 

proc sql;
create table ocps_dial1 as
	select a.*,b.Y as latitude1,b.X as longitude1 from ocps_dial a left join zee b on input(a.zip1,9.)=b.zip;
/*select a.*,b.* from fac1_clar a left join zee b on a.zip1=b.zip;*/
	quit;
proc sort data=ocps_dial1  nodupkey out=reevtemp.ocps_dial1;by fac_code1;
run;

/*OCPS DIALYSIS - PROVIDER MAPPING*/


d


/* format statenames and vice versa*/
proc sql;
create table states as 
select distinct statecode,statename from sashelp.zipcode;
quit;

DATA WORK._EG_CFMT;
    LENGTH label $ 20;
    SET WORK.STATES (KEEP=STATECODE STATENAME RENAME=(STATECODE=start STATENAME=label)) END=__last;
    RETAIN fmtname "states" type "C";

    end=start;

    OUTPUT;
RUN;


PROC FORMAT LIBRARY=WORK CNTLIN=WORK._EG_CFMT;
RUN;

PROC SQL;
    DROP TABLE WORK._EG_CFMT;
QUIT;
DATA WORK._EG_CFMT;
    LENGTH label $ 12;
    SET states (KEEP=STATENAME STATECODE RENAME=(STATENAME=start STATECODE=label));
    RETAIN fmtname "st_abbr" type "C";

    end=start;
	output;
RUN;


PROC FORMAT LIBRARY=WORK CNTLIN=WORK._EG_CFMT;
RUN;

PROC SQL;
    DROP TABLE WORK._EG_CFMT;
QUIT;


data fac_clar;
set fac_clar1;
full_address1=upcase(full_address1);
street_address1=(UPCASE(street_address1));
city1=upcase(city1);
format state1 $states.;
run;

data facility_ocps;/*rename from fac_ocps*/
length kpsc_data_source $20;
set  reevtemp.fac_ocps1 reevtemp.ocps_dial1;
full_address1=upcase(full_address1);
street_address1=(UPCASE(street_address1));
city1=upcase(city1);
format state1 $states.;
run;

proc sql;
create table reevtemp.fac_clar as
select distinct kpsc_facility_name1 as FAC_NAME, medctr1 as kpsc_medctr,
          street_address1 as ADDRESS_LINE_1, 
          city1 as CITY, 
          state1 as STATE format=$states., 
          zip1 as ZIP,
			kpsc_data_source,
			fac_code1,
			relationship1 as relationship,
			relationship_history1 as relationship_history,
			address_facility_type1 as address_facility_type,
			latitude1 as latitude,
			longitude1 as longitude
		  from fac_clar
order by fac_name,ADDRESS_LINE_1;
quit;

proc sql;
create table reevtemp.fac_ocps as
select distinct kpsc_facility_name1 as FAC_NAME, medctr1 as kpsc_medctr,
          street_address1 as ADDRESS_LINE_1, 
          city1 as CITY, 
          state1 as STATE format=$states., 
          zip1 as ZIP,
			kpsc_data_source,
			fac_code1 ,
			relationship1 as relationship,
			relationship_history1 as relationship_history,
			address_facility_type1 as address_facility_type,
			latitude1 as latitude,
			longitude1 as longitude
		  from facility_ocps
order by fac_name,ADDRESS_LINE_1;
quit;

/

proc sql ;
create table claimsconnect_loc as
select  distinct  FAC_NAME, 
          ADDRESS_LINE_1,
		  ADDRESS_LINE_2,
          CITY, 
          ZIP, 
          STATE,FAC_ID from dlsvc.claimsconnect_loc
order by fac_name,ADDRESS_LINE_1;
quit;


data claimsconnect_loc;
length kpsc_data_source $20 kpsc_medctr $40;
set claimsconnect_loc;
kpsc_data_source='CLAIMSCONNECT';
relationship='O';
relationship_history='O';
address_Facility_type='U';
kpsc_medctr='NULL';
run;



/*joining data for duplicates*/

data fac1 dups1;
merge claimsconnect_loc(in=a) reevtemp.fac_clar(in=b);
by fac_name ADDRESS_LINE_1;
format state $states.;
if a=b then output fac1;
else output dups1;
run; 

data fac_cc(rename=(fac_code=fac_code1));/*drop=fac_code1*/
set fac1 dups1;
run;
proc sort data=fac_cc out=fac_cc1;by  fac_name ADDRESS_LINE_1;run;


data dup1_1(drop=fac_code fac_code2 ) dup1_2(rename=(fac_code2=fac_code1)drop=fac_code fac_code1) ;
set fac_cc1;
by  fac_name ADDRESS_LINE_1;
  retain fac_code 388;
  if fac_code1 ne ' ' then output dup1_1;
 if fac_code1 eq ' ' ;
if first.ADDRESS_LINE_1  then do;
  fac_code=fac_code+1;
end;
fac_code2=left(put(fac_code,$12.));

  output dup1_2; 
RUN;
/*fianl claims connect ----clarity table*/
data fac_cc;
set dup1_1 dup1_2;
kpsc_fac_num=input(fac_code1,12.);
run;
proc sort data=fac_cc out=reevtemp.fac_cc;by kpsc_fac_num;run;

/*select last number to continue with provider fac code*/
proc sql;
select max(kpsc_fac_num) into :max_fac_num from reevtemp.fac_cc;
quit;

%put _ALL_;
/*join facility id mapping tables and ncoa with fac_code1*/
PROC SQL;
   CREATE TABLE WORK.fac_cl_map AS 
   SELECT t1.fac_code1, 
          t1.fac_id, 
          t2.fac_ncoa
      FROM reevtemp.FAC_CC t1
           FULL JOIN reevtemp.CL_HOSP_MAP t2 ON (t1.fac_code1 = t2.fac_code1)
      ORDER BY t1.fac_code1;
QUIT;
/*fac ncoa table*/
proc sql;
create table reevtemp.fac_ncoa_map as select fac_code1 as facility_code,fac_ncoa from fac_cl_map where fac_ncoa is not null;
quit;
/*facid map table*/
proc sql;
create table reevtemp.fac_facid_map as select fac_code1 as facility_code,fac_id from fac_cl_map where fac_id is not null;
quit;

/*ocps provider data*/
proc sql ;
create table ocps_prov_upd as
select  distinct  PROVNAME as FAC_NAME, 
          PROVADDR1 AS ADDRESS_LINE_1, 
          PROVADDR2 AS ADDRESS_LINE_2, 
          PROVCITY AS CITY, 
          PROVSTATE AS STATE, 
          PROVZIP AS ZIP,
		  PROVID as prov_id
       from dlsvc.s_ocps_prov_upd
group  by PROVNAME,ADDRESS_LINE_1;
quit;

data ocps_prov_upd;
length kpsc_data_source $20 kpsc_medctr $40;
set ocps_prov_upd;
kpsc_data_source='CC_OCPS_PROV';
relationship='O';
relationship_history='O';
address_Facility_type='U';
kpsc_medctr='NULL';
run;

/* join ocps data for duplicates*/

data fac2 dup2;
merge ocps_prov_upd(in=a) reevtemp.fac_ocps(in=b);
by fac_name ADDRESS_LINE_1;
format state $states.;
if a=b then output fac2;
else output dup2;
run; 



DATA dup2_1(drop=fac_code fac_code2 ) dup2_2(rename=(fac_code2=fac_code1)drop=fac_code fac_code1);
  set dup2 ;
  by  fac_name ADDRESS_LINE_1;
  retain fac_code &max_fac_num;
  if fac_code1 ne ' ' then output dup2_1;
 if fac_code1 eq ' ' ;
if first.ADDRESS_LINE_1  then do;
  fac_code=fac_code+1;
end;
fac_code2=left(put(fac_code,$12.));

  output dup2_2; 
RUN;

/*final ocps provider table*/
data fac_ocps;
set fac2 dup2_1 dup2_2;
kpsc_fac_num=input(fac_code1,12.);
run;
proc sort data=fac_ocps out=reevtemp.fac_ocps;by kpsc_fac_num;run;



/*proc sort data=fac_ocps out=vdw.fac_ocps;by fac_code1;run;*/
proc sort data=fac2;by fac_code1;run;
proc sort data=dup2_1;by fac_code1;run;
proc sort data=dup2_2;by fac_code1;run;

/* join provid mapping tables*/
data prov_map;
merge reevtemp.ocps_hosp_map reevtemp.ocps_dial_map   ;
by fac_code1;
run;

PROC SQL;
   CREATE TABLE WORK.PROV_MAP1 AS 
   SELECT DISTINCT t1.prov_id, 
          t1.fac_code1
      FROM WORK.PROV_MAP t1
           FULL JOIN WORK.DUP2_1 t2 ON (t1.fac_code1 = t2.fac_code1)
		   
      ORDER BY t1.fac_code1;
QUIT;
/*final provider mapping table*/
data reevtemp.fac_prov_map(rename=(fac_code1=facility_code)) ;
set prov_map1
fac2(keep= fac_code1 prov_id)
dup2_2(keep= fac_code1 prov_id);
by fac_code1;
run;



/*final facility table*/
data final_fac(rename=(fac_code1 =facility_code zip=zip1)drop=fac_id prov_id latitude longitude);
/*length zip $5.;*/
set reevtemp.fac_cc  reevtemp.fac_ocps;
if length(zip) > 5 then zip2=put(zip,5.);
format state $st_abbr.;
run;


proc sql;
create table zee as select * from sashelp.zipcode where statecode in(select distinct state from final_fac);
quit; 

proc sql;
create table reevtemp.final_fac1(drop=zip1 zip2) as
	select a.*,b.Y as latitude,b.X as longitude,coalesce(a.zip2,a.zip1)as zip from final_fac a left join zee b on input(coalesce(a.zip2,a.zip1),9.)=b.zip;
/*select a.*,b.* from fac1_clar a left join zee b on a.zip1=b.zip;*/
	quit;

/*final facility table*/
proc sort data=reevtemp.final_fac1 nodupkey out=vdw_facility_final;by kpsc_fac_num;run;

option validvarname=upcase;
proc sql;
create table reevtemp.vdw_facility_final as
select distinct kpsc_data_source,KPSC_MEDCTR,FACILITY_CODE,RELATIONSHIP,RELATIONSHIP_HISTORY,
catx(', ',trim(ADDRESS_LINE_1),trim(ADDRESS_LINE_2),city,state,zip) as full_address,
catx(', ',trim(ADDRESS_LINE_1),trim(ADDRESS_LINE_2)) as street_address,
CITY,STATE,ZIP,ADDRESS_FACILITY_TYPE,LATITUDE,LONGITUDE
from 
vdw_facility_final;
quit;

proc freq data=reevtemp.vdw_facility_final;
table kpsc_medctr/norow nocol nocum nopercent;
run;
/*mapping lab data with loc master for fac_ncoa with loc_sid*/

proc copy in=vdw out=dluser;
select fac_ncoa_map;
run;

%include "/home/o222069/login.txt";
proc sql;
connect to teradata(tdpid=tdp2 user=&user password=&pwd mode=teradata);
execute (create multiset table dl_res_user.o222_loc_fac as 
	( select a.loc_sid,b.facility_code from dl_res_svc.util_loc_master a
		join
		dl_res_user.fac_ncoa_map b
		on a.fac_ncoa=b.fac_ncoa) with data;
		) by teradata;
quit;

proc copy in=dluser out=vdw;
select o222_loc_fac;
run;
proc sort data=vdw.o222_loc_fac; by facility_code;run;
