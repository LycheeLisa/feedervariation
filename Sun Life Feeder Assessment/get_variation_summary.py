import pandas as pd
import pandasql
from pandasql import sqldf
import os
from os import sys

def get_variation_summary(inputfile,outputfile):
 pysqldf = lambda q:sqldf(q,globals())

 #inputfile=sys.argv[1]
#outputfile = sys.argv[2]

 df = pd.read_csv(inputfile)
 print (df.head())

 q="""select q.count_pr_key as total_number_of_feeds,
		f.typename as feed_type_name,
		f.FinancialReportingRequirements,
		case when f.cnt_dtp>1 then (q.count_pr_key-f.cnt_dtp) else q.count_pr_key end as number_of_identical_feeds,
		case when f.cnt_dtp =1 then cast('0.00' as float) else f.cnt_dtp end as number_of_varied_feeds,
		--(cast(f.cnt_dtp as float)/(cast f.cnt_prkey as float)) *100 as variation_percentage,
		case when f.cnt_dtp>1 then (cast(f.cnt_dtp as float)/cast(q.count_pr_key as float))*100 else cast('0.00' as float) end as variation_percentage,
		case when f.cnt_dtp>1 then ((cast(q.count_pr_key as float)-cast(f.cnt_dtp as float))/cast(q.count_pr_key as float))*100 else cast('100.00' as float) end as identical_percentage
		--100-(((cast(f.cnt_prkey as float)-cast(f.cnt_dtp as float))/cast(f.cnt_prkey as float))*100) as variation_percentage
		from
	(select
		--case when a.BUProposedStrategytoMeetRequirements ='Derived from feed attributes' then
				count(distinct (coalesce(lower(a.DataType),'NA')||
					coalesce(lower(a.IfDatewhatislayout),'NA')||
					coalesce(a.Datalength,'NA')||
					coalesce(lower(a.Nullable),'NA')||
					coalesce(lower(a.Concatenated),'NA')||
					coalesce(lower(a.BUProposedStrategytoMeetRequirements),'NA') ||
			 		coalesce(lower(a.Attribute1_DataType),'NA')||
					coalesce(lower(a.Attribute1_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute1_DataLength,'NA')||
					coalesce(lower(a.Attribute1_Nullable),'NA')||
					coalesce(lower(a.Attribute1_Concatenated),'NA')||
					coalesce(lower(a.Attribute1_StrategytoObtainAttribute),'NA')||
					coalesce(lower(a.Attribute2_DataType),'NA')||
					coalesce(lower(a.Attribute2_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute2_DataLength,'NA')||
					coalesce(lower(a.Attribute2_Nullable),'NA')||
					coalesce(lower(a.Attribute2_Concatenated),'NA') ||
					coalesce(lower(a.Attribute3_DataType),'NA')||
					coalesce(lower(a.Attribute3_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute3_DataLength,'NA')||
					coalesce(lower(a.Attribute3_Nullable),'NA')||
					coalesce(lower(a.Attribute3_Concatenated),'NA')||
					coalesce(lower(a.Attribute4_DataType),'NA')||
					coalesce(lower(a.Attribute4_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute4_DataLength,'NA')||
					coalesce(lower(a.Attribute4_Nullable),'NA')||
					coalesce(lower(a.Attribute4_Concatenated),'NA') ||
					coalesce(lower(a.Attribute5_DataType),'NA')||
					coalesce(lower(a.Attribute5_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute5_DataLength,'NA')||
					coalesce(lower(a.Attribute5_Nullable),'NA')||
					coalesce(lower(a.Attribute5_Concatenated),'NA')||
					coalesce(lower(a.Attribute6_DataType),'NA')||
					coalesce(lower(a.Attribute6_IfDatewhatisLayout),'NA')||
					coalesce(a.Attribute6_DataLength,'NA')||
					coalesce(lower(a.Attribute6_Nullable),'NA')||
					coalesce(lower(a.Attribute6_Concatenated),'NA'))) as cnt_dtp,
		count(distinct a.PRKey) as cnt_prkey,
		a.typename,
		lower(a.FinancialReportingRequirements) as FinancialReportingRequirements,
		a.reqcategory
		 from  df a,df b
			where lower(a.TypeName)=lower(b.TypeName)
				and a.Reqcategory=b.Reqcategory and
				lower(a.FinancialReportingRequirements)=lower(b.FinancialReportingRequirements)
				and a.PRKey!=b.PRKey
			group by a.typename,lower(a.FinancialReportingRequirements)) as f	,
			(select count(distinct(prkey)) as count_pr_key ,typename,reqcategory from df group by typename,reqcategory)
            from q
			where f.typename=q.typename and f.reqcategory=q.reqcategory;"""

 variation_summary=pysqldf(q)

 variation_in_feeds_report.to_csv(outputfile,sep=',',index=False,header=True,float_format='%.3f',decimal='.',encoding='utf-8')

get_variation_summary(sys.argv[1],sys.argv[2])
