set mapreduce.job.queuename=root.NONP.HAASAAP0063;
use HAASAAP0063; 
ALTER TABLE HAASAAP0063.VW_EDW_VOSP_REP_CONTENTPRODUCT ADD IF NOT EXISTS 
PARTITION (part_date="${PARTITION_NAME}")  LOCATION "/user/HAASAAP0063/landing/staging/TVRoyalty/CONTENTPRODUCT/${PARTITION_NAME}";

insert  overwrite table HAASAAP0063.VW_EDW_VOSP_REP_CONTENTPRODUCT  PARTITION (part_date="${PARTITION_NAME}")
select
ProductID,
ProductGUID,
ClientAssetID,
AssetTitleID,
VOSP1ProductID,
ContractID,
EditorialVersionID,
StructureType,
ProductType,
ProductOfferringType,
SchedulingID,
ProductPlaylistType,
StorefrontProductID,
ValidForStorefront,
ParentGUID,
ProductAdded_ts,
productupdated_ts,
OfferStart_ts,
OfferEnd_ts,
MasterAgrementStart_ts,
MasterAgrementEnd_ts,
ContentProviderID,
RoyaltyReportingReference,
LinkedAssetTitleID,
ProductOfferings,
ProductDuration,
RentalPeriod,
Subscriptions,
ScopeIDs,
AudioLanguage,
AudioDescription,
CollectionBundleCount,
BundleProductIDs,
PricingTemplate,
InhibitProduct ,
SeriesNumber,
EpisodeNumber,
Amounts,
ProductServiceType,
TargetBandwidth,
Genre,
SchedulerChannel,
Rating,
RatingAuthority,
ProductTitle,
TitleDefault,
TitleSearch,
BBJ_TitleName,
BBJ_SeriesTitle,
BBJ_ExternalID,
BBJ_RoyaltyCategory,
BBJ_RoyaltyContract,
BBJ_Licensor,
BBJ_FirstBroadcaster,
BBJ_EIDR,
BBJ_Artist,
BBJ_ISAN,
BBJ_ISRC,
BBJ_MCPS_PRS_PPL,
BBJ_MSC,
BBJ_Promo_List,
BBJ_ServiceList,
BBJ_ViewFormat,
BBJ_ChannelName,
BBJ_Added_ts,
BBJ_Updated_ts,
BBJ_Type,
BBJ_PlaylistType,
DEALS_ConsolidatedInfo,
PLAYLIST_SchedulingID,
PLAYLIST_ContentProviderID,
PLAYLIST_NumberOfChildren,
PLAYLIST_ChildrenList,
PLAYLIST_Duration,
PLAYLIST_Added_ts,
PLAYLIST_Updated_ts,
RoyaltyReportingTitle,
BBJ_PromoType,
SubGenres,
Grade_Flag,
Grade_Category,
MG_Flag,
EpisodeTitle,
MediaAssetID,
DM_Report_ts,
HaaS_Created_ts
from
HAASAAP0292_10082.VW_EDW_VOSP_REP_CONTENTPRODUCT  where  partition_date=DATE_ADD( "${PARTITION_NAME}" , -2); 

drop table    IF EXISTS W_STG_MAX_CP;
create table W_STG_MAX_CP as select max(ExtractRunNumber) AS MAXRUN, from_unixtime(unix_timestamp(),'dd/MM/yyyy') AS edw_date from TV_Royalty_run_control_CP;
insert into TV_Royalty_run_control_CP
SELECT 
b.MAXRUN+1 as maxrun,
from_unixtime(unix_timestamp(),'dd/MM/yyyy') as ExtractStartTimestamp,
'VW_EDW_VOSP_REP_CONTENTPRODUCT' as AVALONHIVEViewName,
'5'  as InterfaceVersion ,
'APP07770' as SourceSystem,
'APP07770' as OriginatingSystem,
current_timestamp()  as ExtractRangeStartDate,
from_unixtime(unix_timestamp(),'dd/MM/yyyy') as ExtractRangeEndDate,
aa.rec_count,
null as TotalAmount
from
(select 
count(A.ProductGUID) as rec_count,
A.part_date
from  VW_EDW_VOSP_REP_CONTENTPRODUCT A inner join W_STG_MAX_CP  b on
A.part_date=cast(b.edw_date as string)
group by A.part_date)aa,
 W_STG_MAX_CP  b
where aa.part_date=cast(b.edw_date as string);