.mode csv

.import 'Real Estate (Base Data).csv' parcels_staging

create table parcels as
with deduped as (
  select
    *,
    count() over (partition by gpin) as parcels,
    row_number() over (partition by gpin order by parcelnumber) as row_number
  from parcels_staging
)
select
  RecordID_Int,
  ParcelNumber,
  StreetNumber,
  StreetName,
  Unit,
  StateCode,
  TaxType,
  Zone,
  TaxDist,
  Legal,
  Acreage,
  GPIN,
  parcels,
  false as posted
from deduped
where row_number = 1;

create index ix_parcelnumber_posted on parcels (parcelnumber, posted);

drop table parcels_staging;

vacuum;
