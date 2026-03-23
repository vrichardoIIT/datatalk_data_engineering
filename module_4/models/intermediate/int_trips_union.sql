-- Don't need for me, but here is a example for ref to the staging model
--only use source when you want to read directly from the raw data, otherwise use ref to read from the staging model
{# with yellow_trips as (
    select *
 from {{ ref('stg_yellow_taxidata') }}
 where vendorid is not null
) #}