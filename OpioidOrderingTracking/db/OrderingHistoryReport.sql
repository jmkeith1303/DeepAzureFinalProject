select oh.zip_code as 'ZipCode', zc.place_name as 'PlaceName', zc.state_name,  oh.state_code as 'StateAbbreviation', 
zc.county, zc.latitude as 'Latitude', zc.longitude as 'Longitude', sum(oh.ordered_quantity) as 'unitsordered'
from opiod_ordering_tracking.ordering_history oh 
inner join opiod_ordering_tracking.zipcodes zc on zc.zip_code = oh.zip_code
where (oh.ordered_ndc_is_opiod = 'Y' or oh.shipped_ndc_is_opiod = 'Y')
group by oh.zip_code, zc.place_name, zc.state_name,  oh.state_code, zc.county, zc.latitude, zc.longitude
order by sum(oh.ordered_quantity) desc;