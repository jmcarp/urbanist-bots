download-parcels:
	arcgis-hub fetch-datasets-by-id --dataset-id bc72d0590bf940ff952ab113f10a36a8_8 --format csv

sqlite-parcels:
	sqlite3 everylot.db < load-parcels.sql
	
