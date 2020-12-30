https://twitter.com/everylothartfd

An everylot bot for Hartford, CT. Join geometry from the parcels table
with property information from the address points table.

Notes:
* Use the https://github.com/jmcarp/everylotbot/tree/static-map branch
    of everylotbot to include static imagery from google maps.
* Both tables include duplicate rows by pin. To handle this, merge
    duplicate polygons in the parcels and concatenate duplicate
    addresses in the address points table.
