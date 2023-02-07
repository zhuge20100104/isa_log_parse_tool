import re

line = 'RawGPS(latLon=LatLon(lat=41.3890001, lon=2.1563527), elapsedTime=15738188, mm=MMGPS(latLon=LatLon(lat=41.3889967, lon=2.156348), adasPosition=AdasPosition(positionMessage=2309760826875068646, canMsg=CanMsg(data=[10, 2, 2, 0, 8191, 1, 0, 0, 2, 197, 1, 1], elapsedTime=15739200)))) | process time = 1012ms'

line_g = re.match(".*mm=(.*)\|.*", line)
mm_gps_msg = line_g.group(1)

# MMGPS(latLon=LatLon(lat=41.3889967, lon=2.156348), adasPosition=AdasPosition(positionMessage=2309760826875068646,
#  canMsg=CanMsg(data=[10, 2, 2, 0, 8191, 1, 0, 0, 2, 197, 1, 1], elapsedTime=15739200))))
lat_lon_g = re.match(".*lat=(.*)\ lon=(.*)\).*positionMessage=(.*)\,.*data=\[(.*)\].*", mm_gps_msg)
lat = lat_lon_g.group(1)
lon = lat_lon_g.group(2)
pos_msg = lat_lon_g.group(3)
data = lat_lon_g.group(4)
print(lat, lon)
print(pos_msg)
print(data)
