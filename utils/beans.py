from collections import namedtuple

LocDataStr = namedtuple("LocDataStr", ["loc_s", "data_s"])
LocData = namedtuple("LocData", ["loc", "data"])
SendData = namedtuple("SendData", ["timestamp", "data"])
Loc = namedtuple("Loc", ["timestamp", "loc"])

ExceedInfo = namedtuple("ExceedInfo", ["exceed", "value"])
SignValueMap = namedtuple("SignValueMap" , ["sign_name", "value_map", "exceed"])

Detail = namedtuple("Detail", ["sign_name", "key", "value"])
LocAndSignDetails = namedtuple("LocAndSignDetails", ["loc", "details"])


class FailReasons(object):
    FailNumNotEq = "FailNumNotEq"
    FailDataNotMatch = "FailDataNotMatch"