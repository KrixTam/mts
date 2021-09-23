# coding: utf-8

def hex_str(num, bits):
    length = abs(bits)
    res = "{0:0{1}x}".format(num, length)
    if bits > 0:
        return res[:bits]
    else:
        return res[bits:]


SERVICE_CODE_BITS = 6
TIMESTAMP_BITS = 42
PID_CODE_BITS = 4
SEQUENCE_BITS = 12

SERVICE_CODE_BITS_SHIFT = TIMESTAMP_BITS + PID_CODE_BITS + SEQUENCE_BITS
TIMESTAMP_BITS_SHIFT = PID_CODE_BITS + SEQUENCE_BITS
PID_CODE_BITS_SHIFT = SEQUENCE_BITS

SERVICE_CODE_MASK = -1 ^ (-1 << SERVICE_CODE_BITS)
TIMESTAMP_MASK = -1 ^ (-1 << TIMESTAMP_BITS)
PID_CODE_MASK = -1 ^ (-1 << PID_CODE_BITS)
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

DD_TYPE_BITS = 4
OID_BITS = 64

DD_TYPE_BITS_SHIFT = OID_BITS

DD_TYPE_MASK = -1 ^ (-1 << DD_TYPE_BITS)
OID_MASK = -1 ^ (-1 << OID_BITS)

DD_TYPE_OWNER = 'owner'
DD_TYPE_METRIC = 'metric'
DD_TYPE_TAG = 'tag'
DD_TYPE_TAG_VALUE = 'tag_value'

DD_TYPE = {
    DD_TYPE_OWNER: hex_str(1, 1),
    DD_TYPE_METRIC: hex_str(2, 1),
    DD_TYPE_TAG: hex_str(3, 1),
    DD_TYPE_TAG_VALUE: hex_str(4, 1)
}

DD_HEADERS = 'ddid,disc,oid_mask'

FILE_TYPE_DD = 'dd'
FILE_TYPE_SDU = 'sdu'
FILE_TYPE_TDU = 'tdu'

TABLE_TYPE_DD = 'dd'
TABLE_TYPE_SDU = 'sdu'
TABLE_TYPE_TDU = 'tdu'

TABLE_TYPE = [TABLE_TYPE_DD, TABLE_TYPE_SDU, TABLE_TYPE_TDU]

FILE_EXT = {
    FILE_TYPE_DD: '.dd',
    FILE_TYPE_SDU: '.sdu',
    FILE_TYPE_TDU: '.tdu'
}

FIELDS_DD = {'ddid': 'VARCHAR(17)', 'disc': 'VARCHAR(160)', 'oid_mask': 'VARCHAR(32)'}