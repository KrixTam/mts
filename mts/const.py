from pandas import DataFrame
from datetime import timedelta
from ni.config import ParameterValidator


def hex_str(num, bits):
    length = abs(bits)
    res = "{0:0{1}x}".format(num, length)
    if bits > 0:
        return res[:bits]
    else:
        return res[bits:]


EPOCH_DEFAULT = 1608480000
SERVICE_CODE_MIN = int('101000', 2)

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

DD_TYPE_OWNER = hex_str(1, 1)
DD_TYPE_METRIC = hex_str(2, 1)
DD_TYPE_TAG = hex_str(3, 1)
DD_TYPE_TAG_VALUE = hex_str(4, 1)

DD_TYPE = [DD_TYPE_OWNER, DD_TYPE_METRIC, DD_TYPE_TAG, DD_TYPE_TAG_VALUE]

DD_HEADERS = 'ddid,disc,oid_mask'

FILE_TYPE_DD = 'dd'
FILE_TYPE_SDU = 'sdu'
FILE_TYPE_TDU = 'tdu'
FILE_TYPE_TDU_RAW = 'rtdu'

TABLE_TYPE_DD = 'dd'
TABLE_TYPE_SDU = 'sdu'
TABLE_TYPE_TDU = 'tdu'

TABLE_TYPE = [TABLE_TYPE_DD, TABLE_TYPE_SDU, TABLE_TYPE_TDU]

FILE_EXT = {
    FILE_TYPE_DD: '.dd',
    FILE_TYPE_SDU: '.sdu',
    FILE_TYPE_TDU: '.tdu',
    FILE_TYPE_TDU_RAW: '.rtdu'
}

FIELDS_DD = {'ddid': 'VARCHAR(17)', 'disc': 'VARCHAR(160)', 'oid_mask': 'VARCHAR(32)'}

CACHE_TTL_DEFAULT = timedelta(hours=12)
CACHE_MAX_SIZE_DEFAULT = 30

QUERY_OID = {
    'type': 'string',
    'minLength': 16,
    'maxLength': 16
}

PV_TDU_QUERY_INCLUDE = ParameterValidator({
    'include': {
        'type': 'object',
        'properties': {
            'owner': {
                'type': 'array',
                'items': QUERY_OID
            },
            'metric': {
                'type': 'array',
                'items': QUERY_OID
            }
        }
    },
    'exclude': {
        'type': 'object',
        'properties': {
            'owner': {
                'type': 'array',
                'items': QUERY_OID
            },
            'metric': {
                'type': 'array',
                'items': QUERY_OID
            }
        }
    },
    'scope': {
        'type': 'object',
        'properties': {
            'from': {'type': 'string'},
            'to': {'type': 'string'},
            'in': {
                'type': 'array',
                'items': {'type': 'string'}
            }
        }
    }
})

EMPTY = DataFrame.empty
