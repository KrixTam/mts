from pandas import DataFrame
from datetime import timedelta
from ni.config import ParameterValidator
from mts.utils import hex_str


EPOCH_DEFAULT = 1608480000
DB_MODE_CX = 0  # connectX
DB_MODE_SD = 1  # sqlite

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

MOMENT_FORMAT = 'X.SSS'

KEY_OID = 'oid'
KEY_SERVICE_ID = 'service_id'

FIELD_DDID = 'ddid'
FIELD_TIMESTAMP = 'timestamp'

OID_LEN = 16
DDID_LEN = 17

OID = {
    'type': 'string',
    'pattern': '[a-f0-9]{16}'
}

PV_ID = ParameterValidator({
    'oid': OID,
    'ddid': {
        'type': 'string',
        'pattern': '[a-f0-9]{17}'
    }
})

SERVICE_CODE_MIN = int('101000', 2)
SERVICE_CODE_MAX = int('111111', 2)

PV_SERVICE = ParameterValidator({
    'service_code': {
        'type': 'integer',
        'minimum': SERVICE_CODE_MIN,
        'maximum': SERVICE_CODE_MAX
    },
    'service_id': {
        'type': 'string',
        'pattern': '[0-7]{2}',
    }
})

PV_DB_DEFINITION = ParameterValidator({
    'field': {
        'type': 'object',
        'minProperties': 1
    }
})

PV_TDU_QUERY = ParameterValidator({
    'metric': {
        'type': 'array',
        'items': OID
    },
    'interval': {
        'type': 'object',
        'properties': {
            'from': {'type': 'string'},
            'to': {'type': 'string'}
            }
    },
    'in': {
        'type': 'array',
        'items': {'type': 'string'},
        'minItems': 1
    }
})

PV_SDU_QUERY = ParameterValidator({
    'include': {
        'type': 'object',
        'properties': {
            'owner': {
                'type': 'array',
                'items': OID
            },
            'metric': {
                'type': 'array',
                'items': OID
            }
        }
    },
    'exclude': {
        'type': 'object',
        'properties': {
            'owner': {
                'type': 'array',
                'items': OID
            },
            'metric': {
                'type': 'array',
                'items': OID
            }
        }
    }
})

PV_TDU_ADD = ParameterValidator({
    'records': {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': {
                'ts': {'type': 'string'},
                'data': {'type': 'object'}
            }
        }
    }
})

EMPTY = DataFrame.empty
