from pandas import DataFrame
from datetime import timedelta
from ni.config import ParameterValidator, Config
from mts.utils import hex_str
from moment import moment


EPOCH_DEFAULT = 1608480000
EPOCH_MOMENT = moment('2020-12-21')
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
DD_TYPE_SUB_METRIC = hex_str(5, 1)

DD_TYPES = [DD_TYPE_OWNER, DD_TYPE_METRIC, DD_TYPE_TAG, DD_TYPE_TAG_VALUE, DD_TYPE_SUB_METRIC]

FIELD_DDID = 'ddid'
FIELD_DESC = 'desc'
FIELD_OID_MASK = 'oid_mask'
FIELD_TIMESTAMP = 'timestamp'
FIELD_OWNER = 'owner'

DD_HEADERS = FIELD_DDID + ',' + FIELD_DESC + ',' + FIELD_OID_MASK

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

FILE_EXT_BY_TABLE_TYPE = {
    TABLE_TYPE_DD: '.dd',
    TABLE_TYPE_SDU: '.sdu',
    TABLE_TYPE_TDU: '.tdu'
}

FIELDS_DD = {FIELD_DDID: 'VARCHAR(17) PRIMARY KEY', FIELD_DESC: 'VARCHAR(160)', FIELD_OID_MASK: 'VARCHAR(32)'}

CACHE_TTL_DEFAULT = timedelta(hours=12)
CACHE_MAX_SIZE_DEFAULT = 30

MOMENT_FORMAT = 'X.SSS'

KEY_OID = 'oid'
KEY_DDID = 'ddid'
KEY_SERVICE_ID = 'service_id'
KEY_SERVICE_CODE = 'service_code'
KEY_DD_TYPE = 'dd_type'
KEY_METRIC = 'metric'

OID_LEN = 16
DDID_LEN = 17

OID = {
    'type': 'string',
    'pattern': '[a-f0-9]{16}'
}

DDID = {
    'type': 'string',
    'pattern': '[a-f0-9]{17}'
}

DESC = {
    'type': 'string',
    'maxLength': 160
}

DD_TYPE = {
    'type': 'string',
    'pattern': '[0-9a-f]{1}'
}

PV_ID = ParameterValidator({
    'oid': OID,
    'ddid': DDID
})

SERVICE_CODE_MIN = int('101000', 2)
SERVICE_CODE_MAX = int('111111', 2)

SERVICE_ID = {
    'type': 'string',
    'pattern': '[0-7]{2}'
}

PV_SERVICE = ParameterValidator({
    'service_code': {
        'type': 'integer',
        'minimum': SERVICE_CODE_MIN,
        'maximum': SERVICE_CODE_MAX
    },
    'service_id': SERVICE_ID
})

PV_DB_DEFINITION = ParameterValidator({
    'field': {
        'type': 'object',
        'minProperties': 1
    }
})

PV_DD_QUERY = ParameterValidator({
    'dd_type': DD_TYPE,
    'desc': {
        'type': 'array',
        'items': DESC,
        'minItems': 1
    },
    'oid': OID,
    'ddid': {
        'type': 'array',
        'items': DDID,
        'minItems': 1
    }
})

PV_DD_ADD = ParameterValidator({
    'dd_type': DD_TYPE,
    'desc': DESC,
    'oid_mask': {
        'type': 'string',
        'pattern': '[a-f0-9]{0,32}'
    }
})

PV_DD_APPEND = ParameterValidator({
    'service_id': SERVICE_ID,
    'dd_type': DD_TYPE,
    'desc': DESC,
    'oid_mask': {
        'type': 'string',
        'pattern': '[a-f0-9]{0,32}'
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
    'any': {
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
    'ts': {'type': 'string'},
    'data': {
        'type': 'object',
        'propertyNames': {'type': 'string'},
        'patternProperties': {
            '': {'type': 'number'}
        }
    }
})

PV_SDU_ADD = ParameterValidator({
    'owner': {'type': 'string'},
    'data': {
        'type': 'object',
        'propertyNames': {'type': 'string'},
        'patternProperties': {
            '': {
                'type': 'array',
                'items': {
                    'type': 'string',
                    'minItems': 1
                }
            }
        }
    }
})

DEFAULT_TZ = '08:00:00'

PV_TZ = ParameterValidator({
    'tz': {
        'type': 'string',
        'pattern': '^(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$'
    }
})

EMPTY = DataFrame.empty

PV_SDU_DATA = ParameterValidator({
    'tag': {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': {
                'name': {'type': 'string'},
                'values': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'desc': {'type': 'string'},
                            'owners': {
                                'type': 'array',
                                'items': {'type': 'string'}
                            }
                        }
                    },
                    'minItems': 1
                }
            }
        },
        'minItems': 1
    }
})
