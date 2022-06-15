from pandas import DataFrame
from datetime import timedelta
from ni.config import ParameterValidator
from mts.commons import hex_str
from moment import moment


EPOCH_DEFAULT = 1608480000
EPOCH_MOMENT = moment('2020-12-21')

BLANK = ''

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
DD_TYPE_METRIC_VALUE = hex_str(5, 1)

DD_TYPES = [DD_TYPE_OWNER, DD_TYPE_METRIC, DD_TYPE_TAG, DD_TYPE_TAG_VALUE, DD_TYPE_METRIC_VALUE]

FIELD_DDID = 'ddid'
FIELD_DESC = 'desc'
FIELD_OID_MASK = 'oid_mask'
FIELD_TIMESTAMP = 'timestamp'
FIELD_OWNER = 'owner'

FIELD_OID = 'oid'
FIELD_MASK = 'mask'

# DD_HEADERS = FIELD_DDID + ',' + FIELD_DESC + ',' + FIELD_OID_MASK

TABLE_PREFIX_DD = 'dd'
TABLE_PREFIX_SDU = 'sdu'
TABLE_PREFIX_TDU = 'tdu'

FIELDS_DD = {FIELD_DDID: 'VARCHAR(17) PRIMARY KEY', FIELD_DESC: 'VARCHAR(160)', FIELD_OID_MASK: 'VARCHAR(32)'}

CACHE_TTL_DEFAULT = timedelta(hours=12)
CACHE_MAX_SIZE_DEFAULT = 30

MOMENT_FORMAT = 'X.SSS'

KEY_OID = 'oid'
KEY_DDID = 'ddid'
KEY_DESC = 'desc'
KEY_SERVICE_ID = 'service_id'
KEY_SERVICE_CODE = 'service_code'
KEY_DD_TYPE = 'dd_type'
KEY_METRIC = 'metric'
KEY_OID_MASK = 'oid_mask'
KEY_INTERVAL = 'interval'
KEY_ANY = 'any'  # any time in the list
KEY_FROM = 'from'
KEY_TO = 'to'
KEY_TS = 'ts'
KEY_MASK = 'mask'
KEY_OWNER = 'owner'
KEY_TAG = 'tag'
KEY_DATA = 'data'
KEY_DATA_DESC = 'data_desc'
KEY_OP = 'op'

OID_LEN = 16
DDID_LEN = 17

OID = {
    'type': 'string',
    'pattern': '^[a-f0-9]{16}'
}

DDID = {
    'type': 'string',
    'pattern': '^[a-f0-9]{17}'
}

DESC = {
    'type': 'string',
    'maxLength': 160
}

MASK = {
    'type': 'string',
    'pattern': '^[a-f0-9]{16}'
}

DD_TYPE = {
    'type': 'string',
    'pattern': '^[0-9a-f]{1}'
}

PV_ID = ParameterValidator({
    KEY_OID: OID,
    KEY_DDID: DDID
})

SERVICE_CODE_MIN = int('101000', 2)
SERVICE_CODE_MAX = int('111111', 2)

MASK_DEFAULT = '0000000000000000'
MASK_ENUM = 'ffffffffffffffff'

SERVICE_ID = {
    'type': 'string',
    'pattern': '[0-7]{2}'
}

PV_SERVICE = ParameterValidator({
    KEY_SERVICE_CODE: {
        'type': 'integer',
        'minimum': SERVICE_CODE_MIN,
        'maximum': SERVICE_CODE_MAX
    },
    KEY_SERVICE_ID: SERVICE_ID
})

PV_DB_DEFINITION = ParameterValidator({
    'field': {
        'type': 'object',
        'propertyNames': {
            'type': 'string',
            'pattern': '^[a-zA-Z]'
        },
        'patternProperties': {
            '': {'type': 'string'}
        },
        'minProperties': 1
    }
})

PV_DD_REMOVE = ParameterValidator({
    KEY_DDID: DDID
})

PV_DD_QUERY = ParameterValidator({
    KEY_DD_TYPE: DD_TYPE,
    KEY_DESC: {
        'type': 'array',
        'items': DESC,
        'minItems': 1
    },
    KEY_OID: OID,
    KEY_DDID: {
        'type': 'array',
        'items': DDID,
        'minItems': 1
    }
})

PV_DD_ADD = ParameterValidator({
    KEY_DD_TYPE: DD_TYPE,
    KEY_DESC: DESC,
    KEY_OID: OID,
    KEY_MASK: MASK
})

PV_TDU_QUERY = ParameterValidator({
    KEY_METRIC: {
        'type': 'array',
        'items': OID,
        'minItems': 1
    },
    KEY_DESC: {
        'type': 'array',
        'items': DESC,
        'minItems': 1
    },
    KEY_INTERVAL: {
        'type': 'object',
        'properties': {
            KEY_FROM: {'type': 'string'},
            KEY_TO: {'type': 'string'}
            }
    },
    KEY_ANY: {
        'type': 'array',
        'items': {'type': 'string'},
        'minItems': 1
    }
})

PV_TDU_ADD = ParameterValidator({
    KEY_TS: {'type': 'string'},
    KEY_DATA_DESC: {
        'type': 'object',
        'propertyNames': {'type': 'string'},
        'patternProperties': {
            '': {'type': 'number'}
        }
    },
    KEY_DATA: {
        'type': 'object',
        'propertyNames': OID,
        'patternProperties': {
            '': {'type': 'number'}
        }
    }
})

PV_TDU_REMOVE = ParameterValidator({
    KEY_TS: {
        'type': 'object',
        'properties': {
            KEY_FROM: {'type': 'string'},
            KEY_TO: {'type': 'string'}
            }
    }
})

PV_SDU_QUERY = ParameterValidator({
    KEY_OWNER: {
        'type': 'object',
        'properties': {
            KEY_OP: {
                'type': 'string',
                'pattern': '(and)|(or)'
            },
            KEY_DATA: {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'propertyNames': {
                        'type': 'string',
                        'pattern': '(eq)|(ne)'
                    },
                    'patternProperties': {
                        '': OID
                    }
                },
                'minItems': 1,
            },
            KEY_DATA_DESC: {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'propertyNames': {
                        'type': 'string',
                        'pattern': '(eq)|(ne)'
                    },
                    'patternProperties': {
                        '': {'type': 'string'}
                    }
                },
                'minItems': 1,
            }
        }
    },
    KEY_TAG: {
        'type': 'object',
        'properties': {
            KEY_OP: {
                'type': 'string',
                'pattern': '(and)|(or)'
            },
            KEY_DATA: {
                'type': 'object',
                'properties': {
                    'propertyNames': OID,
                    'patternProperties': {
                        '': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'propertyNames': {
                                    'type': 'string',
                                    'pattern': '(eq)|(ne)'
                                },
                                'patternProperties': {
                                    '': {'type': 'number'}
                                }
                            },
                            'minItems': 1,
                        }
                    }
                },
                'minProperties': 1
            },
            KEY_DATA_DESC: {
                'type': 'object',
                'properties': {
                    'propertyNames': {'type': 'string'},
                    'patternProperties': {
                        '': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'propertyNames': {
                                    'type': 'string',
                                    'pattern': '(eq)|(ne)'
                                },
                                'patternProperties': {
                                    '': {'type': 'string'}
                                }
                            },
                            'minItems': 1,
                        }
                    }
                },
                'minProperties': 1
            }
        },
        'minProperties': 1
    }
})

PV_SDU_ADD = ParameterValidator({
    KEY_OWNER: OID,
    'data': {
        'type': 'object',
        'propertyNames': OID,
        'patternProperties': {
            '': {'type': 'number'}
        },
        'minProperties': 1
    }
})

DEFAULT_TZ = '08:00'

PV_TZ = ParameterValidator({
    'timezone': {
        'type': 'string',
        'pattern': '^[+-]?((2[0-3])|([0-1]?[0-9])):[0-5][0-9]$'
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
