from ni.config.tools import Logger

# 1000-1999：DataUnitProcessor
# 2000-2499：TimeDataUnit
# 2500-3999：SpaceDataUnit
# 4000-4499：TDUProcessor
# 4500-4999：DataUnit
# 5000-5499：DataUnitService
# 5500-5599：ObjectId
# 5600-5699：DataDictionaryId
# 5700-5999：DBConnector

ERROR_DEF = {
    '1000': '[{0}] 不存在数据单元服务：[{1}]，未能进行初始化处理。',
    '1001': '[{0}] 参数ds的类型应为DataService或dict，而非{1}。',
    '2000': '[{0}] 调用TimeDataUnit的query时，include参数不符合规范。',
    '5000': '[{0}] 非法id；将自动生成一个新的 Object ID。',
    '5001': '[{0}] DataUnitService配置参数错误。',
    '5002': '[{0}] 请检查DataUnitService配置参数service_id和ds_path，不能使用默认值进行初始化处理。',
    '5003': '[{0}] 文件类型为TDU文件，必须跟有owner信息，可以值为owner id的参数，或者一个存储着一系列owner id 的list。',
    '5004': '[{0}] 暂不支持该file_type({1})，并非预先设定的file_type范围值，获取文件名失败。',
    '5005': '[{0}] 参数headers的类型必须为包含","的字符串或字符串list。',
    '5006': '[{0}] 当参数content的类型为dict时，headers必须为字符串的list。',
    '5007': '[{0}] 参数content的类型必须为list或dict。',
    '5500': '[{0}] service_code值"{1}"异常，取值范围应为[{2}, {3}]。',
    '5501': '[{0}] oid的类型应为str或ObjectId，而非{1}。',
    '5502': '[{0}] 非法id；时间逆流。',
    '5600': '[{0}] ddid的类型应为str或DataDictionaryId，而非{1}。',
    '5601': '[{0}] 构建DataDictionaryId时，遇到异常的参数{1}。',
    '5602': '[{0}] ddid应为17位长度的字符串，或者是DataDictionaryId实例。',
    '5603': '[{0}] 异常：非法ddid。',
    '5700': '[{0}] DBConnector需要对db_url进行登记(register)后方能使用。',
    '5701': '[{0}] 参数table_type应为({1})。',
    '5702': '[{0}] 参数owner_id不能为None。',
    '5703': '[{0}] 异常：未能识别的dd_type({1})。',
}

logger = Logger(ERROR_DEF, 'mts')
