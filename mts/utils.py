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
    '2000': '[{0}] 调用TimeDataUnit的query时，include参数不符合规范。',
    '5000': '[{0}] 非法 id；将自动生成一个新的 Object ID。',
    '5500': '[{0}] service_code值"{1}"异常，取值范围应为[{2}, {3}]。',
    '5501': '[{0}] oid的类型应为str或ObjectId，而非{1}。',
    '5502': '[{0}] 非法id；时间逆流。',
    '5503': '[{0}] '
}

logger = Logger(ERROR_DEF, 'mts')
