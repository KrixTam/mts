from ni.config.tools import Logger
from moment import moment


def hex_str(num, bits):
    length = abs(bits)
    res = "{0:0{1}x}".format(num, length)
    if bits > 0:
        return res[:bits]
    else:
        return res[bits:]

#
# def get_timestamp(ori_data: str):
#     m = moment(ori_data).format('x')

# 1000-1999：DataUnitProcessor
# 2000-2499：TimeDataUnit
# 2500-3999：SpaceDataUnit
# 4000-4499：TDUProcessor
# 4500-4999：DataUnit
# 5000-5499：DataUnitService
# 5500-5599：ObjectId
# 5600-5699：DataDictionaryId
# 5700-5799：DBHandler
# 5800-5899：DataDictionary


ERROR_DEF = {
    '1000': '[{0}] 不存在数据单元服务：[{1}]，未能进行初始化处理。',
    '1001': '[{0}] 参数ds的类型应为DataService或dict，而非{1}。',
    '2000': '[{0}] 调用TimeDataUnit的query方法，其参数不符合规范。',
    '2001': '[{0}] 调用TimeDataUnit的add方法，其参数不符合规范。',
    '2002': '[{0}] 构建TimeDataUnit失败，owner_id值异常。',
    '2003': '[{0}] TimeDataUnit对应的数据库表不存在，或者DB异常，导致query失败。',
    '4500': '[{0}] 构建DataUnit失败，service_id值异常。',
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
    '5703': '[{0}] init_table参数fields值异常',
    '5704': '[{0}] get_table_name的参数service_id值异常。',
    # '5705': '[{0}] get_dd的参数service_id值异常。',
    '5706': '[{0}] get_table_name的参数owner_id值异常。',
    # '5800': '[{0}] 构建DataDictionary失败，service_id值异常。',
    '5801': '[{0}] 异常：未能识别的dd_type({1})。',
    '5802': '[{0}] query_dd的参数service_id值异常。',
}

logger = Logger(ERROR_DEF, 'mts')
