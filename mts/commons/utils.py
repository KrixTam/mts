from ni.config.tools import Logger
from moment import moment


# def get_timestamp(ori_data: str):
#     m = moment(ori_data).format('x')


def hex_str(num, bits):
    length = abs(bits)
    res = "{0:0{1}x}".format(num, length)
    if bits > 0:
        return res[:bits]
    else:
        return res[bits:]


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
# 5900-5999：DataFileHandler
# 6000-6099：Service
# 6100-6199：similarity
# 6200-6299：DataFragments


ERROR_DEF = {
    '1000': '[{0}] 不存在数据单元服务：[{1}]，未能进行初始化处理。',
    '1001': '[{0}] 参数ds的类型应为DataService或dict，而非{1}。',
    # '2000': '[{0}] 调用TimeDataUnit的query方法，其参数不符合规范。',
    '2000': '[{0}] 构建TimeDataUnit失败，ddid值异常（非owner）。',
    '2001': '[{0}] 调用TimeDataUnit的add方法，其参数不符合规范。',
    '2002': '[{0}] 调用TimeDataUnit的remove方法，其参数不符合规范。',
    # '2002': '[{0}] 构建TimeDataUnit失败，owner_id值异常。',
    # '2003': '[{0}] TimeDataUnit对应的数据库表或相关字段不存在，或者DB异常，导致query失败。',
    # '2500': '[{0}] 待导入文件{1}内容不符合SDU数据规则要求，请检查后重新执行导入操作',
    '2500': '[{0}] 调用SpaceDataUnit的query方法，其参数不符合规范。',
    '2501': '[{0}] 调用SpaceDataUnit的add方法，缺少owner参数。',
    '2502': '[{0}] 调用SpaceDataUnit的add方法，其参数owner值<{1}>异常，不存在该取值。',
    '2503': '[{0}] 调用SpaceDataUnit的remove方法，其参数owner值<{1}>异常，不存在该取值。',
    '2504': '[{0}] 调用SpaceDataUnit的remove方法，缺少owner参数。',
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
    '5700': '[{0}] DBHandler需要对db_url进行登记(register)后方能使用。',
    '5701': '[{0}] 参数table_type应为({1})。',
    '5702': '[{0}] 参数owner_id不能为None。',
    '5703': '[{0}] init_table参数fields值异常',
    '5704': '[{0}] get_table_name的参数service_id值异常。',
    # '5705': '[{0}] export_data的参数table_type值异常，暂不支持该table_type({1})。',
    '5706': '[{0}] get_table_name的参数owner_id值异常。',
    # '5707': '[{0}] export_data的参数owner_id不能为None。',
    '5708': '[{0}] DBHandler初始化参数timezone值（"{1}"）异常。',
    '5800': '[{0}] query的参数dd_type值异常。',
    '5801': '[{0}] 异常：未能识别的dd_type({1})。',
    '5802': '[{0}] remove的参数ddid值异常。',
    '5803': '[{0}] query的参数desc值异常。',
    '5804': '[{0}] 新增数据{1}存在重复，无须进行重复插入操作',
    '5805': '[{0}] add参数{1}异常。',
    # '5806': '[{0}] append参数{1}异常。',
    '5807': '[{0}] query的参数ddid值异常。',
    '5808': '[{0}] query的参数oid值异常。',
    '5900': '[{0}] 成功读取文件"{1}"。',
    '5901': '[{0}] 找不到"{1}"。',
    '6000': '[{0}] to_service_code参数类型异常，应该为int或8进制的str。',
    '6001': '[{0}] Service Code值（{1}）异常，不在合理范围[{2}, {3}]内；系统保留Service的原值，不会对Service设置处理。',
    '6100': '[{0}] Jaccard相关方法的参数只能为set或者numpy.ndarray。',
    '6200': '[{0}] 构建DataFragments失败，service_id值异常。',
}

logger = Logger(ERROR_DEF, 'mts')
