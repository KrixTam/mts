import os
import shutil
import coverage
import unittest


def run_all_tests(test_modules):
    suite = unittest.TestSuite()
    for t in test_modules:
        try:
            # If the module defines a suite() function, call it to get the suite.
            mod = __import__(t, globals(), locals(), ['suite'])
            suite_fn = getattr(mod, 'suite')
            suite.addTest(suite_fn())
        except (ImportError, AttributeError):
            # else, just load all the test cases from the module.
            suite.addTest(unittest.defaultTestLoader.loadTestsFromName(t))
    unittest.TextTestRunner().run(suite)


# 删除测试输出目录
dirs = ['log', 'output', 'htmlcov']
for d in dirs:
    if os.path.exists(d):
        shutil.rmtree(d)

test_modules = [
    'mts.test.stats.test_Jaccard',
    'mts.test.core.id.test_ObjectId',
    'mts.test.core.id.test_DataDictionaryId',
    'mts.test.core.id.test_Service',
    'mts.test.core.handler.test_DBHandler',
    'mts.test.core.handler.test_DataFileHandler',
    'mts.test.core.datamodel.test_DataDictionary',
    'mts.test.core.datamodel.test_Metrics',
    'mts.test.core.datamodel.test_Tags',
    # 'mts.test.test_DataUnitService',
    # 'mts.test.test_DataDictionary',
    # 'mts.test.test_SpaceDataUnit',
    # 'mts.test.test_TimeDataUnit'
]

# 执行测试用例
cov = coverage.Coverage()
cov.start()

run_all_tests(test_modules)

cov.stop()
cov.save()

cov.html_report(directory='htmlcov')