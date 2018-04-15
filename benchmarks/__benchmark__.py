''' this script is used to run benchmarks on GraphDB '''

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=0).run(
            unittest.findTestCases(sys.modules[__name__])
    )
