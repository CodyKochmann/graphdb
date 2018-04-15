import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

'''
# this code is for later to test if lambdas/functions/classes
# maintain functionality after serialization
{
    'complex':(lambda i=5:i*2),
    'dict':['hi',6,None,99.0]
}
lambda i:'hello {}'.format(i),
'''

if __name__ == '__main__':
    unittest.main()
