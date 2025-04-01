#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

import sys
import os

package_root = os.path.dirname(os.path.abspath(__file__))
if package_root not in sys.path:
    sys.path.insert(0, package_root)
