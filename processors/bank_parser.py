import re
from typing import Dict, Tuple, Optional, Any, Union
import datetime
from .base_parser import BaseBankParser


class CQRCBParser(BaseBankParser):
    """重庆农商行(CQRCB)产品公告解析器"""

class ICBCParser(BaseBankParser):
    """工商银行产品公告解析器"""

class ABCParser(BaseBankParser):
    """中国农业银行(ABC)产品公告解析器"""


