from .base_parser import BaseBankParser
from .bank_parser import CQRCBParser, ABCParser

class BankParserFactory:
    """银行解析器工厂"""

    _parsers = {
        "CQRCB": CQRCBParser,
        "ABC": ABCParser,
        # 未来可以添加其他银行...
    }

    @classmethod
    def get_parser(cls, bank_identifier: str) -> BaseBankParser:
        """获取指定银行的解析器实例"""
        parser_class = cls._parsers.get(bank_identifier.upper())
        if not parser_class:
            raise ValueError(f"Unsupported bank: {bank_identifier}")
        return parser_class()

    @classmethod
    def detect_bank(cls, text: str) -> str:
        """通过文本内容自动检测银行类型"""

        if any(bank in text for bank in ["中国农业银行","农银理财","ewealth"]):
            return "ABC"

        else:
            return "CQRCB"


        # 可以添加其他银行的检测逻辑...

        return "UNKNOWN"