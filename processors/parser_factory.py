from .base_parser import BaseBankParser
from .bank_parser import CQRCBParser, ABCParser, ICBCParser


class BankParserFactory:
    """银行解析器工厂"""

    _parsers = {
        "CQRCB": CQRCBParser,
        "ABC": ABCParser,
        "ICBC": ICBCParser
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

        if any(bank in text for bank in ["渝农商","重庆农村商业银行","汇华理财","光大银行"]):
            return "CQRCB"

        if "中国农业银行" in text:
            return "ABC"

        if "工商银行" in text:
            return "ICBC"


        # 可以添加其他银行的检测逻辑...

        return "UNKNOWN"