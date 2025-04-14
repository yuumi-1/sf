import unittest
from processors.bank_parser import CQRCBParser


class TestCQRCBParser(unittest.TestCase):
    def setUp(self):
        self.parser = CQRCBParser()
        with open('tests/sample_cqrcb.txt', 'r', encoding='utf-8') as f:
            self.sample_text = f.read()

    def test_parse_reg_code(self):
        reg_code = self.parser.parse_product_info(self.sample_text)[0]['reg_code']
        self.assertEqual(reg_code, "C123456789")

    def test_parse_product_name(self):
        prd_name = self.parser.parse_product_info(self.sample_text)[0]['prd_name']
        self.assertEqual(prd_name, "渝农商理财鑫悦系列2023年第1期")

    # 添加更多测试用例...


if __name__ == '__main__':
    unittest.main()