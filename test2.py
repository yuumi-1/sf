import re

from main import PDFProcessor

if __name__ == "__main__":
    url='https://licai-report-oss-bucket.oss-cn-shenzhen.aliyuncs.com/Financial_announcement/icbc/工银理财·鑫尊享固定收益类封闭式理财产品（长城系列22GS3133）成立报告-2022-05-06.pdf'
    processor = PDFProcessor()
    text = processor.extract_text_from_pdf(url)
    # print(text)
    print("="*50)
    processor.process_pdf(url)
