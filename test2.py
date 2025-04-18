import re

text = 'A份额:24GSGK12807AA份额:中国人民银行公布的7天通知存款利率B份额:24GSGK12807BB份额:中国人民银行公布的7天通知存款利率'

# 正则表达式模式（动态匹配份额名称和利率）
pattern = r"([A-Z]份额:).*?(\1.*?)(?=[A-Z]份额:|$)"
matches = re.findall(pattern, text)
print(matches)
result = " ".join(value for name, value in matches)
print(result)

# 转换为字典格式
result = {f"{name}": value.strip() for name, value in matches}
print(result)