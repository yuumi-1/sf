CREATE DATABASE IF NOT EXISTS testdb3;
USE testdb3;

-- 表1：产品成立公告信息表
CREATE TABLE IF NOT EXISTS product_announcement (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    reg_code VARCHAR(50) COMMENT '产品登记编码',
    prd_code VARCHAR(50) NOT NULL COMMENT '产品代码',
    prd_name VARCHAR(200) NOT NULL COMMENT '产品名称',
    amount_raised DECIMAL(20,6) NOT NULL COMMENT '成立规模(元)',
    product_start_date DATE COMMENT '产品起始日期',
    product_end_date DATE COMMENT '产品结束日期',
    fund_custodian VARCHAR(200) COMMENT '托管机构',
    source_type VARCHAR(255) COMMENT '来源标识',
    source_id INT COMMENT '来源id',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
    INDEX idx_reg_code (reg_code),
    INDEX idx_prd_code (prd_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产品成立公告信息';

-- 表2：业绩比较基准/预期收益率表
CREATE TABLE IF NOT EXISTS performance_benchmark (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    reg_code VARCHAR(50) NOT NULL COMMENT '产品登记编码',
    prd_code VARCHAR(50) NOT NULL COMMENT '产品代码',
    prd_name VARCHAR(200) NOT NULL COMMENT '产品名称',
    perf_benchmark LONGTEXT NOT NULL COMMENT '业绩比较基准/预期收益率',
    perf_benchmark_max DECIMAL(10,4) COMMENT '业绩比较基准上限值',
    perf_benchmark_min DECIMAL(10,4) COMMENT '业绩比较基准下限值',
    start_date DATE COMMENT '启用日期',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
    source_type VARCHAR(255) COMMENT '来源标识',
    source_id INT COMMENT '来源id',
    INDEX idx_reg_code (reg_code),
    INDEX idx_prd_code (prd_code),
    INDEX idx_start_date (start_date),
    CONSTRAINT fk_reg_code FOREIGN KEY (reg_code) REFERENCES product_announcement(reg_code) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='业绩比较基准/预期收益率信息';