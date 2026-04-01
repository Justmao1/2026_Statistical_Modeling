"""
数据清洗脚本：合并所有原始数据源，生成研究用面板数据
研究题目：数字经济对区域碳排放强度的影响研究
输出：clean_panel_data.csv
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, 'clean_panel_data.csv')
REPORT_FILE = os.path.join(BASE_DIR, 'cleaning_report.txt')

# 30个省份（排除港澳台和西藏）
PROVINCES_30 = [
    '北京', '天津', '河北', '山西', '内蒙古',
    '辽宁', '吉林', '黑龙江',
    '上海', '江苏', '浙江', '安徽', '福建', '江西', '山东',
    '河南', '湖北', '湖南', '广东', '广西',
    '海南', '重庆', '四川', '贵州', '云南',
    '陕西', '甘肃', '青海', '宁夏', '新疆'
]

# 英文名 → 中文简称映射（CEADs数据用）
EN_TO_CN = {
    'Anhui': '安徽', 'Beijing': '北京', 'Chongqing': '重庆',
    'Fujian': '福建', 'Gansu': '甘肃', 'Guangdong': '广东',
    'Guangxi': '广西', 'Guizhou': '贵州', 'Hainan': '海南',
    'Hebei': '河北', 'Heilongjiang': '黑龙江', 'Henan': '河南',
    'Hubei': '湖北', 'Hunan': '湖南', 'Inner mongolia': '内蒙古',
    'Jiangsu': '江苏', 'Jiangxi': '江西', 'Jilin': '吉林',
    'Liaoning': '辽宁', 'Ningxia': '宁夏', 'Qinghai': '青海',
    'Shaanxi': '陕西', 'Shandong': '山东', 'Shanghai': '上海',
    'Shanxi': '山西', 'Sichuan': '四川', 'Tianjin': '天津',
    'Xinjiang': '新疆', 'Yunnan': '云南', 'Zhejiang': '浙江'
}

# 全称 → 中文简称映射（城镇化率数据用）
FULL_TO_SHORT = {
    '北京市': '北京', '天津市': '天津', '河北省': '河北',
    '山西省': '山西', '内蒙古自治区': '内蒙古',
    '辽宁省': '辽宁', '吉林省': '吉林', '黑龙江省': '黑龙江',
    '上海市': '上海', '江苏省': '江苏', '浙江省': '浙江',
    '安徽省': '安徽', '福建省': '福建', '江西省': '江西',
    '山东省': '山东', '河南省': '河南', '湖北省': '湖北',
    '湖南省': '湖南', '广东省': '广东', '广西壮族自治区': '广西',
    '海南省': '海南', '重庆市': '重庆', '四川省': '四川',
    '贵州省': '贵州', '云南省': '云南', '西藏自治区': '西藏',
    '陕西省': '陕西', '甘肃省': '甘肃', '青海省': '青海',
    '宁夏回族自治区': '宁夏', '新疆维吾尔自治区': '新疆'
}

# 报告收集
report_lines = []

def log(msg):
    """打印并记录"""
    print(msg)
    report_lines.append(msg)

# ============================================================
# 1. 读取碳排放数据（CEADs）
# ============================================================
log("=" * 60)
log("步骤 1：读取碳排放数据（CEADs 1997-2022）")
log("=" * 60)

carbon_path = os.path.join(BASE_DIR, 'carbon', '01_碳排放数据_CEADs_1997-2022.csv')
df_carbon = pd.read_csv(carbon_path)

# 英文省名 → 中文简称
df_carbon['省份'] = df_carbon['省份'].map(EN_TO_CN)

# 检查映射是否完整
unmapped_carbon = df_carbon[df_carbon['省份'].isna()]['省份'].unique()
if len(unmapped_carbon) > 0:
    log(f"  ⚠️ 碳排放数据中有未映射的省份: {unmapped_carbon}")
else:
    log(f"  ✅ 省份映射完成，共 {df_carbon['省份'].nunique()} 个省份")

# 筛选30省 & 2018-2022
df_carbon = df_carbon[
    (df_carbon['省份'].isin(PROVINCES_30)) &
    (df_carbon['年份'] >= 2018) &
    (df_carbon['年份'] <= 2022)
].copy()

log(f"  筛选 2018-2022，30省后：{len(df_carbon)} 条记录")
log(f"  年份范围: {df_carbon['年份'].min()} - {df_carbon['年份'].max()}")
log(f"  省份数: {df_carbon['省份'].nunique()}")

# ============================================================
# 2. 读取GDP与经济数据（国家统计局）
# ============================================================
log("")
log("=" * 60)
log("步骤 2：读取GDP与经济数据（国家统计局 2016-2025）")
log("=" * 60)

gdp_path = os.path.join(BASE_DIR, 'gdp', '02_GDP与经济数据_国家统计局_2016-2025.csv')
df_gdp = pd.read_csv(gdp_path)

# 省份名已经是简称，不需要映射
# 筛选30省 & 2018-2022，去除空行（2025年数据为空）
df_gdp = df_gdp[
    (df_gdp['省份'].isin(PROVINCES_30)) &
    (df_gdp['年份'] >= 2018) &
    (df_gdp['年份'] <= 2022)
].copy()

# 去除全空行
df_gdp = df_gdp.dropna(subset=['GDP_亿元'])

log(f"  筛选后：{len(df_gdp)} 条记录")
log(f"  年份范围: {df_gdp['年份'].min()} - {df_gdp['年份'].max()}")
log(f"  省份数: {df_gdp['省份'].nunique()}")

# 检查缺失值
gdp_nulls = df_gdp.isnull().sum()
if gdp_nulls.sum() > 0:
    log(f"  ⚠️ GDP数据缺失值:\n{gdp_nulls[gdp_nulls > 0]}")
else:
    log("  ✅ GDP数据无缺失值")

# ============================================================
# 3. 读取城镇化率数据
# ============================================================
log("")
log("=" * 60)
log("步骤 3：读取城镇化率数据（2016-2024）")
log("=" * 60)

urb_path = os.path.join(BASE_DIR, 'gdp', '03_城镇化率_2016-2024.csv')
df_urb = pd.read_csv(urb_path)

# 全称 → 简称
df_urb['省份'] = df_urb['省份'].map(FULL_TO_SHORT)

unmapped_urb = df_urb[df_urb['省份'].isna()]
if len(unmapped_urb) > 0:
    log(f"  ⚠️ 城镇化率数据中有未映射的省份")

# 筛选30省 & 2018-2022
df_urb = df_urb[
    (df_urb['省份'].isin(PROVINCES_30)) &
    (df_urb['年份'] >= 2018) &
    (df_urb['年份'] <= 2022)
].copy()

log(f"  筛选后：{len(df_urb)} 条记录")
log(f"  年份范围: {df_urb['年份'].min()} - {df_urb['年份'].max()}")

# 只保留需要的列
df_urb = df_urb[['省份', '年份', '城镇化率']].copy()

# ============================================================
# 4. 合并数据
# ============================================================
log("")
log("=" * 60)
log("步骤 4：合并所有数据源")
log("=" * 60)

# 先合并 GDP + 碳排放
df = pd.merge(df_gdp, df_carbon[['省份', '年份', 'CO2排放总量_万吨']],
              on=['省份', '年份'], how='left')
log(f"  GDP + 碳排放合并后：{len(df)} 条")

# 再合并城镇化率
df = pd.merge(df, df_urb, on=['省份', '年份'], how='left')
log(f"  + 城镇化率合并后：{len(df)} 条")

# 检查合并后的缺失
merge_nulls = df.isnull().sum()
if merge_nulls.sum() > 0:
    log(f"  ⚠️ 合并后缺失值:")
    for col, cnt in merge_nulls[merge_nulls > 0].items():
        log(f"     {col}: {cnt} 条缺失")
else:
    log("  ✅ 合并后无缺失值")

# ============================================================
# 5. 计算衍生变量
# ============================================================
log("")
log("=" * 60)
log("步骤 5：计算衍生变量")
log("=" * 60)

# 人均GDP（万元）
df['人均GDP_万元'] = (df['GDP_亿元'] / df['常住人口_万人']).round(3)
log("  ✅ 人均GDP_万元 = GDP / 常住人口")

# ln(人均GDP)
df['ln人均GDP'] = np.log(df['人均GDP_万元'] * 10000).round(4)  # 万元→元再取ln
log("  ✅ ln人均GDP = ln(人均GDP × 10000)")

# 产业结构（第二产业占比 %）
df['产业结构_二产占比'] = (df['第二产业增加值_亿元'] / df['GDP_亿元'] * 100).round(2)
log("  ✅ 产业结构_二产占比 = 第二产业增加值 / GDP × 100")

# 第三产业占比（%）
df['第三产业占比'] = (df['第三产业增加值_亿元'] / df['GDP_亿元'] * 100).round(2)
log("  ✅ 第三产业占比 = 第三产业增加值 / GDP × 100")

# 产业结构高级化（中介变量）= 三产/二产
df['产业结构高级化'] = (df['第三产业增加值_亿元'] / df['第二产业增加值_亿元']).round(4)
log("  ✅ 产业结构高级化 = 第三产业 / 第二产业")

# 碳排放强度（因变量）= CO2排放总量 / GDP
# CO2单位：万吨，GDP单位：亿元
# 碳排放强度单位：万吨/亿元 = 吨/万元
df['碳排放强度'] = (df['CO2排放总量_万吨'] / df['GDP_亿元']).round(4)
log("  ✅ 碳排放强度 = CO2排放总量(万吨) / GDP(亿元)  [单位: 万吨CO2/亿元]")

# 人均碳排放（稳健性检验用）
# CO2单位：万吨，人口单位：万人
# 人均碳排放 = 万吨/万人 = 吨/人
df['人均CO2_吨'] = (df['CO2排放总量_万吨'] / df['常住人口_万人']).round(3)
log("  ✅ 人均CO2_吨 = CO2(万吨) / 人口(万人)  [单位: 吨/人]")

# ============================================================
# 6. 数据质量检查
# ============================================================
log("")
log("=" * 60)
log("步骤 6：数据质量检查")
log("=" * 60)

# 6.1 完整性检查
expected_obs = 30 * 5  # 30省 × 5年
actual_obs = len(df)
log(f"  期望观测数: {expected_obs}，实际观测数: {actual_obs}")
if actual_obs == expected_obs:
    log("  ✅ 观测数完整")
else:
    log(f"  ⚠️ 缺少 {expected_obs - actual_obs} 条观测")

# 6.2 省份完整性
actual_provinces = sorted(df['省份'].unique())
missing_provinces = [p for p in PROVINCES_30 if p not in actual_provinces]
if len(missing_provinces) > 0:
    log(f"  ⚠️ 缺少省份: {missing_provinces}")
else:
    log(f"  ✅ 30个省份全部覆盖")

# 6.3 年份完整性
actual_years = sorted(df['年份'].unique())
log(f"  年份覆盖: {actual_years}")

# 6.4 缺失值汇总
log("")
log("  [缺失值汇总]")
null_summary = df.isnull().sum()
for col, cnt in null_summary.items():
    status = "✅" if cnt == 0 else "⚠️"
    log(f"  {status} {col}: {cnt} 条缺失")

# 6.5 异常值检查
log("")
log("  [异常值检查]")

# GDP不应为负
neg_gdp = df[df['GDP_亿元'] <= 0]
if len(neg_gdp) > 0:
    log(f"  ⚠️ GDP为零或负: {len(neg_gdp)} 条")
else:
    log("  ✅ GDP全部为正值")

# 碳排放不应为负
neg_co2 = df[df['CO2排放总量_万吨'] <= 0]
if len(neg_co2) > 0:
    log(f"  ⚠️ CO2排放为零或负: {len(neg_co2)} 条")
else:
    log("  ✅ CO2排放全部为正值")

# 城镇化率应在0-100之间
invalid_urb = df[(df['城镇化率'] < 0) | (df['城镇化率'] > 100)]
if len(invalid_urb) > 0:
    log(f"  ⚠️ 城镇化率超出0-100范围: {len(invalid_urb)} 条")
else:
    log("  ✅ 城镇化率全部在0-100范围内")

# 碳排放强度极端值
ci_mean = df['碳排放强度'].mean()
ci_std = df['碳排放强度'].std()
ci_outliers = df[abs(df['碳排放强度'] - ci_mean) > 3 * ci_std]
if len(ci_outliers) > 0:
    log(f"  ⚠️ 碳排放强度3σ极端值: {len(ci_outliers)} 条")
    for _, row in ci_outliers.iterrows():
        log(f"     {row['省份']} {int(row['年份'])}年: {row['碳排放强度']:.4f}")
else:
    log("  ✅ 碳排放强度无3σ极端值")

# ============================================================
# 7. 描述性统计
# ============================================================
log("")
log("=" * 60)
log("步骤 7：描述性统计")
log("=" * 60)

stat_cols = ['GDP_亿元', '常住人口_万人', '人均GDP_万元', '城镇化率',
             '产业结构_二产占比', '第三产业占比', '产业结构高级化',
             'CO2排放总量_万吨', '碳排放强度', '人均CO2_吨']

desc = df[stat_cols].describe().round(3)
log("")
log(desc.to_string())

# ============================================================
# 8. 整理最终输出列
# ============================================================
log("")
log("=" * 60)
log("步骤 8：整理输出")
log("=" * 60)

output_cols = [
    '省份', '年份',
    'GDP_亿元', '常住人口_万人', '人均GDP_万元', 'ln人均GDP',
    '第一产业增加值_亿元', '第二产业增加值_亿元', '第三产业增加值_亿元',
    '产业结构_二产占比', '第三产业占比', '产业结构高级化',
    '城镇化率',
    'CO2排放总量_万吨', '碳排放强度', '人均CO2_吨'
]

df_out = df[output_cols].sort_values(['省份', '年份']).reset_index(drop=True)

# 保存
df_out.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
log(f"  ✅ 已保存: {OUTPUT_FILE}")
log(f"  行数: {len(df_out)}，列数: {len(df_out.columns)}")
log(f"  列名: {list(df_out.columns)}")

# ============================================================
# 9. 与已有合并数据交叉验证
# ============================================================
log("")
log("=" * 60)
log("步骤 9：与 merged_panel_2018-2022.csv 交叉验证")
log("=" * 60)

merged_path = os.path.join(BASE_DIR, 'merged_panel_2018-2022.csv')
if os.path.exists(merged_path):
    df_old = pd.read_csv(merged_path)
    
    # 取共同样本对比
    common = pd.merge(df_out, df_old, on=['省份', '年份'], suffixes=('_new', '_old'))
    
    # 对比 GDP
    gdp_diff = abs(common['GDP_亿元_new'] - common['GDP_亿元_old'])
    log(f"  GDP差异: 最大 {gdp_diff.max():.2f} 亿元, 平均 {gdp_diff.mean():.4f} 亿元")
    
    # 对比 CO2
    co2_diff = abs(common['CO2排放总量_万吨_new'] - common['CO2排放总量_万吨_old'])
    log(f"  CO2差异: 最大 {co2_diff.max():.2f} 万吨, 平均 {co2_diff.mean():.4f} 万吨")
    
    # 对比城镇化率
    urb_diff = abs(common['城镇化率_new'] - common['城镇化率_old'])
    log(f"  城镇化率差异: 最大 {urb_diff.max():.2f}%, 平均 {urb_diff.mean():.4f}%")
    
    if gdp_diff.max() < 1 and co2_diff.max() < 1:
        log("  ✅ 数据与已有合并表完全一致")
    else:
        log("  ⚠️ 存在差异，请检查")
else:
    log("  ⚠️ 未找到 merged_panel_2018-2022.csv，跳过交叉验证")

# ============================================================
# 10. 保存报告
# ============================================================
log("")
log("=" * 60)
log("数据清洗完成！")
log("=" * 60)

with open(REPORT_FILE, 'w', encoding='utf-8') as f:
    f.write('\n'.join(report_lines))
    f.write('\n')

print(f"\n报告已保存: {REPORT_FILE}")
