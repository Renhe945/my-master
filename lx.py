import random
import pandas as pd


# 生成单个客户数据的函数
def generate_customer_data():
    customer = {}

    # 客户旅程阶段
    journey_stages = ["新访客", "未购买客户回访", "已购买客户回访"]
    customer["journey_stage"] = random.choice(journey_stages)

    # 地理位置
    provinces = ["广东省", "江苏省", "其他省份"]
    customer["geographic_location"] = random.choice(provinces)

    # 性别
    genders = ["男", "女"]
    customer["gender"] = random.choice(genders)

    # 消费层级
    consumption_levels = ["L1", "L2", "L3", "L4", "L5"]
    customer["consumption_level"] = random.choice(consumption_levels)

    # 兴趣偏好
    interests = ["时尚趣味", "白富美", "科技前沿", "户外运动", "美食探索"]
    customer["interest_preference"] = random.choice(interests)

    # 年龄分布
    age_groups = ["18-24岁", "25-29岁", "30-34岁", "40-49岁", "50岁以上"]
    customer["age_distribution"] = random.choice(age_groups)

    return customer


# 生成指定数量客户数据的函数
def generate_customer_data_set(num_customers):
    customer_data_set = []
    for _ in range(num_customers):
        customer_data_set.append(generate_customer_data())

    return pd.DataFrame(customer_data_set)


if __name__ == "__main__":
    # 生成1000名客户的模拟数据示例（可根据需要调整数量）
    num_customers = 1000
    customer_data = generate_customer_data_set(num_customers)

    # 将数据导出到kh.txt文件
    customer_data.to_csv("kh.txt", sep="\t", index=False)
    # journey_stage	geographic_location	gender	consumption_level	interest_preference	age_distribution