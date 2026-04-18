import pandas as pd
import numpy as np
import logging

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Scope3ETLCleaner:
    """
    Scope 3 物流数据前置清洗引擎 (V1.0 Commercial Edition)
    专为 Category 4/9 运输数据设计，解决大型SaaS无法处理的非标单位脏数据。
    """

    DEFAULT_MAPPING = {
        'weight': {
            'kg': 'kg', 'kgs': 'kg', 'kilogram': 'kg', 'kilograms': 'kg', '千克': 'kg', '公斤': 'kg',
            't': 't', 'ton': 't', 'tons': 't', 'tonne': 't', 'mt': 't', '吨': 't',
            'lbs': 'lbs', 'lb': 'lbs', 'pound': 'lbs', 'pounds': 'lbs', '磅': 'lbs'
        },
        'distance': {
            'km': 'km', 'kms': 'km', 'kilometer': 'km', 'kilometers': 'km', '公里': 'km', '千米': 'km',
            'mile': 'mile', 'miles': 'mile', 'mi': 'mile', '英里': 'mile'
        }
    }

    def __init__(self, user_mapping_rules: dict = None):
        self.user_rules = user_mapping_rules or {'weight': {}, 'distance': {}}
        self.active_rules = self._compile_rules()
        logging.info("Scope3ETLCleaner initialized. User rules injected.")

    def _compile_rules(self) -> dict:
        compiled = {
            'weight': self.DEFAULT_MAPPING['weight'].copy(),
            'distance': self.DEFAULT_MAPPING['distance'].copy()
        }
        for category in ['weight', 'distance']:
            if category in self.user_rules:
                custom_rules = {str(k).strip().lower(): str(v).strip().lower() 
                                for k, v in self.user_rules[category].items()}
                compiled[category].update(custom_rules)
                
        return compiled

    def _normalize_unit(self, raw_unit, category: str) -> tuple:
        if pd.isna(raw_unit):
            return np.nan, True 

        raw_lower = str(raw_unit).strip().lower()

        if raw_lower in self.active_rules[category]:
            return self.active_rules[category][raw_lower], False

        return raw_unit, True

    def clean_logistics_data(self, df: pd.DataFrame, weight_col: str, distance_col: str) -> pd.DataFrame:
        result_df = df.copy()
        
        std_w_col = f"Std_{weight_col}"
        std_d_col = f"Std_{distance_col}"
        review_col = "ETL_Review_Flag"

        result_df[std_w_col] = np.nan
        result_df[std_d_col] = np.nan
        result_df[review_col] = "Clean" 

        logging.info(f"Processing DataFrame: {len(result_df)} rows.")

        if weight_col in result_df.columns:
            w_res = result_df[weight_col].apply(lambda x: self._normalize_unit(x, 'weight'))
            result_df[std_w_col] = w_res.apply(lambda x: x[0])
            result_df.loc[w_res.apply(lambda x: x[1]), review_col] = "Needs Manual Review"

        if distance_col in result_df.columns:
            d_res = result_df[distance_col].apply(lambda x: self._normalize_unit(x, 'distance'))
            result_df[std_d_col] = d_res.apply(lambda x: x[0])
            result_df.loc[d_res.apply(lambda x: x[1]), review_col] = "Needs Manual Review"

        dirty_count = len(result_df[result_df[review_col] == 'Needs Manual Review'])
        logging.info(f"ETL Complete. Clean rows: {len(result_df) - dirty_count}, Needs Review: {dirty_count}")

        return result_df

# ================= 测试执行块 =================
if __name__ == "__main__":
    # 模拟外部传入的用户配置规则
    user_settings = {
        'weight': {'kilos': 'kg'},
        'distance': {'ml': 'mile'}
    }

    cleaner = Scope3ETLCleaner(user_mapping_rules=user_settings)

    # 模拟客户上传的非标脏数据
    raw_data = pd.DataFrame({
        'Shipment_ID': ['S01', 'S02', 'S03', 'S04'],
        'Raw_Weight_Unit': ['KGS', 'kilos', 'tons', 'WT_UNKNOWN'], # WT_UNKNOWN 是毒点数据
        'Raw_Distance_Unit': ['KMS', 'ml', 'miles', 'km']
    })

    cleaned_data = cleaner.clean_logistics_data(
        df=raw_data,
        weight_col='Raw_Weight_Unit',
        distance_col='Raw_Distance_Unit'
    )

    print("\n========= 清洗结果输出 =========")
    print(cleaned_data)
    print("================================\n")

# Backward/contract alias for orchestrator scripts
Scope3Cleaner = Scope3ETLCleaner

