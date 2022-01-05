import pandas as pd
import traceback as tb
from functools import reduce
from pytrends.request import TrendReq

# pytrends = TrendReq(hl='ru')

# kw_list = ["акции"]
# pytrends.build_payload(kw_list)
# df_res = pytrends.related_queries()["акции"]["top"].values.tolist()
# print(df_res)

def compare_custom(row):
    if int(row["value"]) > 40:
        return row

def get_stat_all_in_one(row):
    pytrends = TrendReq(hl='ru')
    try:

        kw_list = [row]
        pytrends.build_payload(kw_list)
        df_res = pytrends.related_queries()[row]["top"]
        
        if df_res is None:
            pass
        else:
            df_res = df_res.aggregate(lambda x: compare_custom(x), axis=1)
            df_res = df_res.dropna()
            df_res.to_csv("res.csv", index=False, mode="a", header=False)

    except:
        print("err")


def get_stat_column_for_each(df_res, row):
    pytrends = TrendReq(hl='ru')

    try:
        kw_list = [row]
        pytrends.build_payload(kw_list, timeframe='now 1-y')

        df_tmp = pytrends.related_queries()[row]["top"]

        if df_tmp is None:
            return pd.DataFrame(columns=[row])
        else:
            df_tmp = df_tmp.aggregate(lambda x: compare_custom(x), axis=1)
            df_tmp = df_tmp.dropna()
            df_tmp = df_tmp.rename(columns={'query': row})
            df_tmp = df_tmp.drop(['value'], axis=1)
            return df_tmp

    except:
        print("err")


def main():

    df = pd.read_csv("./kw.csv", encoding="utf-8")
    df["kw"] = df["kw"].map(lambda x: x.strip())

    # all in one column with rating
    # df["kw"].map(lambda x: get_stat_all_in_one(x))

    # for each keyword column
    df_res = pd.DataFrame(columns=df["kw"].values.tolist())
    list_df = df["kw"].map(lambda x: get_stat_column_for_each(df_res, x))
    df_res = reduce(lambda df1, df2: pd.concat([df1, df2], axis=1),list_df)

    df_res.to_excel("res.xlsx", index=False, header=True, encoding="utf-8")


if __name__ == "__main__":
    main()
    