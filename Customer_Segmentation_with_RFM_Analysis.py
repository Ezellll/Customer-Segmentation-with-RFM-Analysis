import datetime as dt
import pandas as pd


pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

##################################################################
# Görev 1: Veriyi Anlama ve Hazırlama
##################################################################


df_ = pd.read_csv("flo_data_20K.csv")
df = df_.copy()

# Adım 2: Veri setinde:

# İlk 10 gözlem
df.head(10)

# Değişken isimleri
df.columns

# Betimsel istatislik
df.describe().T

# Boş değerler
df.isnull().sum()

#Değişken tiplerinin incelenmesi
df.dtypes

df["master_id"].nunique()
df.columns

# ifade etmektedir. Her bir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler
# oluşturunuz.


# Toplam alışveriş sayısı =  Online + Offline
df["Order_num_total_ever"] = df['order_num_total_ever_online'] + df['order_num_total_ever_offline']

# Toplam alışveriş harcaması = Online + Offline
df["Customer_value_total_ever"] = df['customer_value_total_ever_online'] + df['customer_value_total_ever_offline']

df.tail(10)


df.dtypes
for col in df.columns:
    if "date" in col:
        df[col] = pd.to_datetime(df[col])




df.head(10)
df.columns
df.groupby('order_channel').agg({"master_id": lambda x: x.nunique(),
                                 "Order_num_total_ever": "mean",
                                 "Customer_value_total_ever": "mean"})



df.sort_values(by="Customer_value_total_ever", ascending=False).head(10)




df.sort_values(by="Order_num_total_ever", ascending=False).head(10)


def Data_Preparation(dataframe):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    print("##################### İlk 10 Değer #####################")
    print(df.head(10))
    print("##################### VARIABLES #####################")
    print(df.columns)
    print("##################### Betimsel İstatislik #####################")
    print(df.describe().T)
    print("##################### Null Değerler #####################")
    print(df.isnull().sum())
    print("##################### Data Types #####################")
    print(df.dtypes)

    # her bir müşetirinin toplam alışveriş sayısı ve harcaması
    dataframe["Order_num_total_ever"] = dataframe['order_num_total_ever_online'] + dataframe['order_num_total_ever_offline']
    dataframe["Customer_value_total_ever"] = dataframe['customer_value_total_ever_online'] + dataframe['customer_value_total_ever_offline']

    for col in dataframe.columns:
        if "date" in col:
            dataframe[col] = pd.to_datetime(dataframe[col])

    return dataframe


##################################################
# Görev 2: RFM Metriklerinin Hesaplanması
###################################################


# Receny = Analiz tarihi - En son alışveriş yapılan tarih (online veya offline)
# Frequency  = Toplam ne kadar  alışveriş yapıldığını gösterir ---> Order_num_total_ever
# Monetary = Alışverişlerden ne firmanın ne kadar kazanç sağladığını gösterir ------>Customer_value_total_ever

df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)
df.head(10)
# Bu kısmı değiştirebiliriz


rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()),
                                     'Order_num_total_ever': lambda Order_num_total_ever: Order_num_total_ever,
                                     'Customer_value_total_ever': lambda Customer_value_total_ever: Customer_value_total_ever })



rfm.columns = ['recency', 'frequency', 'monetary']


#########################################################################################
# Görev 3: RF Skorunun Hesaplanması
#########################################################################################

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])


rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                   rfm['frequency_score'].astype(str))

#########################################################################################
# Görev 4: RF Skorunun Segment Olarak Tanımlanması
#########################################################################################


seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
rfm = rfm[["recency", "frequency", "monetary", "segment"]]
#########################################################################################
# Görev 5
#########################################################################################



rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])


# Adım 2:
# a
df.head(10)
df_rfm = pd.merge(df, rfm, on="master_id")
case_a = df_rfm[((df_rfm["segment"] == "champions") | (df_rfm["segment"] == "loyal_customers"))
                & (((df_rfm["Customer_value_total_ever"]/df_rfm["Order_num_total_ever"]) > 250)
                & (df_rfm["interested_in_categories_12"].str.contains('KADIN')))]
customers_id_a = case_a["master_id"]
customers_id_a.to_csv("görev-5-a.csv")

# b
df_rfm = pd.merge(df, rfm, on="master_id")
case_b = df_rfm[((df_rfm["segment"] == "cant_loose") | (df_rfm["segment"] == "about_to_sleep")
                | (df_rfm["segment"] == "new_customers")) & ((df_rfm["interested_in_categories_12"].str.contains('ERKEK'))
                & (df_rfm["interested_in_categories_12"].str.contains('COCUK')))]

customers_id_b = case_b["master_id"]
customers_id_b.to_csv("görev-5-b.csv")