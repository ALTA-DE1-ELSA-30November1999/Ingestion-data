import pandas as pd 
pd.set_option('display.max_columns', None)

#-----Nomor 1--------
df = pd.read_csv("../dataset/sample.csv", sep=",")
print("------NOMOR 1-------")
print(df.head(10))
print("--------------------")

#------Nomor 2---------
ubah_kolom = df.rename(columns={'VendorID':'Vendor_ID',
                                'RatecodeID':'Rate_code_ID',
                                'PULocationID':'PU_Location_ID',
                                'DOLocationID':'DO_Location_ID'})
print('------NOMOR 2 --------')
print(ubah_kolom.head(10))
print('----------------------')

#------Nomor 3---------
top_10 = df.nlargest(10, 'passenger_count')[['VendorID','passenger_count', 'trip_distance', 
                                             'payment_type', 'fare_amount','extra', 
                                             'mta_tax', 'tip_amount', 'tolls_amount', 
                                             'improvement_surcharge', 'total_amount', 'congestion_surcharge']]
print('-------NOMOR 3-------')
print(top_10)
print('---------------------')


