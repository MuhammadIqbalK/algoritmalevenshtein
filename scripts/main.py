import pandas as pd
import psycopg2
from fuzzywuzzy import fuzz

# Fungsi untuk normalisasi data
def normalize(text):
    if pd.isna(text):
        return ''
    return text.lower().replace(' ', '').replace('-', '').replace('/', '').replace('_', '')

# Fungsi untuk mencocokkan data menggunakan Levenshtein distance
def match_by_levenshtein(rek_norm, sap_norm, threshold=50):
    return fuzz.ratio(rek_norm, sap_norm) > threshold

# Ganti dengan detail koneksi Anda
conn = psycopg2.connect(
    dbname='develop',
    user='develop',
    password='dev2019',
    host='10.62.175.21',
    port='5433'
)

# Ambil data dari database
query_rekon = 'SELECT "PROJECT_DESC" FROM "RPB_REKON" LIMIT 10000;'
query_sap = 'SELECT "SHORT_TEX" FROM "SAP" LIMIT 10000;'

# Membaca data ke dalam DataFrame
data_rekon = pd.read_sql_query(query_rekon, conn)
data_sap = pd.read_sql_query(query_sap, conn)

# Normalisasi data
data_rekon['norm_rekon'] = data_rekon['PROJECT_DESC'].apply(normalize)
data_sap['norm_sap'] = data_sap['SHORT_TEX'].apply(normalize)

# Menggabungkan data berdasarkan kondisi pencocokan
results = []

# Mengambil list dari norm_sap
sap_norm_list = data_sap['norm_sap'].tolist()

for _, rek_row in data_rekon.iterrows():
    for sap_norm in sap_norm_list:
        if match_by_levenshtein(rek_row['norm_rekon'], sap_norm):
            results.append({
                'rekon_id': rek_row.name,  # Menggunakan index sebagai ID
                'sap_id': sap_norm_list.index(sap_norm),  # Menggunakan index sebagai ID
                'norm_rekon': rek_row['norm_rekon'],
                'norm_sap': sap_norm
            })

# Menghilangkan duplikat dan membatasi hasil
results_df = pd.DataFrame(results).drop_duplicates().head(100)

# Mengatur tampilan untuk menampilkan semua baris dan kolom
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Simpan hasil ke file
with open('output2.txt', 'w') as f:
    f.write(results_df.to_string())

# Tutup koneksi
conn.close()
