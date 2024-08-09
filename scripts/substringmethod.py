import pandas as pd
import psycopg2
from fuzzywuzzy import fuzz

# Fungsi untuk normalisasi data
def normalize(text):
    if pd.isna(text):
        return ''
    return text.lower().replace(' ', '').replace('-', '').replace('/', '').replace('_', '').replace('(', '').replace(')', '')

# Fungsi untuk mendapatkan substring dari string
def get_substrings(s, length):
    """Mengembalikan list substring dari panjang tertentu dari string."""
    return [s[i:i+length] for i in range(len(s) - length + 1)]

# Fungsi untuk mencocokkan substring dari dua string
def match_substrings(s1, s2, length):
    """Cocokkan substring dari s1 dan s2 berdasarkan panjang tertentu."""
    substrings1 = set(get_substrings(s1, length))
    substrings2 = set(get_substrings(s2, length))
    return len(substrings1.intersection(substrings2)) > 0

# Fungsi untuk mencocokkan data menggunakan Levenshtein distance
def match_by_levenshtein(rek_norm, sap_norm, threshold=75):
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

# Mendapatkan list dari norm_sap
sap_norm_list = data_sap['norm_sap'].tolist()

# Mengatur panjang substring untuk pencocokan
substring_length = 4

results = []

for _, rek_row in data_rekon.iterrows():
    rek_norm = rek_row['norm_rekon']
    for sap_norm in sap_norm_list:
        # Pencocokan substring
        if match_substrings(rek_norm, sap_norm, substring_length):
            results.append({
                'rekon_id': rek_row.name,  # Menggunakan index sebagai ID
                'sap_id': sap_norm_list.index(sap_norm),  # Menggunakan index sebagai ID
                'norm_rekon': rek_norm,
                'norm_sap': sap_norm
            })

# Menghilangkan duplikat dan membatasi hasil
results_df = pd.DataFrame(results).drop_duplicates().head(100)

# Mengatur tampilan untuk menampilkan semua baris dan kolom
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Simpan hasil ke file
with open('output.txt', 'w') as f:
    f.write(results_df.to_string())

# Tutup koneksi
conn.close()
