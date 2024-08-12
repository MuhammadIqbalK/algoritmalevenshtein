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

# Pengecekan duplikat awal dengan substring matching
def drop_duplicates_with_substring(df, column, length):
    seen = set()
    unique_rows = []

    for _, row in df.iterrows():
        norm_value = row[column]
        substrings = get_substrings(norm_value, length)
        is_duplicate = False

        for seen_substr in seen:
            if any(match_substrings(seen_substr, substr, length) for substr in substrings):
                is_duplicate = True
                break

        if not is_duplicate:
            unique_rows.append(row)
            seen.add(norm_value)

    return pd.DataFrame(unique_rows).reset_index(drop=True)

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
query_rekon = 'SELECT DISTINCT "PROJECT_DESC" FROM "RPB_REKON";'
query_sap = 'SELECT DISTINCT "SHORT_TEX" FROM "SAP";'

# Membaca data ke dalam DataFrame
data_rekon = pd.read_sql_query(query_rekon, conn)
data_sap = pd.read_sql_query(query_sap, conn)

# Normalisasi data
data_rekon['norm_rekon'] = data_rekon['PROJECT_DESC'].apply(normalize)
data_sap['norm_sap'] = data_sap['SHORT_TEX'].apply(normalize)

# Pengecekan duplikat awal
data_rekon = data_rekon.drop_duplicates(subset='norm_rekon').reset_index(drop=True)
data_sap = data_sap.drop_duplicates(subset='norm_sap').reset_index(drop=True)

# Mendapatkan list dari norm_sap
sap_norm_list = data_sap['norm_sap'].tolist()

# Mengatur panjang substring untuk pencocokan
substring_length = 5

# Proses penghapusan duplikat yang diperkuat
data_rekon = drop_duplicates_with_substring(data_rekon, 'norm_rekon', substring_length)
data_sap = drop_duplicates_with_substring(data_sap, 'norm_sap', substring_length)

# Menyimpan hasil pencocokan
results = []

for _, rek_row in data_rekon.iterrows():
    rek_norm = rek_row['norm_rekon']
    for sap_norm in sap_norm_list:
        if match_by_levenshtein(rek_norm, sap_norm):
            if match_substrings(rek_norm, sap_norm, substring_length):
                results.append({
                    'rekon_id': rek_row.name,  # Menggunakan index sebagai ID
                    'sap_id': sap_norm_list.index(sap_norm),  # Menggunakan index sebagai ID
                    'norm_rekon': rek_norm,
                    'norm_sap': sap_norm
                })

# Mengubah hasil pencocokan menjadi DataFrame
results_df = pd.DataFrame(results).drop_duplicates()

# Mengeliminasi duplikasi pada `norm_sap` terlebih dahulu
results_df = results_df.drop_duplicates(subset='norm_sap')

# Mengeliminasi duplikasi pada `norm_rekon` setelah `norm_sap`
final_results = results_df.drop_duplicates(subset='norm_rekon').head(100)

# Mengatur tampilan untuk menampilkan semua baris dan kolom
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

# Simpan hasil ke file
with open('output.txt', 'w') as f:
    f.write(final_results.to_string())

# Tutup koneksi
conn.close()
